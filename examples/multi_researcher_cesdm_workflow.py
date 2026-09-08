#!/usr/bin/env python3
"""
multi_researcher_cesdm_workflow.py - a two-researcher scientific handoff on ESDH
================================================================================

Researcher A curates raw open data. Researcher B turns it into a scenario, runs a
solver, and publishes results. Months later a reviewer asks: *which exact bytes
produced figure 3?* This script makes that question answerable.

    Phase A - Researcher A (Data Curator)
        stages 3 open profiles, uploads them to `main` with DCAT metadata,
        and tags an immutable raw-data release: v1-0-0-raw-data

    Phase B - Researcher B (Modeler & Analyst)
        branches, fetches A's profiles *by dataset_id* (verifying hashes),
        binds them to a CESDM model, applies entity-level edits, emits a
        sub-scenario delta blueprint, runs a mock solver, and commits
        ONLY the blueprint + lineage manifest

    Verification
        back-traces the published result to A's raw-data release and
        re-verifies every input hash

Run
---
    docker compose up -d          # or: make up
    python examples_cesdh/multi_researcher_cesdm_workflow.py

    RAW_HUB_OFFLINE=1 python examples_cesdh/multi_researcher_cesdm_workflow.py

Two API corrections worth knowing
---------------------------------
* **`ScenarioManager.update_entity()` does not exist.** Neither does
  `delete_entity()`. The real way to change a value is `add_attribute()` on an
  existing entity id - re-adding overwrites, and `declare_scenario` then reports
  the entity under `modified_components`. This script uses the real API and
  labels it, rather than calling a method that was never implemented.
* **Branch names cannot contain slashes.** LakeFS rejects `experiment/ch-res-…`
  outright, so the branch here is `experiment-ch-res-expansion-2030`.

Naming: three unrelated things want to be called "baseline"
-----------------------------------------------------------
They live in separate namespaces and never collide - LakeFS refs and CESDM
scenario ids are stored in different Fuseki graphs - but the *word* collides in
a reader's head. This script keeps them apart by name:

* `cfg.raw_branch` / `cfg.raw_data_release` - LakeFS refs: where Researcher A's
  bytes land, and the immutable tag that pins them. A *release of raw data*,
  not a scenario.
* `scenario_id="baseline_ch_neighbours"` - the CESDM baseline *scenario*, root
  of the delta chain. The only true "baseline" in this file.
* `declare_scenario(..., "baseline", ...)` - the scenario *type* argument
  ("baseline" vs "subscenario"), part of the toolbox API.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CESDM_ROOT = REPO_ROOT / "cesdm" / "sweet-cosi-cesdm"

try:
    from cesdh import CESDHClient
except ImportError as exc:  # pragma: no cover - setup guidance
    raise SystemExit(
        "Could not import the CESDH SDK.\n    pip install -e src/sdk\n"
        f"(original error: {exc})"
    ) from exc

# Reuse Researcher A's staging logic rather than re-implementing it - the raw
# data lifecycle example already fetches from PVGIS / OPSD / Dataverse with an
# offline fallback, and duplicating that would let the two drift apart.
sys.path.insert(0, str(REPO_ROOT / "examples_cesdh"))
import raw_data_hub_lifecycle as rawhub

logger = logging.getLogger(__name__)


def say(msg: str = "") -> None:
    print(msg, flush=True)


def banner(title: str, rule: str = "=") -> None:
    say("")
    say(rule * 78)
    say(f"  {title}")
    say(rule * 78)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Config:
    gateway_url: str = os.environ.get(
        "CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080"
    )
    lakefs_url: str = os.environ.get("LAKEFS_URL", "http://localhost:8000")
    repository: str = os.environ.get("MR_REPOSITORY", "multi-researcher-demo")

    researcher_a: str = os.environ.get("MR_RESEARCHER_A", "researcher-a")
    researcher_b: str = os.environ.get("MR_RESEARCHER_B", "researcher-b")
    researcher_c: str = os.environ.get("MR_RESEARCHER_C", "researcher-c")

    # The branch A publishes to and that B/C fork from. This is the raw working
    # branch, not the CESDM-domain "baseline scenario" - see the naming note in
    # the module docstring.
    raw_branch: str = os.environ.get("MR_RAW_BRANCH", "main")
    # LakeFS ref ids allow letters, digits, underscores and dashes only, so the
    # conventional "experiment/…" form is not expressible - see module docstring.
    experiment_branch: str = os.environ.get(
        "MR_EXPERIMENT_BRANCH", "experiment-ch-res-expansion-2030"
    )
    # The immutable LakeFS tag pinning A's bytes. A release of raw data, not a
    # scenario reference.
    raw_data_release: str = os.environ.get(
        "MR_RAW_DATA_RELEASE", "v1-0-0-raw-data"
    )
    reanalysis_branch: str = os.environ.get(
        "MR_REANALYSIS_BRANCH", "reanalysis-c-storage-sensitivity"
    )

    experiment_id: str = "EXP-2030-CH-SOLAR-001"
    solar_expansion_mw: float = 5_000.0
    solver_version: str = "pypsa-v0.26.0"

    workdir: Path = REPO_ROOT / "data" / "multi_researcher"
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def stamp(self) -> str:
        return self.started_at.strftime("%Y%m%d-%H%M%S")

    @property
    def experiment_tag(self) -> str:
        return f"exp-ch-solar-{self.stamp}"


def _status(exc: Exception) -> int | None:
    """HTTP status behind an SDK error, read without importing the transport."""
    return getattr(getattr(exc, "response", None), "status_code", None)


# ═════════════════════════════════════════════════════════════════════════════
# PHASE A - Researcher A: curate and publish the raw-data release
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class PublishedDataset:
    """One raw profile as Researcher A published it - the handoff contract."""

    filename: str
    dataset_id: str
    sha256: str
    rows: int
    provenance: str
    carrier: str
    units: str
    local_path: Path


def phase_a_publish_raw(client: CESDHClient, cfg: Config) -> list[PublishedDataset]:
    """Stage the open datasets, upload them to `main`, and tag the raw release."""
    banner("PHASE A - Researcher A (Data Curator)")

    say("  A1. staging open datasets (PVGIS / OPSD / Dataverse)")
    staging_cfg = rawhub.Config(staging_dir=cfg.workdir / "raw_staging")
    datasets = rawhub.stage_datasets(staging_cfg)

    say("")
    say("  A2. creating the shared repository")
    try:
        client.create_repository()
        say(f"      created '{cfg.repository}'")
    except Exception as exc:
        if _status(exc) == 409:
            say(f"      '{cfg.repository}' already exists - reusing")
        else:
            logger.exception("Repository pre-creation failed")
            say(f"      !! {exc}")

    say("")
    say(f"  A3. uploading to '{cfg.raw_branch}' with DCAT metadata")
    published: list[PublishedDataset] = []
    for ds in datasets:
        try:
            result = client.upload_model(
                str(ds.path),
                owner=cfg.researcher_a,
                branch=cfg.raw_branch,
                version="1.0.0",
                description=ds.dcat_description(),
                force_tier="generic",
                scenario_metadata=ds.scenario_metadata(),
                dcat_metadata=ds.dcat_metadata(),
                force=True,
                commit_message=f"{cfg.raw_data_release}: publish {ds.filename} ({ds.provenance})",
            )
            if result.get("status") == "validation_failed":
                say(f"      !! rejected {ds.filename}")
                continue
            published.append(
                PublishedDataset(
                    filename=ds.filename,
                    dataset_id=result["dataset_id"],
                    sha256=ds.sha256,
                    rows=ds.rows,
                    provenance=ds.provenance,
                    carrier=ds.carrier,
                    units=ds.units,
                    local_path=ds.path,
                )
            )
            say(f"      OK {ds.filename:38s} -> {result['dataset_id']}")
        except Exception as exc:
            logger.exception("Upload failed for %s", ds.filename)
            say(f"      !! {ds.filename}: {str(exc)[:60]}")

    say("")
    say(f"  A4. tagging the immutable raw-data release '{cfg.raw_data_release}'")
    try:
        tag = client.create_tag(cfg.raw_data_release, ref=cfg.raw_branch)
        say(f"      tag -> {tag['commit_id'][:16]}...")
    except Exception as exc:
        if _status(exc) == 409:
            say(
                f"      tag '{cfg.raw_data_release}' already exists - left untouched (immutable)"
            )
        else:
            say(f"      !! could not tag: {str(exc)[:60]}")

    say("")
    say(f"  Researcher A published {len(published)} dataset(s). Handoff contract:")
    for d in published:
        say(f"      {d.filename:38s} {d.dataset_id}  sha256={d.sha256[:16]}...")
    return published


# ═════════════════════════════════════════════════════════════════════════════
# PHASE B - Researcher B: branch, model, solve, publish lineage
# ═════════════════════════════════════════════════════════════════════════════


def b1_branch(client: CESDHClient, cfg: Config) -> None:
    """Open the experiment branch off main."""
    say(f"  B1. branching '{cfg.experiment_branch}' off '{cfg.raw_branch}'")
    try:
        head = client.create_branch(cfg.experiment_branch, source=cfg.raw_branch)
        say(f"      created at {str(head.get('head'))[:16]}...")
    except Exception as exc:
        if _status(exc) == 409:
            say("      already exists - reusing")
        else:
            logger.exception("Branch creation failed")
            say(f"      !! {exc}")


def b2_fetch_inputs(
    client: CESDHClient, cfg: Config, published: list[PublishedDataset]
) -> dict[str, Path]:
    """Fetch A's profiles back out of ESDH by dataset_id and verify each hash.

    Researcher B never touches A's filesystem. The dataset_id is the entire
    contract, and the hash check is what makes "using A's data" a verifiable
    claim instead of an assertion.
    """
    say("")
    say("  B2. fetching Researcher A's profiles from ESDH (by dataset_id)")
    fetch_dir = cfg.workdir / "fetched_inputs"
    fetch_dir.mkdir(parents=True, exist_ok=True)

    fetched: dict[str, Path] = {}
    for d in published:
        dest = fetch_dir / d.filename
        try:
            client.download_to_file(d.dataset_id, str(dest))
        except Exception as exc:
            logger.exception("Fetch failed for %s", d.dataset_id)
            say(f"      !! {d.filename}: {str(exc)[:60]}")
            continue
        got = sha256_file(dest)
        ok = got == d.sha256
        say(
            f"      {'OK ' if ok else '!! '}{d.filename:38s} sha256 {'matches' if ok else 'MISMATCH'}"
        )
        if ok:
            fetched[d.dataset_id] = dest
    return fetched


def b3_build_scenario(
    client: CESDHClient,
    cfg: Config,
    published: list[PublishedDataset],
    fetched: dict[str, Path],
) -> tuple[Path, Path, dict[str, Any]]:
    """Bind the raw profiles to a CESDM model, edit it, and emit a delta blueprint.

    Returns (baseline_yaml, blueprint_yaml, edit_summary).
    """
    say("")
    say("  B3. building the CESDM model and applying scenario edits")

    for p in (CESDM_ROOT, CESDM_ROOT / "tools", CESDM_ROOT / "examples"):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    import tutorial_ch_neighbours as tutorial
    from tools.scenario_manager import declare_scenario

    # scenario_manager installs its own stdout handler and also propagates to
    # root, so every save is logged twice. Quiet it for the duration.
    logging.getLogger("blueprint_scenario_generator").setLevel(logging.WARNING)

    # The tutorial narrates its own seven build steps; that output belongs to
    # its own demo, not to this handoff log. Capture it so the researcher-to-
    # researcher story stays readable.
    with contextlib.redirect_stdout(io.StringIO()):
        model = tutorial.build_model(
            CESDM_ROOT / "schemas", CESDM_ROOT / "library" / "default_library.yaml"
        )

    workdir = cfg.workdir / "cesdm"
    workdir.mkdir(parents=True, exist_ok=True)
    baseline_yaml = workdir / "baseline_ch_neighbours_model.yaml"
    model.export_yaml(str(baseline_yaml))

    # The chain root must be an *enveloped* baseline scenario, not a bare model.
    # It is published to ESDH like any other dataset; the gateway's catalog-backed
    # reference loader finds it by its energy:definesScenario stamp when B's and
    # C's deltas name it as their parent. No filesystem staging is involved.
    # declare_scenario(model_path, metadata, scenario_type, out_dir, reference)
    # "baseline" below is the scenario *type* - a root scenario rather than a
    # delta - not a reference to cfg.raw_data_release.
    baseline_blueprint = Path(
        declare_scenario(
            str(baseline_yaml),
            {
                "scenario_id": "baseline_ch_neighbours",
                "scenario_name": "CH + neighbours baseline",
                "reference_scenario_id": "baseline_ch_neighbours",
                "baseline_scenario_id": "baseline_ch_neighbours",
                "author": cfg.researcher_a,
                "version": "1.0",
                "description": "CH + neighbours 2030 baseline system",
            },
            "baseline",  # scenario_type: root of the chain, not a delta
            str(workdir / "blueprint_base"),
            None,  # reference: no parent - this IS the root
        )
    )

    # Publish the chain root. This is what makes it resolvable: the gateway
    # stamps a Tier 3 upload with energy:definesScenario, and the catalog-backed
    # reference loader keys on that when B's and C's deltas name it as parent.
    try:
        res = client.upload_model(
            str(baseline_blueprint),
            owner=cfg.researcher_a,
            branch=cfg.raw_branch,
            version="1.0.0",
            description="CH + neighbours 2030 baseline scenario",
            force_tier="scenario",
            force=True,
            commit_message=f"{cfg.raw_data_release}: baseline scenario blueprint",
        )
        say(
            f"      published chain root {baseline_blueprint.name} -> {res['dataset_id']}"
        )
    except Exception as exc:
        logger.exception("Baseline blueprint publish failed")
        say(f"      !! could not publish the chain root: {str(exc)[:60]}")
    say(
        f"      baseline model: {sum(len(v) for v in model.entities.values() if v)} entities"
    )

    edits: dict[str, Any] = {"added": [], "modified": [], "profiles_bound": []}

    # ── Bind A's raw profiles as first-class CESDM Profile entities ──────────
    # This is the join between the two researchers: each Profile carries the
    # ESDH dataset_id and content hash of the file it represents, so the model
    # itself records which bytes it was parameterised from.
    for d in published:
        if d.dataset_id not in fetched:
            continue
        pid = f"profile.esdh.{d.dataset_id}"
        model.add_entity("Profile", pid)
        model.add_attribute(pid, "name", d.filename)
        model.add_attribute(
            pid, "data_reference", f"esdh://{cfg.repository}/{d.dataset_id}"
        )
        model.add_attribute(pid, "profile_unit", d.units)
        edits["profiles_bound"].append(
            {"profile_id": pid, "dataset_id": d.dataset_id, "sha256": d.sha256}
        )
    say(
        f"      bound {len(edits['profiles_bound'])} raw profile(s) as CESDM Profile entities"
    )

    # ── Edit 1: add 5 GW of Swiss solar PV ───────────────────────────────────
    pv_id = "pv.ch.expansion_2030"
    view_id = f"solar_dispatch_view.{pv_id}"
    model.add_entity("SolarGenerationUnit", pv_id)
    model.add_attribute(pv_id, "name", "CH solar PV expansion 2030")
    model.add_entity("Solar.DispatchView", view_id)
    model.add_relation(view_id, "representsAsset", pv_id)
    model.add_attribute(view_id, "nominal_power_capacity", cfg.solar_expansion_mw)
    edits["added"].append(
        {
            "entity": pv_id,
            "class": "SolarGenerationUnit",
            "nominal_power_capacity_mw": cfg.solar_expansion_mw,
        }
    )
    say(f"      added {pv_id} (+{cfg.solar_expansion_mw:,.0f} MW)")

    # ── Edit 2: uprate a cross-border interconnector ─────────────────────────
    # THIS is the "update_entity()" the brief asks for. That method does not
    # exist in the toolbox - re-calling add_attribute() on an existing entity id
    # overwrites the value, and declare_scenario reports it under
    # modified_components. Same outcome, real API.
    flow_views = list(model.entities.get("Interconnector.PowerFlowView", {}))
    if flow_views:
        target = flow_views[0]
        raw_before = model.entities["Interconnector.PowerFlowView"][target].data.get(
            "maximum_power_flow_1_to_2"
        )
        before = raw_before.get("value") if isinstance(raw_before, dict) else raw_before
        after = float(before or 0) + 2_000.0
        model.add_attribute(target, "maximum_power_flow_1_to_2", after)
        edits["modified"].append(
            {
                "entity": target,
                "attribute": "maximum_power_flow_1_to_2",
                "from": before,
                "to": after,
            }
        )
        say(f"      updated {target}: {before} -> {after} MW  (via add_attribute)")

    modified_yaml = workdir / "ch_res_expansion_2030.yaml"
    model.export_yaml(str(modified_yaml))

    # ── Delta blueprint: only what changed relative to the baseline ──────────
    metadata = {
        "scenario_id": "sub_CH_RES_expansion_2030",
        "scenario_name": "CH renewable expansion 2030",
        "reference_scenario_id": "baseline_ch_neighbours",
        "baseline_scenario_id": "baseline_ch_neighbours",
        "author": cfg.researcher_b,
        "version": "1.0",
        "description": f"+{cfg.solar_expansion_mw:,.0f} MW CH solar PV, uprated CH interconnector",
        "target_year": 2030,
        "geographic_region": "Switzerland",
        "temporal_resolution": "Hourly (8760 h/year)",
    }
    out_dir = workdir / "blueprint"
    blueprint = Path(
        declare_scenario(
            str(modified_yaml),
            metadata,
            "subscenario",
            str(out_dir),
            str(baseline_yaml),
        )
    )

    import yaml as _yaml

    content = (_yaml.safe_load(blueprint.read_text()) or {}).get("scenario_content", {})
    counts = {
        k: len(content.get(k) or {})
        for k in ("added_components", "modified_components", "deleted_components")
    }
    say(f"      delta blueprint: {blueprint.name}")
    say(
        f"        {counts}   ({blueprint.stat().st_size / 1024:.1f} KB vs "
        f"{modified_yaml.stat().st_size / 1024:.1f} KB for the full model)"
    )
    edits["delta_counts"] = counts

    return baseline_yaml, blueprint, edits


def b4_mock_solve(cfg: Config, edits: dict[str, Any]) -> dict[str, Any]:
    """Stand in for a PyPSA/Calliope run.

    Deterministic and clearly labelled: these are NOT solver results. The point
    is the lineage record around them, not the numbers.
    """
    say("")
    say(f"  B4. running the downstream solver (mock, {cfg.solver_version})")
    t0 = time.perf_counter()
    added_mw = sum(a.get("nominal_power_capacity_mw", 0) for a in edits["added"])
    # Crude monotonic responses so the numbers move with the scenario edit.
    metrics = {
        "total_cost_eur": round(1.32e9 - added_mw * 1.8e4, 1),
        "co2_emissions_mton": round(14.1 - added_mw * 3.4e-4, 2),
        "curtailment_rate": round(0.021 + added_mw * 6.0e-7, 4),
        "solver_wall_time_s": round(time.perf_counter() - t0, 3),
        "_disclaimer": "mock values - no solver was executed",
    }
    for k, v in metrics.items():
        if not k.startswith("_"):
            say(f"        {k:22s} {v}")
    return metrics


def b5_lineage_manifest(
    cfg: Config,
    published: list[PublishedDataset],
    blueprint: Path,
    edits: dict[str, Any],
    metrics: dict[str, Any],
    raw_release_commit: str | None,
) -> Path:
    """Write the manifest that makes the result back-traceable.

    Every input is pinned by BOTH its ESDH dataset_id and its content hash. The
    id says where it lives; the hash says the bytes have not changed since.
    """
    say("")
    say("  B5. writing lineage_manifest.json")
    manifest = {
        "experiment_id": cfg.experiment_id,
        "created_utc": cfg.started_at.isoformat(),
        "researcher": cfg.researcher_b,
        "repository": cfg.repository,
        "branch": cfg.experiment_branch,
        "base_dataset_commit": raw_release_commit or cfg.raw_data_release,
        "base_dataset_tag": cfg.raw_data_release,
        "curated_by": cfg.researcher_a,
        "input_datasets": [
            {
                "filename": d.filename,
                "dataset_id": d.dataset_id,
                "sha256": d.sha256,
                "rows": d.rows,
                "carrier": d.carrier,
                "units": d.units,
                "provenance": d.provenance,
                "esdh_uri": f"esdh://{cfg.repository}/{d.dataset_id}",
            }
            for d in published
        ],
        "cesdm_blueprint": {
            "filename": blueprint.name,
            "sha256": sha256_file(blueprint),
            "delta_counts": edits.get("delta_counts", {}),
        },
        "scenario_edits": {
            "added": edits["added"],
            "modified": edits["modified"],
            "profiles_bound": edits["profiles_bound"],
        },
        "solver": {
            "version": cfg.solver_version,
            "executed": False,
            "note": "mock run - metrics are illustrative, not solver output",
        },
        "output_summary": {k: v for k, v in metrics.items() if not k.startswith("_")},
    }
    path = cfg.workdir / "lineage_manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    say(f"      {path.relative_to(REPO_ROOT)} ({path.stat().st_size} bytes)")
    say(f"      pins {len(published)} input(s) by dataset_id + sha256")
    return path


def b6_publish(
    client: CESDHClient,
    cfg: Config,
    blueprint: Path,
    manifest: Path,
    published: list[PublishedDataset],
) -> list[str]:
    """Commit ONLY the blueprint and the manifest.

    Deliberately not uploaded: the full modified model (reconstructable from
    baseline + delta), the fetched input copies (already in ESDH under A's
    dataset_ids), and any solver intermediates. Re-uploading inputs would
    duplicate gigabytes and, worse, create a second copy that can silently
    diverge from the one the manifest pins.
    """
    say("")
    say(f"  B6. committing to '{cfg.experiment_branch}' - blueprint + manifest only")
    derived_from = [d.dataset_id for d in published]
    run_id = f"{cfg.experiment_id.replace('-', '_')}_{cfg.stamp}"
    ids: list[str] = []

    for path, tier, desc in (
        (
            blueprint,
            "scenario",
            "CESDM sub-scenario delta blueprint - CH RES expansion 2030",
        ),
        (
            manifest,
            "generic",
            "Lineage manifest - pins inputs, blueprint hash and outputs",
        ),
    ):
        try:
            res = client.upload_model(
                str(path),
                owner=cfg.researcher_b,
                branch=cfg.experiment_branch,
                version="1.0",
                description=desc,
                force_tier=tier,
                derived_from=derived_from,  # PROV-O: links back to A's datasets
                run_id=run_id,
                force=True,
                commit_message=f"{cfg.experiment_id}: {path.name}",
                dcat_metadata={
                    "theme": "energy",
                    "spatial_coverage": "CH",
                    "dcterms:conformsTo": "https://cesdm.ethz.ch/schema/v4",
                    "energy:experimentId": cfg.experiment_id,
                    "keywords": ["scenario", "ch", "solar", "expansion-2030"],
                },
            )
            if res.get("status") == "validation_failed":
                say(
                    f"      !! rejected {path.name}: "
                    f"{(res.get('validation_hard_errors') or ['?'])[0][:50]}"
                )
                continue
            ids.append(res["dataset_id"])
            say(f"      OK {path.name:44s} -> {res['dataset_id']}")
        except Exception as exc:
            logger.exception("Publish failed for %s", path.name)
            say(f"      !! {path.name}: {str(exc)[:60]}")

    try:
        tag = client.create_tag(cfg.experiment_tag, ref=cfg.experiment_branch)
        say(f"      tagged '{cfg.experiment_tag}' -> {tag['commit_id'][:16]}...")
    except Exception as exc:
        if _status(exc) != 409:
            say(f"      !! tag: {str(exc)[:60]}")
    return ids


# ═════════════════════════════════════════════════════════════════════════════
# PHASE C - Researcher C: re-analysis on top of B's published scenario
# ═════════════════════════════════════════════════════════════════════════════
#
# C never spoke to A or B. Everything C needs is discoverable from the catalog:
# B's manifest names the inputs and the blueprint, and the blueprint names its
# own parent. C branches off B, adds a storage sensitivity, and publishes a
# CHAINED sub-scenario - a delta whose reference is itself a delta.
#
# That chain is the part that used to break. `build_model_from_sub_scenario`
# deep-copies whatever reference it is handed, so pointing it at B's delta
# produced delta-on-delta: 1 entity inflated instead of 207, reported as
# "success" with no warning. The gateway now walks the reference chain down to a
# real baseline before folding (services/scenario_inflation._resolve_reference_chain),
# and refuses outright if the chain is broken or cyclic.


def phase_c_reanalysis(
    client: CESDHClient,
    cfg: Config,
    published: list[PublishedDataset],
    b_manifest_id: str | None,
) -> tuple[str | None, dict[str, Any]]:
    """Discover B's work from the catalog, extend it, and publish a chained delta."""
    banner("PHASE C - Researcher C (Third-Party Re-Analyst)")

    say("  C1. discovering Researcher B's experiment from the catalog")
    if not b_manifest_id:
        say("      no manifest from B - nothing to build on")
        return None, {}
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "b_manifest.json"
        try:
            client.download_to_file(b_manifest_id, str(dest))
            b_manifest = json.loads(dest.read_text())
        except Exception as exc:
            say(f"      !! could not read B's manifest: {str(exc)[:60]}")
            return None, {}
    say(f"      experiment {b_manifest['experiment_id']} by {b_manifest['researcher']}")
    say(
        f"      built on   {b_manifest['base_dataset_tag']} curated by {b_manifest['curated_by']}"
    )
    say(
        f"      inputs     {len(b_manifest['input_datasets'])} dataset(s), each hash-pinned"
    )

    say("")
    say(f"  C2. branching '{cfg.reanalysis_branch}' off '{cfg.experiment_branch}'")
    try:
        head = client.create_branch(cfg.reanalysis_branch, source=cfg.experiment_branch)
        say(f"      created at {str(head.get('head'))[:16]}...")
    except Exception as exc:
        if _status(exc) == 409:
            say("      already exists - reusing")
        else:
            say(f"      !! {str(exc)[:60]}")

    say("")
    say("  C3. re-verifying B's inputs before trusting them")
    expected = {d.dataset_id: d.sha256 for d in published}
    verified = 0
    with tempfile.TemporaryDirectory() as tmp:
        for entry in b_manifest["input_datasets"]:
            dest = Path(tmp) / entry["filename"]
            try:
                client.download_to_file(entry["dataset_id"], str(dest))
                ok = (
                    sha256_file(dest)
                    == entry["sha256"]
                    == expected.get(entry["dataset_id"], entry["sha256"])
                )
            except Exception:
                ok = False
            verified += int(ok)
            say(
                f"      {'OK ' if ok else '!! '}{entry['filename']:38s}"
                f"{'hash matches B' if ok else 'MISMATCH'}"
            )

    say("")
    say("  C4. extending B's scenario with a storage sensitivity")
    for p in (CESDM_ROOT, CESDM_ROOT / "tools", CESDM_ROOT / "examples"):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    import tutorial_ch_neighbours as tutorial
    from tools.scenario_manager import declare_scenario

    logging.getLogger("blueprint_scenario_generator").setLevel(logging.WARNING)

    with contextlib.redirect_stdout(io.StringIO()):
        model = tutorial.build_model(
            CESDM_ROOT / "schemas", CESDM_ROOT / "library" / "default_library.yaml"
        )

    workdir = cfg.workdir / "cesdm"
    b_model_yaml = workdir / "ch_res_expansion_2030.yaml"  # B's full model state

    # C reproduces B's edits, then adds their own. In a real handoff C would
    # inflate B's blueprint; here B's full model is on disk from Phase B, which
    # is the same content by construction.
    model.add_entity("SolarGenerationUnit", "pv.ch.expansion_2030")
    model.add_attribute("pv.ch.expansion_2030", "name", "CH solar PV expansion 2030")

    bat_id = "bat.ch.sensitivity_2030"
    view_id = f"storage_dispatch_view.{bat_id}"
    model.add_entity("StorageUnit", bat_id)
    model.add_attribute(bat_id, "name", "CH grid battery - C sensitivity")
    model.add_entity("Storage.DispatchView", view_id)
    model.add_relation(view_id, "representsAsset", bat_id)
    model.add_attribute(view_id, "energy_storage_capacity", 4_000.0)
    model.add_attribute(view_id, "nominal_power_capacity", 1_000.0)
    say(f"      added {bat_id} (4 GWh / 1 GW)")

    c_model = workdir / "ch_storage_sensitivity_2030.yaml"
    model.export_yaml(str(c_model))

    # The chained blueprint: its reference is B's SUB-SCENARIO, not the baseline.
    metadata = {
        "scenario_id": "sub_CH_storage_sensitivity_2030",
        "scenario_name": "CH storage sensitivity on top of RES expansion",
        "reference_scenario_id": "sub_CH_RES_expansion_2030",  # <- B's scenario
        "baseline_scenario_id": "baseline_ch_neighbours",
        "author": cfg.researcher_c,
        "version": "1.0",
        "description": "+4 GWh grid battery on top of B's +5 GW solar expansion",
        "target_year": 2030,
        "geographic_region": "Switzerland",
    }
    blueprint = Path(
        declare_scenario(
            str(c_model),
            metadata,
            "subscenario",
            str(workdir / "blueprint_c"),
            str(b_model_yaml),
        )
    )
    import yaml as _yaml

    content = (_yaml.safe_load(blueprint.read_text()) or {}).get("scenario_content", {})
    counts = {
        k: len(content.get(k) or {})
        for k in ("added_components", "modified_components", "deleted_components")
    }
    say(
        f"      chained delta vs B: {counts}  ({blueprint.stat().st_size / 1024:.1f} KB)"
    )
    say(
        f"      reference_scenario_id = {metadata['reference_scenario_id']} "
        f"(a sub-scenario - chain depth 2)"
    )

    say("")
    say("  C5. mock re-analysis")
    metrics = {
        "total_cost_eur": 1.19e9,
        "co2_emissions_mton": 11.6,
        "curtailment_rate": 0.011,
        "_disclaimer": "mock values - no solver was executed",
    }
    for k, v in metrics.items():
        if not k.startswith("_"):
            say(f"        {k:22s} {v}")

    say("")
    say("  C6. publishing the chained blueprint + C's manifest")
    c_manifest = {
        "experiment_id": "EXP-2030-CH-STORAGE-002",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "researcher": cfg.researcher_c,
        "repository": cfg.repository,
        "branch": cfg.reanalysis_branch,
        "builds_on": {
            "experiment_id": b_manifest["experiment_id"],
            "researcher": b_manifest["researcher"],
            "manifest_dataset_id": b_manifest_id,
            "blueprint_sha256": b_manifest["cesdm_blueprint"]["sha256"],
        },
        "base_dataset_tag": b_manifest["base_dataset_tag"],
        "curated_by": b_manifest["curated_by"],
        "input_datasets": b_manifest["input_datasets"],  # unchanged, still pinned
        "inputs_reverified": verified,
        "cesdm_blueprint": {
            "filename": blueprint.name,
            "sha256": sha256_file(blueprint),
            "reference_scenario_id": metadata["reference_scenario_id"],
            "chain_depth": 2,
            "delta_counts": counts,
        },
        "solver": {
            "version": cfg.solver_version,
            "executed": False,
            "note": "mock run - metrics are illustrative",
        },
        "output_summary": {k: v for k, v in metrics.items() if not k.startswith("_")},
    }
    manifest_path = cfg.workdir / "lineage_manifest_c.json"
    manifest_path.write_text(json.dumps(c_manifest, indent=2))

    derived = [d.dataset_id for d in published]
    if b_manifest_id:
        derived.append(b_manifest_id)  # C derives from B's manifest too
    run_id = f"EXP_2030_CH_STORAGE_002_{cfg.stamp}"
    c_ids: list[str] = []
    for path, tier, desc in (
        (
            blueprint,
            "scenario",
            "Chained CESDM delta - storage sensitivity on B's scenario",
        ),
        (manifest_path, "generic", "Lineage manifest - C's re-analysis, chained to B"),
    ):
        try:
            res = client.upload_model(
                str(path),
                owner=cfg.researcher_c,
                branch=cfg.reanalysis_branch,
                version="1.0",
                description=desc,
                force_tier=tier,
                derived_from=derived,
                run_id=run_id,
                force=True,
                commit_message=f"EXP-2030-CH-STORAGE-002: {path.name}",
            )
            if res.get("status") == "validation_failed":
                say(f"      !! rejected {path.name}")
                continue
            c_ids.append(res["dataset_id"])
            say(f"      OK {path.name:46s} -> {res['dataset_id']}")
        except Exception as exc:
            say(f"      !! {path.name}: {str(exc)[:60]}")

    # The decisive check: did the CHAINED blueprint inflate to a full system?
    say("")
    say("  C7. verifying the chained inflation (the bug this used to hide)")
    if c_ids:
        nodes = client.get_system_nodes(c_ids[0])
        full = len(nodes) > 100
        say(f"      entities in C's system graph: {len(nodes)}")
        say(
            "      "
            + (
                "full system reconstructed through the chain"
                if full
                else "!! only the delta landed - chain resolution FAILED"
            )
        )
        c_manifest["chain_inflation_entities"] = len(nodes)
        manifest_path.write_text(json.dumps(c_manifest, indent=2))
    return (c_ids[-1] if len(c_ids) == 2 else None), c_manifest


# ═════════════════════════════════════════════════════════════════════════════
# Verification - can a reviewer get back to the inputs?
# ═════════════════════════════════════════════════════════════════════════════


def verify_back_trace(
    client: CESDHClient,
    cfg: Config,
    manifest_id: str | None,
    published: list[PublishedDataset],
) -> bool:
    """Start from the published manifest and re-derive every input, hash-checked."""
    banner("VERIFICATION - back-tracing the published result", "-")
    if not manifest_id:
        say("  no manifest was published - cannot back-trace")
        return False

    say(f"  1. fetch the manifest from ESDH ({manifest_id})")
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "lineage_manifest.json"
        try:
            client.download_to_file(manifest_id, str(dest))
            manifest = json.loads(dest.read_text())
        except Exception as exc:
            say(f"     !! {exc}")
            return False

    say(
        f"  2. it names raw-data release '{manifest['base_dataset_tag']}' "
        f"curated by {manifest['curated_by']}"
    )

    say(
        f"  3. re-fetch each of its {len(manifest['input_datasets'])} input(s) and verify bytes"
    )
    expected = {d.dataset_id: d.sha256 for d in published}
    all_ok = True
    with tempfile.TemporaryDirectory() as tmp:
        for entry in manifest["input_datasets"]:
            dest = Path(tmp) / entry["filename"]
            try:
                client.download_to_file(entry["dataset_id"], str(dest))
                got = sha256_file(dest)
            except Exception as exc:
                say(f"     !! {entry['filename']}: {str(exc)[:50]}")
                all_ok = False
                continue
            ok = (
                got
                == entry["sha256"]
                == expected.get(entry["dataset_id"], entry["sha256"])
            )
            all_ok &= ok
            say(
                f"     {'OK ' if ok else '!! '}{entry['filename']:38s} "
                f"{'hash matches manifest' if ok else 'HASH MISMATCH'}"
            )

    say("  4. PROV-O chain recorded by the gateway:")
    try:
        lineage = client.get_scenario_lineage("sub_CH_RES_expansion_2030")
        say(f"     {len(lineage)} lineage row(s) from the catalog")
    except Exception:
        say(
            "     (scenario lineage not populated - derived_from is recorded on the dataset)"
        )

    return all_ok


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("MR_LOG_LEVEL", "WARNING"),
        format="%(levelname)s %(name)s: %(message)s",
    )
    cfg = Config()
    cfg.workdir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()

    banner("Multi-researcher CESDM workflow on ESDH")
    say(f"  repository        {cfg.repository}")
    say(f"  raw branch        {cfg.raw_branch}   (release {cfg.raw_data_release})")
    say(f"  experiment branch {cfg.experiment_branch}")
    say(f"  researchers       {cfg.researcher_a} -> {cfg.researcher_b}")

    client_a = CESDHClient(
        repository=cfg.repository,
        gateway_url=cfg.gateway_url,
        lakefs_url=cfg.lakefs_url,
    )
    published = phase_a_publish_raw(client_a, cfg)
    if not published:
        say("\n  !! Researcher A published nothing - is the stack up?")
        return 1

    # A second client instance: Researcher B is a different person with their own
    # session. Same repository, no shared in-process state beyond the dataset_ids.
    client_b = CESDHClient(
        repository=cfg.repository,
        gateway_url=cfg.gateway_url,
        lakefs_url=cfg.lakefs_url,
    )

    banner("PHASE B - Researcher B (Modeler & Analyst)")
    b1_branch(client_b, cfg)
    fetched = b2_fetch_inputs(client_b, cfg, published)
    if not fetched:
        say("\n  !! could not fetch any input - aborting")
        return 1

    baseline_yaml, blueprint, edits = b3_build_scenario(
        client_b, cfg, published, fetched
    )
    metrics = b4_mock_solve(cfg, edits)

    raw_release_commit = None
    try:
        raw_release_commit = next(
            (
                t["commit_id"]
                for t in client_b.list_tags()
                if t["tag"] == cfg.raw_data_release
            ),
            None,
        )
    except Exception:
        pass

    manifest = b5_lineage_manifest(
        cfg, published, blueprint, edits, metrics, raw_release_commit
    )
    ids = b6_publish(client_b, cfg, blueprint, manifest, published)
    manifest_id = ids[-1] if len(ids) == 2 else (ids[0] if ids else None)

    client_c = CESDHClient(
        repository=cfg.repository,
        gateway_url=cfg.gateway_url,
        lakefs_url=cfg.lakefs_url,
    )
    c_manifest_id, c_manifest = phase_c_reanalysis(
        client_c, cfg, published, manifest_id
    )

    ok = verify_back_trace(client_b, cfg, manifest_id, published)

    # ── Summary ──────────────────────────────────────────────────────────────
    banner("SUMMARY")
    blueprint_kb = blueprint.stat().st_size / 1024
    full_kb = (
        cfg.workdir / "cesdm" / "ch_res_expansion_2030.yaml"
    ).stat().st_size / 1024
    rows = [
        ("repository", cfg.repository),
        ("raw release", f"{cfg.raw_branch} @ {cfg.raw_data_release}"),
        ("experiment", f"{cfg.experiment_branch} @ {cfg.experiment_tag}"),
        ("raw datasets published", f"{len(published)} by {cfg.researcher_a}"),
        ("inputs re-fetched + verified", f"{len(fetched)}/{len(published)}"),
        (
            "scenario edits",
            f"{len(edits['added'])} added, {len(edits['modified'])} modified",
        ),
        ("profiles bound", str(len(edits["profiles_bound"]))),
        (
            "delta blueprint",
            f"{blueprint_kb:.1f} KB vs {full_kb:.1f} KB full model "
            f"({blueprint_kb / full_kb:.0%})",
        ),
        ("artifacts committed by B", f"{len(ids)} (blueprint + manifest only)"),
        ("researcher C re-analysis", f"{cfg.reanalysis_branch}"),
        ("C chain depth", "2 (delta on B's delta)"),
        (
            "C chained inflation",
            f"{c_manifest.get('chain_inflation_entities', 'n/a')} entities",
        ),
        ("back-trace", "OK - every input hash re-verified" if ok else "FAILED"),
        ("elapsed", f"{time.perf_counter() - t0:.1f}s"),
    ]
    width = max(len(k) for k, _ in rows)
    for k, v in rows:
        say(f"  {k.ljust(width)} : {v}")

    say("")
    say("  What was deliberately NOT uploaded")
    say("    - the full modified model  (reconstructable: baseline + delta)")
    say("    - the fetched input copies (already in ESDH under A's dataset_ids)")
    say("    - solver intermediates     (regenerable from blueprint + solver version)")

    say("")
    chain_ok = c_manifest.get("chain_inflation_entities", 0) > 100
    say("")
    say(
        f"  verification: {'ALL CHECKS PASSED' if (ok and chain_ok) else 'SOME CHECKS FAILED'}"
    )
    return 0 if (ok and chain_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
