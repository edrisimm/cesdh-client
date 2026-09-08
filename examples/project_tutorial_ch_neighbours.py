#!/usr/bin/env python3
"""
project_tutorial_ch_neighbours.py
==================================

Runs the CESDM "Switzerland + neighbours" tutorial and persists every artifact
it produces into CESDH - the Semantic Lakehouse that versions, catalogues and
provenance-tracks energy-model data. The CESDM model code is *imported and
executed*, not re-implemented: this script calls
``examples/tutorial_ch_neighbours.py::build_model()`` directly, exports the
resulting ``CesdmModel`` in several formats, and pushes each export through the
CESDH gateway onto an isolated, per-run LakeFS branch.

Run
---
    # 1. start the stack (there is no Makefile in this repo yet)
    docker compose up --build
    docker exec dlm-ollama ollama pull llama3.1:8b   # only needed for Section 8

    # 2. run this example
    python examples_cesdh/project_tutorial_ch_neighbours.py

What you'll see
---------------
    1. A plain-language explanation of what CESDH is and why it is here.
    2. The real CESDM tutorial building the 5-country model (its own step log).
    3. Seven artifacts exported to bytes: YAML x2, JSON x2, CSV, HDF5,
       datapackage - plus Parquet when pyarrow is installed.
    4. An upload line per artifact:  OK uploaded ch_neighbours_2030.yaml (62.0 KB)
    5. Commit SHAs for the run branch and the tag name that identifies it.
    6. A catalogue search and a byte-for-byte SHA-256 round-trip check.
    7. A summary table and a few suggestions for what to try next.

Nothing here is destructive and the script is safe to re-run: each run gets its
own branch, and uploads use force=True so a repeated run on a pinned branch
supersedes rather than collides.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, NamedTuple

# ── Optional pretty output ───────────────────────────────────────────────────
# `rich` is not in src/requirements.txt, so it is strictly optional: if it is
# importable we use it, otherwise everything degrades to plain print().
try:  # pragma: no cover - presentation only
    from rich.console import Console as _RichConsole
    from rich.table import Table as _RichTable

    _CONSOLE: Any | None = _RichConsole()
    RICH_AVAILABLE = True
except ImportError:  # pragma: no cover - presentation only
    _CONSOLE = None
    _RichTable = None  # type: ignore[assignment]
    RICH_AVAILABLE = False

logger = logging.getLogger(__name__)

ArtifactKind = Literal["yaml", "csv", "json", "h5", "parquet"]


def say(message: str = "") -> None:
    """Print a line through rich when available, plain stdout otherwise.

    markup=False matters: rich reads square brackets as style tags, so a literal
    "(a) ..." label or "[uploaded]" status would be silently swallowed as markup.
    highlight=False stops rich from recolouring numbers and paths. Together they
    make the rich path render byte-identical text to the plain fallback.
    """
    if RICH_AVAILABLE and _CONSOLE is not None:
        _CONSOLE.print(message, markup=False, highlight=False)
    else:
        print(message, flush=True)


def banner(title: str) -> None:
    say("")
    say("=" * 78)
    say(f"  {title}")
    say("=" * 78)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2 - What is CESDH, and why are we using it here?
# ═════════════════════════════════════════════════════════════════════════════

WHAT_IS_CESDH = """
CESDH is a place to put the files your energy models produce, so that later on
you can still find them, trust them, and prove where they came from.

Here is the problem it solves. You just ran the CESDM tutorial. It built a
5-country electricity model and produced a pile of outputs: a YAML dump of the
model, a JSON version, a table of every attribute, an HDF5 grid, a datapackage
manifest. Now what? Usually those land in output/ on somebody's laptop. Two
months later nobody can answer simple questions. Which of these seven YAML
files is the one in the paper? Is this version 3 or version 7? Who produced it?
Which scenario does it belong to? Was it made before or after we fixed the
hydro efficiencies? The files are still there, but the *meaning* around them is
gone.

CESDH keeps that meaning attached to the files. It has four parts:

  MinIO    the warehouse. Boxes of stuff on shelves. It stores the actual
           bytes and does not care what is inside them.
  LakeFS   the logbook over the warehouse. Every change is a commit on a
           branch, so you can put this run on its own branch, compare it with
           last week's, and roll back if it was wrong.
  Fuseki   the smart catalogue. Not just filenames, but facts: this file is a
           hydro reservoir dataset, for Switzerland, from run X, uploaded by
           Y, derived from Z. You can then ask questions like "find me every
           hydro reservoir dataset in Switzerland" and get real answers.
  Gateway  the front desk. One door in and one door out. You hand it a file
  + SDK    and it does the rest: stores the bytes, writes the catalogue entry,
           records who you are. You never touch MinIO or LakeFS directly, and
           you never need their credentials.

The analogy that usually lands: CESDH is GitHub for energy-model data. Branches,
commits and history, but built for multi-gigabyte HDF5 grids instead of source
files, and with a searchable catalogue bolted on so the data describes itself.

The takeaway: CESDH lets your CESDM runs be reproducible, searchable and
auditable without changing how you write the model. Notice that Section 4 below
imports the tutorial unchanged and calls it. The modelling code does not know
CESDH exists.
"""


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3 - Setup and configuration
# ═════════════════════════════════════════════════════════════════════════════
#
# REAL SYMBOLS, REAL PORTS.  Everything below was checked against the source
# rather than assumed, because several plausible-looking guesses are wrong:
#
#   * The SDK class is `EnergyDLMClient`, re-exported from the `cesdh` package
#     as `CESDHClient` (src/sdk/__init__.py). There is no `cesdh.sdk` module.
#   * The gateway listens on :8080, NOT :8000 - :8000 is LakeFS (and the LakeFS
#     UI, so Section 9 points you at :8000 rather than :8001).
#   * MinIO is :9000 for the S3 API and :9001 for the console. Fuseki is :3030.
#   * `repository` is REQUIRED on every call. It is validated as an S3 bucket
#     name (lowercase alphanumeric + hyphens) by
#     src/gateway/dependencies.py::valid_repository, so the underscore form
#     `project_tutorial_ch_neighbours` is rejected with HTTP 422. We use the
#     hyphenated `project-tutorial-ch-neighbours` instead.

try:
    from cesdh import CESDHClient  # alias of EnergyDLMClient
except ImportError as exc:  # pragma: no cover - setup guidance
    raise SystemExit(
        "Could not import the CESDH SDK.\n"
        "Install it in editable mode from the repository root:\n"
        "    pip install -e src/sdk\n"
        f"(original error: {exc})"
    ) from exc

# Repository root = parent of examples_cesdh/
REPO_ROOT = Path(__file__).resolve().parents[1]
CESDM_ROOT = REPO_ROOT / "cesdm" / "sweet-cosi-cesdm"


@dataclass(frozen=True)
class Config:
    """Connection settings and run identity, all overridable by environment."""

    # Only gateway_url and lakefs_url are used by the SDK; the MinIO and Fuseki
    # endpoints are recorded so the summary can tell you where to go looking.
    gateway_url: str = os.environ.get(
        "CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080"
    )
    lakefs_url: str = os.environ.get("LAKEFS_URL", "http://localhost:8000")
    minio_url: str = os.environ.get("MINIO_URL", "http://localhost:9000")
    minio_console_url: str = os.environ.get(
        "MINIO_CONSOLE_URL", "http://localhost:9001"
    )
    fuseki_url: str = os.environ.get("FUSEKI_URL", "http://localhost:3030")

    # Bucket-safe: lowercase alphanumeric and hyphens only.
    repository: str = os.environ.get(
        "CESDH_REPOSITORY", "project-tutorial-ch-neighbours"
    )
    # owner must not contain spaces - validated in src/sdk/__init__.py.
    owner: str = os.environ.get("CESDH_OWNER", "FEN-team")
    version: str = os.environ.get("CESDH_VERSION", "1.0")
    # The tutorial builds the 2030 horizon (see its ch_neighbours_2030 exports).
    # Surfaced as scenario metadata so the catalog can tag the run with the year.
    target_year: int = int(os.environ.get("CESDH_TARGET_YEAR", "2030"))

    # Set CESDH_RUN_BRANCH to pin a branch and re-upload into it (force=True
    # supersedes the previous datasets); otherwise a fresh run-<UTC> branch.
    run_branch_override: str | None = os.environ.get("CESDH_RUN_BRANCH")
    search_timeout: int = int(os.environ.get("CESDH_SEARCH_TIMEOUT", "180"))

    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def stamp(self) -> str:
        return self.started_at.strftime("%Y%m%d-%H%M%S")

    @property
    def run_branch(self) -> str:
        """Per-run branch so every execution is isolated and reviewable."""
        return self.run_branch_override or f"run-{self.stamp}"

    @property
    def tag_name(self) -> str:
        """The immutable reference you would cite in a paper or report."""
        return f"ch-neighbours-{self.stamp}"


class Artifact(NamedTuple):
    """One file produced by the CESDM run, already serialized to bytes.

    `force_tier` is an optional fifth field with a default, so the documented
    four-field construction `Artifact(name, data, kind, description)` still
    works. It maps to the gateway's three-tier ingestion override
    (see src/ingestion/CLAUDE.md): None lets structural fingerprinting decide,
    "generic" pins Tier 1 for tabular or binary payloads that are not CESDM
    system models and should not be schema-validated as if they were.
    """

    name: str
    data: bytes
    kind: ArtifactKind
    description: str
    force_tier: str | None = None

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


def human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GB"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4 - Run the CESDM tutorial
# ═════════════════════════════════════════════════════════════════════════════
#
# The tutorial is imported and executed as-is. It expects its own package root
# and tools/ directory on sys.path (it does this itself when run as __main__),
# so we replicate that here before importing.


def _import_tutorial() -> Any:
    """Import cesdm/sweet-cosi-cesdm/examples/tutorial_ch_neighbours.py."""
    for extra in (CESDM_ROOT, CESDM_ROOT / "tools", CESDM_ROOT / "examples"):
        path = str(extra)
        if path not in sys.path:
            sys.path.insert(0, path)
    import tutorial_ch_neighbours as tutorial

    return tutorial


def _export_bytes(export_call: Callable[[str], Any], filename: str) -> bytes:
    """Run a CesdmModel.export_* method that writes one file, return its bytes.

    The CESDM exporters write to disk; CESDH wants bytes. We bridge through a
    temporary directory so the example holds artifacts purely in memory, as
    required, without depending on the tutorial's own output/ folder.
    """
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / filename
        export_call(str(target))
        return target.read_bytes()


def build_model_and_collect_artifacts(cfg: Config) -> tuple[Any, list[Artifact]]:
    """Execute the real tutorial, then serialize its outputs to Artifacts."""
    tutorial = _import_tutorial()

    schema_dir = CESDM_ROOT / "schemas"
    library_path = CESDM_ROOT / "library" / "default_library.yaml"

    say("Building the CH + neighbours model (tutorial output follows)...")
    say("")
    model = tutorial.build_model(schema_dir, library_path)

    errors = model.validate()
    if errors:
        say(f"  Model reported {len(errors)} validation issue(s):")
        for err in errors[:5]:
            say(f"    ! {err}")
    else:
        say("  Model is valid (0 errors).")

    artifacts: list[Artifact] = []

    # Each entry: (filename, kind, description, exporter, force_tier)
    # YAML/JSON dumps of the system model are genuine CESDM payloads, so we let
    # structural fingerprinting route them (they land in Tier 2 and are fully
    # shredded into the knowledge graph). The derived tabular/binary/schema
    # exports are pinned to Tier 1 - they are outputs, not system models.
    plan: list[tuple[str, ArtifactKind, str, Callable[[str], Any], str | None]] = [
        (
            "ch_neighbours_2030.yaml",
            "yaml",
            "CESDM system model, hierarchical YAML (views nested under assets)",
            model.export_yaml_hierarchical,
            None,
        ),
        (
            "ch_neighbours_2030_flat.yaml",
            "yaml",
            "CESDM system model, flat YAML (one section per class, views first-class)",
            model.export_yaml,
            None,
        ),
        (
            "ch_neighbours_2030.json",
            "json",
            "CESDM system model serialized as JSON",
            model.export_json,
            None,
        ),
        (
            "ch_neighbours_2030_long.csv",
            "csv",
            "Long-format table: one row per entity/attribute pair",
            model.export_long_csv,
            "generic",
        ),
        (
            "ch_neighbours_2030.h5",
            "h5",
            "HDF5 export of the model, one group per entity class",
            model.export_hdf5,
            "generic",
        ),
        (
            "ch_neighbours_2030_schema.json",
            "json",
            "JSON Schema describing the CESDM classes used by this model",
            model.export_json_schema,
            "generic",
        ),
    ]

    for filename, kind, description, exporter, tier in plan:
        try:
            payload = _export_bytes(exporter, filename)
            artifacts.append(Artifact(filename, payload, kind, description, tier))
            say(f"  exported {filename} ({human_bytes(len(payload))})")
        except Exception:
            logger.exception("Export failed for %s - skipping this artifact", filename)
            say(f"  skipped  {filename} (export failed, see log)")

    # Frictionless datapackage: writes a directory, so it is handled separately
    # and we keep the manifest, which is the part worth cataloguing.
    try:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "frictionless"
            model.export_frictionless(
                str(out_dir),
                name="tutorial-ch-neighbours-2030",
                title="CH + Neighbours 2030 - CESDM Tutorial",
            )
            manifest = out_dir / "datapackage.json"
            payload = manifest.read_bytes()
            n_resources = len(list(out_dir.rglob("*.csv")))
            artifacts.append(
                Artifact(
                    "datapackage.json",
                    payload,
                    "json",
                    f"Frictionless Data Package manifest ({n_resources} CSV resources)",
                    "generic",
                )
            )
            say(f"  exported datapackage.json ({human_bytes(len(payload))})")
    except Exception:
        logger.exception("Frictionless export failed - skipping")
        say("  skipped  datapackage.json (export failed, see log)")

    # Parquet needs pyarrow, which is not a hard dependency of this repo. This
    # is a deliberate demonstration of the "never crash on one artifact" rule.
    try:
        payload = _export_bytes(model.export_parquet, "ch_neighbours_2030.parquet")
        artifacts.append(
            Artifact(
                "ch_neighbours_2030.parquet",
                payload,
                "parquet",
                "Columnar Parquet export of the model tables",
                "generic",
            )
        )
        say(f"  exported ch_neighbours_2030.parquet ({human_bytes(len(payload))})")
    except ImportError:
        say("  skipped  ch_neighbours_2030.parquet (pyarrow not installed - optional)")
    except Exception:
        logger.exception("Parquet export failed - skipping")
        say("  skipped  ch_neighbours_2030.parquet (export failed, see log)")

    # A small provenance sidecar describing the run itself.
    #
    # Every top-level value is deliberately a SCALAR. The gateway runs semantic
    # shredding on any .json it accepts, regardless of tier, and the shredder
    # reads a top-level mapping as {entity_class: {entity_id: {...}}}. A nested
    # dict of counts would therefore be walked as if it were CESDM entities and
    # log "'int' object has no attribute 'get'". Keeping the record flat means
    # the shredder skips it cleanly (non-dict values are ignored), and the file
    # is still perfectly readable as JSON.
    try:
        summary = model_summary(model)
        counts = {cls: len(ents) for cls, ents in model.entities.items() if ents}
        payload = json.dumps(
            {
                "run_started_utc": cfg.started_at.isoformat(),
                "owner": cfg.owner,
                "repository": cfg.repository,
                "branch": cfg.run_branch,
                "source_tutorial": "cesdm/sweet-cosi-cesdm/examples/tutorial_ch_neighbours.py",
                "validation_errors": len(errors),
                "summary": summary,
                "entity_class_count": len(counts),
                "entity_total": sum(counts.values()),
                "entity_counts": "; ".join(
                    f"{cls}={n}"
                    for cls, n in sorted(counts.items(), key=lambda kv: -kv[1])
                ),
            },
            indent=2,
        ).encode("utf-8")
        artifacts.append(
            Artifact(
                "run_manifest.json",
                payload,
                "json",
                "Run manifest: who ran the tutorial, when, and what it produced",
                "generic",
            )
        )
        say(f"  exported run_manifest.json ({human_bytes(len(payload))})")
    except Exception:
        logger.exception("Run manifest generation failed - skipping")

    return model, artifacts


def scenario_metadata_from_model(model: Any, cfg: Config) -> dict[str, Any]:
    """Describe the modelled system so the catalog can index it for discovery.

    The gateway derives dcat:keyword tags from entity classes automatically, but
    entity CLASS names carry no geography: the tutorial's countries live in
    GeographicalRegion *instances* ("Switzerland", "France", ...), which the
    catalog never sees. Passing them as scenario_metadata.geographic_region is
    what lets a search for "Switzerland hydro reservoir" match this run.

    The region list is read from the model rather than hardcoded, so it stays
    correct if the tutorial adds or drops a country.
    """
    # Reuse the tutorial's own attribute accessor rather than reaching into the
    # entity internals: CESDM entities hold their values in `.data`, not in an
    # `.attributes` list.
    tutorial = _import_tutorial()
    regions: list[str] = []
    for entity in model.entities.get("GeographicalRegion", {}).values():
        name = tutorial._av(entity, "name", None)
        if name:
            regions.append(str(name))

    metadata: dict[str, Any] = {"target_year": cfg.target_year}
    if regions:
        metadata["geographic_region"] = ", ".join(regions)
    return metadata


def model_summary(model: Any) -> str:
    """One-line model summary used in the commit message and the manifest."""
    counts = {cls: len(ents) for cls, ents in model.entities.items() if ents}

    def total(*classes: str) -> int:
        return sum(counts.get(c, 0) for c in classes)

    regions = counts.get("GeographicalRegion", 0)
    generators = total(
        "ThermalGenerationUnit",
        "NuclearGenerationUnit",
        "WindGenerationUnit",
        "SolarGenerationUnit",
        "GenericGenerationUnit",
    )
    hydro = counts.get("HydroGenerationUnit", 0)
    storage = total("ReservoirStorageUnit", "StorageUnit")
    links = counts.get("Interconnector", 0)
    return (
        f"{regions} countries, {generators} generators, {hydro} hydro units, "
        f"{storage} storage units, {links} interconnectors"
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5 - Create a CESDH repository (project root) and a per-run branch
# ═════════════════════════════════════════════════════════════════════════════
#
# Both the repository and the branch are created EXPLICITLY here, using the
# strict lifecycle calls: they raise HTTP 409 when the target already exists,
# which is what makes a typo'd repository name fail loudly instead of quietly
# provisioning a second project. (Uploads still autoprovision on first write, so
# this section is a pre-flight check rather than a hard requirement.)
#
# Treating 409 as "already there, carry on" is what keeps the script idempotent.


def _status_code(exc: Exception) -> int | None:
    """HTTP status behind an SDK error, or None if it was not an HTTP failure.

    Read off the exception rather than importing requests, so this example
    keeps to the SDK-as-Standard rule of never calling the gateway directly.
    """
    return getattr(getattr(exc, "response", None), "status_code", None)


def prepare_repository(client: CESDHClient, cfg: Config) -> dict[str, Any]:
    """Explicitly create the project repository and the per-run branch."""
    state: dict[str, Any] = {"repository_existed": False, "branch_existed": False}

    try:
        client.create_repository()
        say(f"  created repository '{cfg.repository}' (+ its MinIO bucket)")
    except Exception as exc:
        if _status_code(exc) == 409:
            state["repository_existed"] = True
            say(f"  repository '{cfg.repository}' already exists - reusing it")
        else:
            # Not fatal: the first upload autoprovisions the repository anyway.
            logger.exception("Explicit repository creation failed")
            say(f"  !! could not pre-create the repository: {exc}")
            say("     continuing - the first upload will autoprovision it")

    try:
        result = client.create_branch(cfg.run_branch, source="main")
        say(f"  created branch '{cfg.run_branch}' at {str(result.get('head'))[:12]}...")
    except Exception as exc:
        if _status_code(exc) == 409:
            state["branch_existed"] = True
            say(f"  branch '{cfg.run_branch}' already exists - reusing it")
        else:
            logger.exception("Explicit branch creation failed")
            say(f"  !! could not pre-create the branch: {exc}")
            say("     continuing - the first upload will open it")

    return state


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6 - Upload each artifact through the gateway
# ═════════════════════════════════════════════════════════════════════════════
#
# The gateway stores the bytes in MinIO under {branch}/raw/{dataset_id}/{name},
# registers a dcat:Dataset in Fuseki with dcat:title / dcat:description /
# dcterms:format, links it with prov:wasAttributedTo to an agent derived from
# `owner`, and stamps energy:version plus energy:inRepository.
#
# Each upload carries its own commit_message, so the LakeFS log reads as a
# narrative of the run rather than nine identical auto-generated subjects. The
# structured commit metadata (dataset_id, owner, format, ingested_at) is still
# attached underneath.
#
# TODO(cesdh-sdk): upload_model() takes a file path, not bytes. Artifacts are
# held in memory as required, so each one is written to a temporary file just
# long enough to hand it to the SDK.


@dataclass
class UploadOutcome:
    artifact: Artifact
    dataset_id: str | None = None
    commit_sha: str | None = None
    lakefs_uri: str | None = None
    status: str = "pending"
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "uploaded"


def upload_artifacts(
    client: CESDHClient,
    cfg: Config,
    artifacts: list[Artifact],
    run_id: str,
    scenario_metadata: dict[str, Any] | None = None,
) -> list[UploadOutcome]:
    """Upload every artifact, never letting one failure abort the run."""
    outcomes: list[UploadOutcome] = []

    for artifact in artifacts:
        outcome = UploadOutcome(artifact=artifact)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                staged = Path(tmp) / artifact.name
                staged.write_bytes(artifact.data)

                result = client.upload_model(
                    str(staged),
                    owner=cfg.owner,
                    branch=cfg.run_branch,
                    version=cfg.version,
                    description=artifact.description,
                    force=True,  # idempotent re-runs supersede, never collide
                    force_tier=artifact.force_tier,
                    run_id=run_id,
                    # Supplies the geography and target year the catalog turns
                    # into dcat:keyword tags - this is what makes the run
                    # findable by "Switzerland" in Section 8 [a].
                    scenario_metadata=scenario_metadata,
                    commit_message=(
                        f"{run_id}: add {artifact.name} - {artifact.description}"
                    ),
                )

            status = result.get("status", "success")
            outcome.dataset_id = result.get("dataset_id")
            outcome.commit_sha = result.get("lakefs_commit_id")
            outcome.lakefs_uri = result.get("lakefs_uri")

            if status == "validation_failed" or result.get("validation_hard_errors"):
                # A rejected upload is a normal, reportable outcome: the gateway
                # answers 201 with a failure body rather than raising.
                errs = result.get("validation_hard_errors") or []
                outcome.status = "rejected"
                outcome.detail = errs[0] if errs else "validation failed"
                say(f"  !! rejected {artifact.name}: {outcome.detail}")
            else:
                outcome.status = "uploaded"
                say(
                    f"  OK uploaded {artifact.name} ({human_bytes(artifact.size)}) "
                    f"-> {outcome.dataset_id}"
                )
        except Exception as exc:
            logger.exception("Upload failed for %s", artifact.name)
            outcome.status = "failed"
            outcome.detail = str(exc)
            say(f"  !! failed   {artifact.name}: {exc}")

        outcomes.append(outcome)

    return outcomes


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 7 - Commit and tag the run
# ═════════════════════════════════════════════════════════════════════════════


def report_commit_and_tag(
    client: CESDHClient, cfg: Config, outcomes: list[UploadOutcome], summary: str
) -> str | None:
    """Report the per-upload commits and tag the finished run.

    There is no separate "commit the run" call, and deliberately so: the gateway
    commits after every upload, so nothing is ever left staged and a standalone
    commit could only ever be empty. Each upload carried its own
    commit_message (Section 6); the run as a whole is marked by a tag.

    Returns the tag's commit id, or None when tagging did not happen.
    """
    shas = [o.commit_sha for o in outcomes if o.ok and o.commit_sha]
    unique = list(dict.fromkeys(shas))
    say(f"  LakeFS commits on this branch: {len(unique)}")
    for sha in unique[:3]:
        say(f"    - {sha[:16]}...")
    if len(unique) > 3:
        say(f"    ... and {len(unique) - 3} more")

    say("")
    say(f"  tagging the run as '{cfg.tag_name}'")
    say(f"    {summary}")

    # Tags are immutable: creating one that exists returns 409 and never
    # repoints it, so a reference cited in a paper keeps its meaning.
    try:
        result = client.create_tag(cfg.tag_name, ref=cfg.run_branch)
        commit_id = result.get("commit_id", "")
        say(f"    tag created -> {commit_id}")
        say(f"    cite this: lakefs://{cfg.repository}/{cfg.tag_name}")
        return commit_id
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"    tag '{cfg.tag_name}' already exists - left untouched (immutable)")
        else:
            logger.exception("Tag creation failed")
            say(f"    !! could not create the tag: {exc}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 8 - Verify the catalog and the data
# ═════════════════════════════════════════════════════════════════════════════


def verify_catalog(
    client: CESDHClient, cfg: Config, query: str
) -> list[dict[str, Any]]:
    """Natural-language search over Fuseki (LLM -> SPARQL, via the gateway).

    Non-deterministic by nature: a local 8B model does not always emit valid,
    repository-scopable SPARQL, and the gateway then answers 503. That is a
    property of the translation layer, so a failure here is reported and
    skipped rather than allowed to abort the run.
    """
    say(f"  [a] natural-language search for: {query!r}")
    hits: list[dict[str, Any]] = []
    try:
        response = client.search(query, limit=5, timeout=cfg.search_timeout)
        hits = response.get("results", []) or []
        say(f"      {len(hits)} DCAT record(s) returned:")
        for hit in hits:
            title = hit.get("title") or hit.get("dataset") or hit
            say(f"        - {title}")
        if not hits:
            say("      (no match: the catalogue indexes dataset titles and")
            say("       descriptions, and nothing in this run is literally named")
            say("       after a Swiss hydro reservoir. The hydro entities live")
            say("       inside the shredded model graph, not in the DCAT titles.)")
    except Exception as exc:
        logger.exception("Natural-language search failed")
        say(f"      !! search unavailable this run: {exc}")
        say("         (the local LLM does not always emit valid SPARQL; retry,")
        say("          or use the deterministic listing below)")

    # [b] Deterministic catalogue proof. The NL path above is experimental and
    # may legitimately return nothing, so this second check is what actually
    # demonstrates that the DCAT records for this run exist and are scoped to
    # this repository. list_datasets() is a plain catalogue query - no LLM.
    say("")
    say(f"  [b] deterministic catalogue listing for branch {cfg.run_branch!r}")
    try:
        records = client.list_datasets(branch=cfg.run_branch, limit=50)
        say(f"      {len(records)} dcat:Dataset record(s) registered:")
        for rec in records:
            title = rec.get("title", "?")
            fmt = rec.get("format", "?")
            owner = rec.get("owner", "?")
            say(f"        - {title:34s} format={fmt:6s} owner={owner}")
    except Exception:
        logger.exception("Catalogue listing failed")
        say("      !! catalogue listing failed - see log")

    return hits


def verify_round_trip(
    client: CESDHClient,
    outcomes: list[UploadOutcome],
    search_hits: list[dict[str, Any]],
) -> bool:
    """Download one artifact back and prove the bytes are unchanged.

    Prefers an artifact surfaced by the search; falls back to the first
    successful upload when the search returned nothing usable.
    """
    uploaded = [o for o in outcomes if o.ok and o.dataset_id]
    if not uploaded:
        say("  nothing was uploaded successfully - skipping round-trip")
        return False

    chosen: UploadOutcome | None = None
    hit_ids = {str(h.get("dataset", "")).rsplit("#", 1)[-1] for h in search_hits} | {
        str(h.get("dataset_id", "")) for h in search_hits
    }
    for outcome in uploaded:
        if outcome.dataset_id in hit_ids:
            chosen = outcome
            break
    if chosen is None:
        chosen = uploaded[0]
        say("  search returned nothing from this run - using the first upload instead")

    say(f"  downloading {chosen.artifact.name} ({chosen.dataset_id})")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / chosen.artifact.name
            client.download_to_file(chosen.dataset_id or "", str(destination))
            downloaded = destination.read_bytes()

        original_sha = chosen.artifact.sha256
        downloaded_sha = hashlib.sha256(downloaded).hexdigest()
        match = original_sha == downloaded_sha

        say(f"    uploaded   sha256 = {original_sha}")
        say(f"    downloaded sha256 = {downloaded_sha}")
        say(f"    bytes: {chosen.artifact.size} -> {len(downloaded)}")
        say("    round-trip is lossless" if match else "    !! CHECKSUM MISMATCH")
        return match
    except Exception as exc:
        logger.exception("Round-trip download failed")
        say(f"  !! round-trip failed: {exc}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 9 - Cleanup / next steps
# ═════════════════════════════════════════════════════════════════════════════


def print_summary(
    cfg: Config,
    outcomes: list[UploadOutcome],
    round_trip_ok: bool,
    summary: str,
    tag_commit: str | None = None,
) -> None:
    ok = [o for o in outcomes if o.ok]
    total_bytes = sum(o.artifact.size for o in ok)
    commits = list(dict.fromkeys(o.commit_sha for o in ok if o.commit_sha))
    head = f"{commits[0][:12]}..." if commits else "(none)"

    rows = [
        ("repository", cfg.repository),
        ("branch", cfg.run_branch),
        ("commit (head)", head),
        (
            "tag",
            f"{cfg.tag_name} -> {tag_commit[:12]}..."
            if tag_commit
            else f"{cfg.tag_name} (not created)",
        ),
        ("citable ref", f"lakefs://{cfg.repository}/{cfg.tag_name}"),
        ("model", summary),
        ("artifacts uploaded", f"{len(ok)} / {len(outcomes)}"),
        ("total bytes", f"{total_bytes:,} ({human_bytes(total_bytes)})"),
        ("search round-trip", "OK" if round_trip_ok else "not OK"),
    ]

    banner("SECTION 9 - Summary")
    if RICH_AVAILABLE and _CONSOLE is not None and _RichTable is not None:
        table = _RichTable(show_header=True, header_style="bold")
        table.add_column("Field")
        table.add_column("Value", overflow="fold")
        for key, value in rows:
            table.add_row(key, str(value))
        _CONSOLE.print(table)
    else:
        width = max(len(k) for k, _ in rows)
        say("")
        for key, value in rows:
            say(f"  {key.ljust(width)} : {value}")

    failed = [o for o in outcomes if not o.ok]
    if failed:
        say("")
        say(f"  {len(failed)} artifact(s) did not land:")
        for outcome in failed:
            say(
                f"    - {outcome.artifact.name} [{outcome.status}] {outcome.detail[:70]}"
            )

    say("")
    say("  What to try next")
    say(f"    1. Open the LakeFS UI at {cfg.lakefs_url} and diff branch")
    say(f"       '{cfg.run_branch}' against 'main' to see exactly what this run added.")
    say("    2. Query Fuseki directly with the SPARQL the gateway generated - e.g.")
    say(
        "       client.sparql('SELECT ?s WHERE { GRAPH <...catalog> { ?dataset ?p ?s } }')"
    )
    say("       Bind your subject to ?dataset: the gateway splices a repository")
    say("       filter onto that exact variable name.")
    say("    3. Promote this run to main once you are happy with it (merge")
    say(f"       '{cfg.run_branch}' in the LakeFS UI, or via lakectl merge).")
    say("    4. Change the fuel costs or CO2 price in Section 4 of the CESDM")
    say("       tutorial and re-run: CESDH gives you a fresh branch and tag")
    say("       automatically, so the two runs stay independently citable.")
    say("")


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("CESDH_LOG_LEVEL", "WARNING"),
        format="%(levelname)s %(name)s: %(message)s",
    )
    cfg = Config()

    banner("SECTION 2 - What is CESDH, and why are we using it here?")
    say(WHAT_IS_CESDH)

    banner("SECTION 3 - Setup and configuration")
    say(f"  gateway      {cfg.gateway_url}")
    say(f"  lakefs       {cfg.lakefs_url}")
    say(f"  minio        {cfg.minio_url}  (console {cfg.minio_console_url})")
    say(f"  fuseki       {cfg.fuseki_url}")
    say(f"  repository   {cfg.repository}")
    say(f"  branch       {cfg.run_branch}")
    say(f"  owner        {cfg.owner}")
    say(f"  rich output  {'yes' if RICH_AVAILABLE else 'no (plain text fallback)'}")

    client = CESDHClient(
        repository=cfg.repository,
        gateway_url=cfg.gateway_url,
        lakefs_url=cfg.lakefs_url,
    )

    banner("SECTION 4 - Run the CESDM tutorial")
    try:
        model, artifacts = build_model_and_collect_artifacts(cfg)
    except Exception:
        logger.exception("The CESDM tutorial could not be executed")
        say("  !! Could not run the CESDM tutorial - aborting.")
        say("     Check that cesdm/sweet-cosi-cesdm/ is present and importable.")
        return 1

    summary = model_summary(model)
    say("")
    say(f"  model summary: {summary}")
    say(f"  artifacts collected: {len(artifacts)}")

    if not artifacts:
        say("  !! No artifacts were produced - nothing to upload.")
        return 1

    banner("SECTION 5 - Create a CESDH repository and a per-run branch")
    prepare_repository(client, cfg)

    banner("SECTION 6 - Upload each artifact through the gateway")
    run_id = f"ch_neighbours_{cfg.stamp}"  # no spaces: used to build a PROV IRI
    scen_meta = scenario_metadata_from_model(model, cfg)
    say(f"  tagging every artifact with: {scen_meta}")
    outcomes = upload_artifacts(client, cfg, artifacts, run_id, scen_meta)

    banner("SECTION 7 - Commit and tag the run")
    tag_commit = report_commit_and_tag(client, cfg, outcomes, summary)

    banner("SECTION 8 - Verify the catalog and the data")
    hits = verify_catalog(client, cfg, "Switzerland hydro reservoir")
    say("")
    round_trip_ok = verify_round_trip(client, outcomes, hits)

    print_summary(cfg, outcomes, round_trip_ok, summary, tag_commit)

    return 0 if any(o.ok for o in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
