#!/usr/bin/env python3
"""
groupe_e_planning_process_provisioning.py - Groupe-E planning pipeline, end to end
====================================================================================

Provisions a full Groupe-E grid-planning pipeline as a single repository
in the Energy Systems Data Hub, with complete PROV-O lineage from raw
inputs to publication figures.

The pipeline follows ``group_e_flowchart.mmd``::

    raw_input_data  ─►  input_clean_data  ─►  regionalization/all_pckls
            ─►  grid_planning_results  ─►  grid_planning_publication/figures

Every upload captures ``prov:wasDerivedFrom`` via the SDK's
``derived_from=`` and ``run_id=`` arguments. After the script finishes,
any user can:

  1. search for a figure by natural language,
  2. call ``cesdh.get_lineage(figure_id, direction="ancestors")`` to walk
     the derivation chain back to the raw BFS / SOLKAT inputs,
  3. call ``cesdh.get_pipeline(repository, branch)`` to see the full
     derivation graph for the branch.

This example is the canonical "production-grade lineage" demo for the
Energy Systems Data Hub.

Prerequisites
-------------
1. Install the CESDH SDK in editable mode (one-time, per machine):

       pip install -e src/sdk

   This makes ``import cesdh`` resolve to the source tree in
   ``src/sdk/``. The package name is ``cesdh``; the import name is also
   ``cesdh``. The package version (currently 0.1.0) and dependency
   list are in ``src/sdk/pyproject.toml``.

   Full documentation is found at Getting started section at
   https://docs.fen-esdh.ch/trial/onboarding/

2. Read the SDK documentation:

       https://docs.fen-esdh.ch/reference/basic_functions/

3. Have the gateway URL for the deployment you'll target. The
   supported deployments are:

   - Local stack (default):   http://localhost:8080
   - Trial VPN stack:         https://fen-esdh.ch

   The URL is read from the environment variable
   ``CESDH_DATA_HUB_ENDPOINT``. Set it before running the example if
   you're not on localhost.

4. (Optional, for SKIP_FILE_CHECKS=0) Have the local source files
   for the pipeline on your laptop. The default paths point to a
   Yaman-specific layout under ``~/Desktop/Yaman/...``. If you have
   the pipeline data in a different location, override the path
   environment variables (see Configuration below). If you don't have
   the files yet, use ``SKIP_FILE_CHECKS=1`` to run the example as a
   smoke test against a pre-populated repository.

Configuration
-------------
All input paths are read from environment variables so the script is
portable across machines. The defaults match the original Groupe-E
pipeline layout used by Edrisi (Yaman's shared files).

Path environment variables::

    RAW_BFS        - raw BFS EGID list (xlsx)
    RAW_SOLK       - raw SOLKAT solar potential (xlsx)
    CLEAN_BFS      - cleaned BFS EGID list after HAK filter (csv)
    CLEAN_PV       - cleaned PV input per EGID (csv)
    CLEAN_SCN      - scenario assumptions (csv)
    PCKL_DIR       - directory of regionalized pickle files
    OGP1           - OGP1 grid-expansion results (xlsx)
    OGP2           - OGP2 grid-expansion costs (xlsx)
    FIG_DIR        - directory of publication figures (png)

Behaviour flags::

    KEEP=1               mark the run as "keep the repository" (default: 0)
                         Note: this script does not auto-teardown; KEEP
                         is read and echoed in the summary so the
                         end-of-run hints stay accurate. The actual
                         teardown command is shown in the summary
                         (``cesdh.delete_repository(...)``).
    SKIP_FILE_CHECKS=1  bypass the local-file existence checks (default: 0)
                         Use this when running against a pre-populated
                         repository whose data you do not have locally,
                         or in CI.

Run
---
Run from the repository root::

    # Trial VPN stack (production-style)
    CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch \\
        python examples_cesdh/groupe_e_planning_process_provisioning.py

    # Local stack (default endpoint, no env override needed)
    python examples_cesdh/groupe_e_planning_process_provisioning.py

    # Smoke test against a pre-populated repository (no local files needed)
    SKIP_FILE_CHECKS=1 CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch \\
        python examples_cesdh/groupe_e_planning_process_provisioning.py

    # Run with custom local paths (Yaman's defaults, but redirected)
    CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch \\
    RAW_BFS=/data/yaman/BFS.xlsx \\
    CLEAN_PV=/data/yaman/PV.csv \\
        python examples_cesdh/groupe_e_planning_process_provisioning.py

Output
------
The script prints a banner per phase and a final summary table. The
last phase, "Task 2 - Search and traverse PROV-O lineage", runs four
checks against the populated repository:

    1. Natural-language search for one of the figures
    2. Ancestors of that figure (raw inputs)
    3. Full bidirectional lineage of that figure
    4. Full pipeline view of the branch (all derivation edges)

If any check fails (typically because the catalog is empty or
Fuseki's NL->SPARQL translator is unavailable), the script prints the
exception and continues. A successful run shows the figure's full
derivation chain ending in the raw BFS and SOLKAT inputs.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import glob
import os
from collections.abc import Iterable

try:
    import cesdh
except ImportError as exc:
    raise SystemExit(
        "cesdh SDK not found. Install it (one-time, per machine):\n"
        "    pip install -e src/sdk\n"
        "Docs: https://elisabet-cg.github.io/idm/sdk/\n"
        f"(original error: {exc})"
    ) from exc

#####################
# ── Configuration ────────────────────────────────────────────────────────────
#####################

REPO = "groupe-e-planning-process"
BR = "main"  # target branch
OWNER = "yaman"
USER_DISPLAY = "Yaman"

# Local source paths - override via env vars if your layout differs.
ENV = os.environ

RAW_BFS = ENV.get(
    "RAW_BFS", "/Users/edrisi/Desktop/Yaman/raw_downloaded/BFS_VS_VD_FR_GE.xlsx"
)
RAW_SOLK = ENV.get(
    "RAW_SOLK",
    "/Users/edrisi/Desktop/Yaman/raw_downloaded/SOLKAT_CH_DACH_Intersect_GroupE.xlsx",
)
CLEAN_BFS = ENV.get(
    "CLEAN_BFS", "/Users/edrisi/Desktop/Yaman/data_cleaned/input_BFS_HAK_EGID.csv"
)
CLEAN_PV = ENV.get(
    "CLEAN_PV", "/Users/edrisi/Desktop/Yaman/data_cleaned/input_PV_HAK_EGID.csv"
)
CLEAN_SCN = ENV.get(
    "CLEAN_SCN", "/Users/edrisi/Desktop/Yaman/data_cleaned/input_scenarios.csv"
)
PCKL_DIR = ENV.get("PCKL_DIR", "/Users/edrisi/Desktop/Yaman/all_pckls")
OGP1 = ENV.get(
    "OGP1",
    "/Users/edrisi/Desktop/Yaman/grid_planning/2024-04-30_expansion_results.xlsx",
)
OGP2 = ENV.get(
    "OGP2", "/Users/edrisi/Desktop/Yaman/grid_planning/2024-04-30_expansion_costs.xlsx"
)
FIG_DIR = ENV.get("FIG_DIR", "/Users/edrisi/Desktop/Yaman/paper/figures")

# PROV-O run identifiers (one Activity node per pipeline stage)
RUN_CLEAN = "clean_2024-04-30"
RUN_REGIONALIZE = "regionalize_2024-04-30"
RUN_GP = "grid_planning_2024-04-30"
RUN_PUB = "publication_2024-04-30"

KEEP = ENV.get("KEEP", "0") == "1"
SKIP_FILE_CHECKS = ENV.get("SKIP_FILE_CHECKS", "0") == "1"

# Resolve the gateway endpoint once at module load so the banner at
# the top of every run can echo the value the script will actually use.
GATEWAY = ENV.get("CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080")

# Storage subpaths (must match the SDK subpath grammar:
# segments [A-Za-z0-9_\-\.]+, no leading/trailing /, no '..')
SP_RAW = "raw_input_data"
SP_CLEAN = "input_clean_data"
SP_PCKL = "regionalization_and_time_series/all_pckls"
SP_GP = "grid_planning_results"
SP_FIG = "grid_planning_publication/figures"

FIGURES = [
    "fig_PV_curt_box_default_en.png",
    "fig_PV2Dem_Cost_default_en.png",
    "fig_PV2NewDem_Cost_region_default_en.png",
    "fig_PV2NewDem_Cost_year_default_en.png",
    "fig_QU_box_default_en.png",
    "fig_RONT_box_default_en.png",
    "fig_Trafo_vs_Akm_default_en.png",
]

#####################
# ── Console helpers ──────────────────────────────────────────────────────────
#####################


def say(msg: str = "") -> None:
    print(msg, flush=True)


def banner(title: str) -> None:
    say("")
    say("=" * 72)
    say(f"  {title}")
    say("=" * 72)


def _status_code(exc: Exception) -> int | None:
    return getattr(getattr(exc, "response", None), "status_code", None)


def require_file(path: str) -> None:
    if SKIP_FILE_CHECKS:
        return
    if not Path(path).is_file():
        raise SystemExit(
            f"Missing local file: {path}\n"
            f"Set the appropriate env var (RAW_BFS, RAW_SOLK, CLEAN_BFS, "
            f"CLEAN_PV, CLEAN_SCN, OGP1, OGP2, or a parent FIG_DIR/PCKL_DIR) "
            f"to point at the file on this machine, or run with "
            f"SKIP_FILE_CHECKS=1 to bypass."
        )


def require_dir(path: str) -> None:
    if SKIP_FILE_CHECKS:
        return
    if not Path(path).is_dir():
        raise SystemExit(
            f"Missing local directory: {path}\n"
            f"Set the appropriate env var (PCKL_DIR or FIG_DIR) to point at "
            f"the directory on this machine, or run with SKIP_FILE_CHECKS=1 "
            f"to bypass."
        )


def ingest(
    file_path: str,
    *,
    subpath: str,
    description: str,
    derived_from: list[str] | None = None,
    run_id: str | None = None,
    keywords: Iterable[str] = (),
    theme: str = "",
    commit_message: str | None = None,
) -> dict:
    """Single canonical call to cesdh.upload with PROV-O wired in."""
    kwargs = dict(
        file_path=file_path,
        owner=OWNER,
        repository=REPO,
        branch=BR,
        subpath=subpath,
        description=description,
        force_tier="generic",  # binary assets - no CESDM schema
        commit_message=commit_message or f"ingest: {Path(file_path).name}",
        dcat_metadata={
            "theme": theme or subpath.split("/")[0],
            "keywords": list(keywords) + [subpath.split("/")[0], REPO],
        },
    )
    if derived_from is not None:
        kwargs["derived_from"] = derived_from
    if run_id is not None:
        kwargs["run_id"] = run_id
    return cesdh.upload(**kwargs)


#####################
# ── Pipeline phases ──────────────────────────────────────────────────────────
#####################


def phase_0_setup() -> None:
    banner("Phase 0 - Configure identity and create the repository")
    cesdh.set_user(OWNER, USER_DISPLAY)
    say(f"  gateway        {GATEWAY}")
    say(f"  user           {OWNER} <{USER_DISPLAY}>")
    say(f"  repository     {REPO}")
    say(f"  branch         {BR}")
    say(f"  KEEP           {KEEP}")
    say(f"  SKIP_FILE_CHECKS  {SKIP_FILE_CHECKS}")

    try:
        cesdh.create_repository(repository=REPO)
        say(f"  created repository '{REPO}'")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  repository '{REPO}' already exists - reusing")
        else:
            raise


def phase_1_raw() -> tuple[str, str]:
    banner("Phase 1a - raw_input_data/")
    require_file(RAW_BFS)
    require_file(RAW_SOLK)

    bfs = ingest(
        RAW_BFS,
        subpath=SP_RAW,
        description="Raw BFS EGID list (building register) for Group E planning process",
        keywords=["EGID", "BFS", "raw"],
        theme="building-register",
        commit_message="seed: raw BFS EGID register",
    )
    solk = ingest(
        RAW_SOLK,
        subpath=SP_RAW,
        description="Raw SOLKAT solar potential per EGID (DACH intersect)",
        keywords=["SOLKAT", "PV", "DACH", "raw"],
        theme="solar-potential",
        commit_message="seed: raw SOLKAT solar potential",
    )
    say(f"  bfs_raw   {bfs['dataset_id']}")
    say(f"  solk_raw  {solk['dataset_id']}")
    return bfs["dataset_id"], solk["dataset_id"]


def phase_2_clean(raw_bfs_id: str, raw_solk_id: str) -> tuple[str, str, str]:
    banner("Phase 1b - input_clean_data/  (PROV-O: derived_from raw)")
    require_file(CLEAN_BFS)
    require_file(CLEAN_PV)
    require_file(CLEAN_SCN)

    bfs = ingest(
        CLEAN_BFS,
        subpath=SP_CLEAN,
        description="Cleaned BFS EGID list (HAK filter applied) - DataCleaning output",
        derived_from=[raw_bfs_id],
        run_id=RUN_CLEAN,
        keywords=["EGID", "BFS", "cleaned"],
        theme="building-register",
    )
    pv = ingest(
        CLEAN_PV,
        subpath=SP_CLEAN,
        description="Cleaned PV input per EGID - DataCleaning output",
        derived_from=[raw_solk_id],
        run_id=RUN_CLEAN,
        keywords=["SOLKAT", "PV", "cleaned"],
        theme="solar-potential",
    )
    scn = ingest(
        CLEAN_SCN,
        subpath=SP_CLEAN,
        description="Scenario assumptions (load growth, PV penetration, etc.)",
        # scenarios is hand-curated - no derived_from
        keywords=["scenarios"],
        theme="scenario-assumptions",
    )
    say(f"  clean_bfs  {bfs['dataset_id']}  derived_from={raw_bfs_id}")
    say(f"  clean_pv   {pv['dataset_id']}   derived_from={raw_solk_id}")
    say(f"  clean_scn  {scn['dataset_id']}  (no derived_from)")
    return bfs["dataset_id"], pv["dataset_id"], scn["dataset_id"]


def phase_3_pckl(clean_ids: list[str]) -> list[str]:
    banner(
        "Phase 1c - regionalization_and_time_series/all_pckls/  (PROV-O: 3 inputs -> 1 run)"
    )
    require_dir(PCKL_DIR)
    files = sorted(glob.glob(os.path.join(PCKL_DIR, "*.pckl")))
    if not files:
        raise SystemExit(f"No .pckl files found in {PCKL_DIR}")

    say(f"  found {len(files)} pckl files")
    pckl_ids: list[str] = []
    for i, path in enumerate(files, start=1):
        res = ingest(
            path,
            subpath=SP_PCKL,
            description=f"Regionalized time series: {Path(path).name}",
            derived_from=clean_ids,
            run_id=RUN_REGIONALIZE,
            keywords=["pckl", "regionalization", "time-series"],
            theme="time-series",
        )
        pckl_ids.append(res["dataset_id"])
        if i % 10 == 0 or i == len(files):
            say(f"  uploaded {i}/{len(files)} pckl files")
    return pckl_ids


def phase_4_grid(pckl_ids: list[str]) -> tuple[str, str]:
    banner("Phase 1d - grid_planning_results/  (PROV-O: 45 inputs -> 1 run)")
    require_file(OGP1)
    require_file(OGP2)

    results = ingest(
        OGP1,
        subpath=SP_GP,
        description="OGP1 grid-expansion results (2024-04-30 run)",
        derived_from=pckl_ids,
        run_id=RUN_GP,
        keywords=["OGP1", "expansion", "results"],
        theme="grid-expansion",
    )
    costs = ingest(
        OGP2,
        subpath=SP_GP,
        description="OGP2 grid-expansion costs (2024-04-30 run)",
        derived_from=pckl_ids,
        run_id=RUN_GP,
        keywords=["OGP2", "expansion", "costs"],
        theme="grid-expansion",
    )
    say(f"  gp_results  {results['dataset_id']}  derived_from={len(pckl_ids)} pckl")
    say(f"  gp_costs    {costs['dataset_id']}    derived_from={len(pckl_ids)} pckl")
    return results["dataset_id"], costs["dataset_id"]


def phase_5_figures(pub_inputs: list[str]) -> dict[str, str]:
    banner(
        "Phase 1e - grid_planning_publication/figures/  (PROV-O: results+costs -> fig)"
    )
    require_dir(FIG_DIR)

    figure_ids: dict[str, str] = {}
    for name in FIGURES:
        local = os.path.join(FIG_DIR, name)
        require_file(local)
        res = ingest(
            local,
            subpath=SP_FIG,
            description=f"Publication figure: {name}",
            derived_from=pub_inputs,
            run_id=RUN_PUB,
            keywords=["figure", "publication", "en"],
            theme="publication",
        )
        figure_ids[name] = res["dataset_id"]
    say(f"  registered {len(figure_ids)} figures")
    return figure_ids


def phase_6_audit(figure_ids: dict[str, str]) -> None:
    banner("Task 2 - Search and traverse PROV-O lineage")
    target = "fig_PV2Dem_Cost_default_en.png"
    if target not in figure_ids:
        target = next(iter(figure_ids))
    target_id = figure_ids[target]

    # 1. Natural-language search
    say(f"\n  cesdh.search('{target}')")
    try:
        hits = cesdh.search(query=target, repository=REPO, limit=5)
        for h in hits.get("results", [])[:5]:
            say(
                f"    {h.get('dataset_id')}  {h.get('title')}  branch={h.get('branch')}"
            )
    except Exception as exc:
        say(f"    (search unavailable: {exc})")

    # 2. Ancestors of a single figure - "where did this come from?"
    say(f"\n  cesdh.get_lineage('{target_id}', direction='ancestors')")
    try:
        lineage = cesdh.get_lineage(target_id, repository=REPO, direction="ancestors")
        if lineage.empty:
            say("    (no ancestors - verify derived_from was passed at upload)")
        else:
            for _, row in lineage.iterrows():
                sp = row.get("subpath") or "<no subpath>"
                say(f"    [{sp}]  {row['title']}")
    except Exception as exc:
        say(f"    (lineage unavailable: {exc})")

    # 3. Full lineage of the same figure - ancestors + self + descendants
    say(f"\n  cesdh.get_lineage('{target_id}')  - both directions")
    try:
        full = cesdh.get_lineage(target_id, repository=REPO)
        for rel in ("self", "ancestor", "descendant"):
            subset = full[full["relationship"] == rel]
            say(f"    {rel}: {len(subset)} dataset(s)")
    except Exception as exc:
        say(f"    (lineage unavailable: {exc})")

    # 4. Pipeline view - all derivation edges on the branch
    say(f"\n  cesdh.get_pipeline('{REPO}', branch='{BR}')")
    try:
        edges = cesdh.get_pipeline(repository=REPO, branch=BR)
        say(f"    {len(edges)} derivation edge(s) on branch '{BR}'")
        for _, e in edges.head(10).iterrows():
            say(f"    {e['source_title']}  ->  {e['target_title']}")
        if len(edges) > 10:
            say(f"    ... and {len(edges) - 10} more")
    except Exception as exc:
        say(f"    (pipeline unavailable: {exc})")


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    banner("Bootstrap groupe_e_planning_process with full PROV-O lineage")
    say(f"  repository     {REPO}")
    say(f"  branch         {BR}")
    say(f"  owner          {OWNER}")
    say(f"  gateway        {GATEWAY}")
    say(f"  keep artifacts {KEEP}")
    say(f"  skip fs check  {SKIP_FILE_CHECKS}")

    phase_0_setup()
    raw_bfs_id, raw_solk_id = phase_1_raw()
    clean_bfs_id, clean_pv_id, clean_scn_id = phase_2_clean(raw_bfs_id, raw_solk_id)
    pckl_ids = phase_3_pckl([clean_bfs_id, clean_pv_id, clean_scn_id])
    gp_results_id, gp_costs_id = phase_4_grid(pckl_ids)
    figure_ids = phase_5_figures([gp_results_id, gp_costs_id])
    phase_6_audit(figure_ids)

    say("")
    say("=" * 72)
    say("  Summary")
    say("=" * 72)
    say("  raw_input_data                : 2 datasets")
    say("  input_clean_data              : 3 datasets")
    say(f"  regionalization/all_pckls     : {len(pckl_ids)} datasets")
    say("  grid_planning_results         : 2 datasets")
    say(f"  grid_planning_publication/fig : {len(figure_ids)} datasets")
    say("")
    say("  To delete the repository and all its state at any time:")
    say(f"    cesdh.delete_repository('{REPO}', confirm=True)")
    if KEEP:
        say("")
        say("  KEEP=1: leaving the repository in place (no teardown performed).")
    say("")
    say("Done. The repository is fully provisioned and every dataset is")
    say("traceable to its raw input via PROV-O prov:wasDerivedFrom+.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
