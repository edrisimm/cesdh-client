#!/usr/bin/env python3
"""
groupe_e_planning_process_provisioning_part2.py — modify & audit
======================================================================

Part 2 of the Groupe-E planning process tutorial.

**Prerequisite:** Part 1 must have been run with ``KEEP=1`` so that the
repository ``groupe-e-planning-process`` still exists on ``main`` with
its full PROV-O lineage intact.

This script demonstrates the *modify → audit* half of the data
lifecycle:

    Phase 7  – verify Part 1 state, configure a second identity, and
               create a ``sensitivity-2025`` branch
    Phase 8  – download the costs dataset, apply a sensitivity
               adjustment, and upload the revision with lineage
    Phase 9  – audit: compare baseline and revision across branches

After the script finishes, the evaluator can see:

- the published baseline on ``main`` is untouched,
- the revision on ``sensitivity-2025`` is traceable back to the
  original raw inputs via PROV-O,
- the two branches diverge by exactly one dataset, with clear
  attribution (who changed what, when, on which branch).

Run::

    # After Part 1 was run with KEEP=1
    python examples_cesdh/groupe_e_planning_process_provisioning_part2.py

    # Override the sensitivity factor
    PART2_SCALE_FACTOR=1.25 python examples_cesdh/groupe_e_planning_process_provisioning_part2.py

    # Point at a different dataset to revise
    PART2_TARGET_QUERY="expansion_results" python examples_cesdh/groupe_e_planning_process_provisioning_part2.py
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    import cesdh
except ImportError as exc:
    raise SystemExit(
        "cesdh SDK not found. Install it:\n"
        "    pip install -e src/sdk\n"
        f"(original error: {exc})"
    )

#####################
# ── Configuration ────────────────────────────────────────────────────────────
#####################

ENV = os.environ

REPO = ENV.get("REPO", "groupe-e-planning-process")
PART2_BRANCH = ENV.get("PART2_BRANCH", "sensitivity-2025")
PART2_OWNER = ENV.get("PART2_OWNER", "marie")
PART2_DISPLAY = ENV.get("PART2_DISPLAY", "Marie Dupont")
PART2_RUN_ID = ENV.get("PART2_RUN_ID", "sensitivity_rerun_2025")
PART2_WORK_DIR = ENV.get("PART2_WORK_DIR", os.path.join(".", "part2_workdir"))
PART2_SCALE_FACTOR = float(ENV.get("PART2_SCALE_FACTOR", "1.15"))
PART2_TARGET_QUERY = ENV.get("PART2_TARGET_QUERY", "expansion_costs")

# Storage subpath for the revision (matches Part 1's grid-planning subpath)
SP_GP = "grid_planning_results"

#####################
# ── Console helpers (reused from Part 1) ─────────────────────────────────────
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


#####################
# ── Pipeline phases ──────────────────────────────────────────────────────────
#####################


def phase_7_verify_and_branch() -> str:
    """Verify Part 1 state, set Part 2 identity, create branch.

    Returns the dataset_id of the costs dataset on main (the revision target).
    """
    banner("Phase 7 – Verify Part 1 state and create the sensitivity branch")

    # ── Identity ──────────────────────────────────────────────────────────
    cesdh.set_user(PART2_OWNER, PART2_DISPLAY)
    say(
        f"  gateway     {ENV.get('CESDH_DATA_HUB_ENDPOINT', 'http://localhost:8080')}"
    )
    say(f"  user        {PART2_OWNER} <{PART2_DISPLAY}>")
    say(f"  repository  {REPO}")
    say(f"  branch      {PART2_BRANCH}")
    say(f"  scale       ×{PART2_SCALE_FACTOR}")

    # ── Verify the Part 1 repository exists ───────────────────────────────
    say("\n  Checking Part 1 state …")
    try:
        branches = cesdh.list_branches(repository=REPO)
    except Exception as exc:
        if _status_code(exc) == 404:
            raise SystemExit(
                f"\n  Repository '{REPO}' not found.\n"
                "  Run Part 1 with KEEP=1 first:\n"
                "    KEEP=1 python examples_cesdh/"
                "groupe_e_planning_process_provisioning.py\n"
            )
        raise

    branch_names = [b["branch"] for b in branches] if branches else []
    if "main" not in branch_names:
        raise SystemExit(
            f"\n  Repository '{REPO}' exists but has no 'main' branch.\n"
            "  Re-run Part 1 with KEEP=1 to provision it.\n"
        )
    say(f"  ✓ repository exists with {len(branch_names)} branch(es): {branch_names}")

    # ── Discover the target dataset via search ────────────────────────────
    say(f"\n  Searching for '{PART2_TARGET_QUERY}' on main …")
    hits = cesdh.search(query=PART2_TARGET_QUERY, repository=REPO, limit=5)
    results = hits.get("results", [])
    if not results:
        raise SystemExit(
            f"\n  No datasets matched '{PART2_TARGET_QUERY}' in '{REPO}'.\n"
            "  Check that Part 1 completed successfully.\n"
        )

    target = results[0]
    target_id = target["dataset_id"]
    target_title = target.get("title", target_id)
    say(f"  ✓ found target: {target_title}  ({target_id})")

    # ── Create the sensitivity branch ─────────────────────────────────────
    say(f"\n  Creating branch '{PART2_BRANCH}' from main …")
    try:
        cesdh.create_branch(branch=PART2_BRANCH, repository=REPO, source="main")
        say(f"  ✓ branch '{PART2_BRANCH}' created")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  branch '{PART2_BRANCH}' already exists – reusing")
        else:
            raise

    return target_id


def phase_8_revise(original_dataset_id: str) -> str:
    """Download the target dataset, apply sensitivity adjustment, re-upload.

    Returns the revised dataset_id.
    """
    banner("Phase 8 – Download, revise, and upload with lineage")

    # ── Prepare work directory ────────────────────────────────────────────
    work_dir = Path(PART2_WORK_DIR)
    work_dir.mkdir(parents=True, exist_ok=True)

    # ── Download ──────────────────────────────────────────────────────────
    say(f"  Downloading dataset {original_dataset_id} …")
    local_path = cesdh.download_to_file(
        dataset_id=original_dataset_id,
        destination=str(work_dir),
        repository=REPO,
    )
    say(f"  ✓ saved to {local_path}")

    # ── Modify ────────────────────────────────────────────────────────────
    say(f"\n  Applying ×{PART2_SCALE_FACTOR} sensitivity adjustment …")
    revised_path = _apply_sensitivity(local_path, PART2_SCALE_FACTOR)
    say(f"  ✓ revised file: {revised_path}")

    # ── Upload to the sensitivity branch with lineage ─────────────────────
    say(f"\n  Uploading revision to branch '{PART2_BRANCH}' …")
    original_name = Path(local_path).stem
    res = cesdh.upload(
        file_path=revised_path,
        owner=PART2_OWNER,
        repository=REPO,
        branch=PART2_BRANCH,
        subpath=SP_GP,
        description=(
            f"Sensitivity revision of {original_name} "
            f"(×{PART2_SCALE_FACTOR} cost adjustment)"
        ),
        derived_from=[original_dataset_id],
        run_id=PART2_RUN_ID,
        force_tier="generic",
        commit_message=(
            f"sensitivity: revise {original_name} ×{PART2_SCALE_FACTOR}"
        ),
        dcat_metadata={
            "theme": "grid-expansion",
            "keywords": [
                "sensitivity",
                "costs",
                "revision",
                SP_GP,
                REPO,
            ],
        },
    )
    revised_id = res["dataset_id"]
    say(f"  ✓ revised dataset  {revised_id}")
    say(f"    derived_from     {original_dataset_id}")
    say(f"    run_id           {PART2_RUN_ID}")
    say(f"    branch           {PART2_BRANCH}")
    return revised_id


def _apply_sensitivity(local_path: str, scale: float) -> str:
    """Apply a multiplicative sensitivity factor to numeric columns.

    Supports .xlsx, .xls, .csv, and .json.  Falls back to a byte-level
    copy (no modification) for unsupported formats so the tutorial still
    runs end-to-end.

    Returns the path to the revised file.
    """
    import shutil

    ext = Path(local_path).suffix.lower()
    revised_name = f"{Path(local_path).stem}_sensitivity_{scale}{ext}"
    revised_path = str(Path(local_path).parent / revised_name)

    try:
        import pandas as pd
    except ImportError:
        say("    (pandas not available – copying file unchanged)")
        shutil.copy2(local_path, revised_path)
        return revised_path

    try:
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(local_path)
        elif ext == ".csv":
            df = pd.read_csv(local_path)
        elif ext == ".json":
            df = pd.read_json(local_path)
        else:
            say(f"    (unsupported format '{ext}' – copying file unchanged)")
            shutil.copy2(local_path, revised_path)
            return revised_path
    except Exception as exc:
        say(f"    (could not parse file: {exc} – copying file unchanged)")
        shutil.copy2(local_path, revised_path)
        return revised_path

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        say("    (no numeric columns found – copying file unchanged)")
        shutil.copy2(local_path, revised_path)
        return revised_path

    say(f"    scaling {len(numeric_cols)} numeric column(s) by ×{scale}")
    df[numeric_cols] = df[numeric_cols] * scale

    if ext in (".xlsx", ".xls"):
        df.to_excel(revised_path, index=False)
    elif ext == ".csv":
        df.to_csv(revised_path, index=False)
    elif ext == ".json":
        df.to_json(revised_path)

    return revised_path


def phase_9_audit(original_dataset_id: str, revised_dataset_id: str) -> None:
    """Audit: compare the baseline and the revision across branches."""
    banner("Phase 9 – Audit: compare baseline and revision")

    # ── 9a. What changed on the sensitivity branch? ───────────────────────
    say("\n  9a. What changed on the sensitivity branch?")
    say(f"      cesdh.get_branch_changes('{PART2_BRANCH}', '{REPO}')")
    try:
        changes = cesdh.get_branch_changes(branch=PART2_BRANCH, repository=REPO)
        if hasattr(changes, "empty") and changes.empty:
            say("      (no changes detected)")
        else:
            say(f"      {len(changes)} dataset(s) diverge from main:")
            for _, row in changes.head(10).iterrows():
                title = row.get("title") or row.get("path", "<unknown>")
                say(f"        • {title}")
    except Exception as exc:
        say(f"      (branch changes unavailable: {exc})")

    # ── 9b. Where did the revision come from? ─────────────────────────────
    say(f"\n  9b. Where did the revision come from?")
    say(f"      cesdh.get_lineage('{revised_dataset_id}', direction='ancestors')")
    try:
        ancestors = cesdh.get_lineage(
            revised_dataset_id, repository=REPO, direction="ancestors"
        )
        if hasattr(ancestors, "empty") and ancestors.empty:
            say("      (no ancestors – verify derived_from was passed)")
        else:
            say(f"      {len(ancestors)} ancestor(s) in the chain:")
            for _, row in ancestors.iterrows():
                sp = row.get("subpath") or "<no subpath>"
                say(f"        [{sp}]  {row.get('title', row.get('dataset_id'))}")
    except Exception as exc:
        say(f"      (lineage unavailable: {exc})")

    # ── 9c. Who touched this file, on which branch? ──────────────────────
    say(f"\n  9c. Who touched this file, on which branch?")
    say(
        f"      cesdh.get_file_branches_history('<file_path>', '{REPO}')"
    )
    try:
        # Resolve the file path from the original dataset metadata
        meta = cesdh.search(
            query=PART2_TARGET_QUERY, repository=REPO, limit=1
        )
        results = meta.get("results", [])
        file_path = results[0].get("storage_path", "") if results else ""
        if file_path:
            history = cesdh.get_file_branches_history(
                file_path=file_path, repository=REPO
            )
            if hasattr(history, "empty") and history.empty:
                say("      (no branch history found)")
            else:
                say(f"      {len(history)} version(s) across branches:")
                for _, row in history.iterrows():
                    branch = row.get("branch", "?")
                    owner = row.get("owner", "?")
                    checksum = str(row.get("checksum", ""))[:12]
                    say(f"        branch={branch}  owner={owner}  hash={checksum}…")
        else:
            say("      (could not resolve file path – skipping)")
    except Exception as exc:
        say(f"      (file history unavailable: {exc})")

    # ── 9d. How do the two pipelines compare? ─────────────────────────────
    say(f"\n  9d. How do the two pipelines compare?")
    main_count = 0
    branch_count = 0
    try:
        say(f"      cesdh.get_pipeline('{REPO}', branch='main')")
        main_edges = cesdh.get_pipeline(repository=REPO, branch="main")
        main_count = len(main_edges)
        say(f"      main: {main_count} derivation edge(s)")
    except Exception as exc:
        say(f"      (main pipeline unavailable: {exc})")

    try:
        say(f"      cesdh.get_pipeline('{REPO}', branch='{PART2_BRANCH}')")
        branch_edges = cesdh.get_pipeline(repository=REPO, branch=PART2_BRANCH)
        branch_count = len(branch_edges)
        say(f"      {PART2_BRANCH}: {branch_count} derivation edge(s)")
    except Exception as exc:
        say(f"      ({PART2_BRANCH} pipeline unavailable: {exc})")

    if main_count and branch_count:
        delta = branch_count - main_count
        if delta > 0:
            say(
                f"\n      The sensitivity branch has {delta} additional "
                f"derivation edge(s) – the lineage extended, not broke."
            )
        elif delta == 0:
            say(
                "\n      Both branches have the same number of edges. "
                "The revision replaced an existing node rather than "
                "extending the chain."
            )


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    banner(
        "Part 2: Modify & Audit — what happens when an assumption changes?"
    )
    say(f"  repository     {REPO}")
    say(f"  branch         {PART2_BRANCH}")
    say(f"  owner          {PART2_OWNER} <{PART2_DISPLAY}>")
    say(f"  scale factor   ×{PART2_SCALE_FACTOR}")
    say(f"  target query   '{PART2_TARGET_QUERY}'")

    original_id = phase_7_verify_and_branch()
    revised_id = phase_8_revise(original_id)
    phase_9_audit(original_id, revised_id)

    say("")
    say("=" * 72)
    say("  Summary")
    say("=" * 72)
    say(f"  branch 'main'             : baseline (untouched)")
    say(f"  branch '{PART2_BRANCH}'  : +1 revised dataset")
    say(f"  original dataset          : {original_id}")
    say(f"  revised dataset           : {revised_id}")
    say(f"  derived_from              : original → revision (PROV-O)")
    say(f"  owner                     : {PART2_OWNER}")
    say("")
    say("  The published baseline on main is untouched.")
    say("  The revision on the sensitivity branch is fully traceable")
    say("  back to the original raw inputs via PROV-O lineage.")
    say("")
    say("  To remove the sensitivity branch:")
    say(f"    cesdh.delete_branch('{PART2_BRANCH}', repository='{REPO}')")
    say("")
    say("  To remove the entire repository (Part 1 + Part 2):")
    say(f"    cesdh.delete_repository('{REPO}', confirm=True)")
    say("")
    say("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
