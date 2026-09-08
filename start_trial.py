#!/usr/bin/env python3
"""
start_trial.py — Phase 2: per-user, per-trial branch creation
==============================================================

A trial user runs this once per experiment they start.  The script creates a
timestamped branch off ``main`` in their repository, plus an immutable tag
marking the empty starting point.

    python start_trial.py trial-alice "ch-res-expansion-2030"

    # Or with environment variables (preferred when using the cesdh-client archive):
    CESDH_REPOSITORY=trial-alice \\
    CESDH_TRIAL_NAME=ch-res-expansion-2030 \\
    python start_trial.py

Branch naming
-------------
The branch name is ``trial-<UTC timestamp>-<slug>``, e.g.
``trial-20260908T093000Z-ch-res-expansion-2030``.

Two reasons for the timestamp prefix:

  * **Replay safety** — if the script is run twice (network blip, user
    retried), the second run gets a *different* branch name rather than
    silently reusing the first one.  If the exact same branch already
    exists (same second), the 409 is caught and reported.

  * **Chronological sorting** — branches sort oldest-first in the LakeFS
    UI, which matches the order they were created.

Environment
-----------
    CESDH_DATA_HUB_ENDPOINT   gateway URL  (default: https://fen-esdh.ch)
    LAKEFS_ACCESS_KEY_ID      shared credentials
    LAKEFS_SECRET_ACCESS_KEY
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Auto-load .env from the repo root or cwd (same mechanism the SDK uses).
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=os.environ.get("CESDH_LOG_LEVEL", "WARNING"))
logger = logging.getLogger(__name__)

try:
    import cesdh
except ImportError:
    raise SystemExit(
        "CESDH SDK not installed.\n"
        "  From the full repo:    pip install -e ./src/sdk\n"
        "  From cesdh-client:     pip install ./sdk"
    )


def slugify(name: str) -> str:
    """Turn a free-text trial name into a LakeFS-safe branch slug.

    LakeFS branch names: letters, digits, underscores and dashes only.
    Cannot start with a dash.
    """
    name = name.strip().lower()
    out = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        elif ch == " ":
            out.append("-")
    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "trial"


def main() -> int:
    # ── Parse arguments (CLI or env vars) ────────────────────────────────
    if len(sys.argv) >= 3:
        repo = sys.argv[1]
        trial_name = sys.argv[2]
    else:
        repo = os.environ.get("CESDH_REPOSITORY", "")
        trial_name = os.environ.get("CESDH_TRIAL_NAME", "")
        if not repo or not trial_name:
            print(__doc__)
            return 1

    gateway = os.environ.get("CESDH_DATA_HUB_ENDPOINT", "https://fen-esdh.ch")

    # ── Build branch + tag names ─────────────────────────────────────────
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = slugify(trial_name)
    branch = f"trial-{timestamp}-{slug}"
    tag = f"{slug}-start-{timestamp}"

    print(f"gateway:    {gateway}")
    print(f"repository: {repo}")
    print(f"trial:      {trial_name}")
    print(f"branch:     {branch}")
    print(f"tag:        {tag}")
    print()

    # ── 1. Create the branch from main ───────────────────────────────────
    try:
        result = cesdh.create_branch(branch, repository=repo, source="main")
        head_id = result.get("head", result.get("commit_id", ""))
        print(f"OK  created branch '{branch}' at {str(head_id)[:16]}")
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 409:
            print(f"OK  branch '{branch}' already exists — reusing")
        else:
            logger.exception("Branch creation failed")
            print(f"!!  could not create branch: {exc}")
            return 1

    # ── 2. Tag the empty starting point ──────────────────────────────────
    try:
        cesdh.create_tag(tag, repository=repo, ref=branch)
        print(f"OK  tagged starting point as '{tag}'")
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 409:
            print(f"OK  tag '{tag}' already exists (immutable) — reusing")
        else:
            logger.exception("Tag creation failed")
            print(f"!!  could not create tag: {exc}")

    # ── Next steps ───────────────────────────────────────────────────────
    print()
    print("next steps:")
    print()
    print("  1. Upload data to this branch:")
    print(f'     cesdh.upload_raw("my_data.csv", owner="your-name",')
    print(f'         repository="{repo}", branch="{branch}", force_tier="generic")')
    print()
    print("  2. When done, tag the final state:")
    print(f'     cesdh.create_tag("{slug}-final-{timestamp}",')
    print(f'         repository="{repo}", ref="{branch}")')
    print()
    print("  3. Browse your data:")
    print("     Dashboard:  https://dash.fen-esdh.ch")
    print(f"     LakeFS UI:  https://lakefs.fen-esdh.ch/repositories/{repo}/objects?ref={branch}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
