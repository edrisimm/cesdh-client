"""Stage a new file version and commit it (Git-like stage + commit flow).

Replaces the "update a dataset" mental model with the same semantics as
git: stage your changes, inspect the diff, then commit atomically.

Usage:
    python examples/04_commit_update.py
"""
from __future__ import annotations

import cesdh

# 1. Identify yourself.
cesdh.set_user("alice", "Alice from FEN-team")

REPO = "energy-repository"
BRANCH = "experiment-2030"

# Replace with a real dataset_id from a previous upload.
DATASET_ID = "dataset_abc123"
LOCAL_FILE = "./q4-demand.csv"

# 2. Stage: upload new bytes without committing.
print(f"Staging {LOCAL_FILE} for {DATASET_ID} on {BRANCH}...")
stage_result = cesdh.stage_dataset_update(
    dataset_id=DATASET_ID,
    file_path=LOCAL_FILE,
    repository=REPO,
    branch=BRANCH,
)
parent_sha = stage_result.get("parent_commit_sha", "")
print(f"  Staged. Parent commit: {parent_sha[:12]}")

# 3. Inspect: see what changed before committing.
diff = cesdh.get_staged_changes(
    dataset_id=DATASET_ID,
    repository=REPO,
    branch=BRANCH,
)
print(f"  Diff: {diff}")

# 4. Commit: atomically apply as a new commit on the branch.
result = cesdh.commit_dataset_update(
    dataset_id=DATASET_ID,
    repository=REPO,
    commit_message="Update demand profile for Q4 2025",
    branch=BRANCH,
    parent_commit_sha=parent_sha,
)
print(f"  Committed: {result.get('commit_id', result)}")

# To discard instead of committing:
# cesdh.discard_staged_changes(DATASET_ID, repository=REPO, branch=BRANCH)
