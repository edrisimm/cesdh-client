"""Clone every active dataset on a branch into a local working tree.

Demonstrates the Git-like Lakehouse behaviour: the local copy is a
snapshot at the branch HEAD; subsequent commits don't affect the local files.

Run from any workstation that can reach the FEN gateway (via VPN or local).

Usage:
    python examples/03_clone_branch.py
"""
from __future__ import annotations

import pathlib

import cesdh

# 1. Identify yourself (matches the dashboard's user-creation gate).
cesdh.set_user("alice", "Alice from FEN-team")

# 2. Clone a branch.
REPO = "energy-repository"
BRANCH = "main"

local = pathlib.Path("./energy-mirror")
local.mkdir(parents=True, exist_ok=True)

paths = cesdh.clone_branch(
    branch=BRANCH,
    destination=str(local),
    repository=REPO,
)
print(f"Cloned {len(paths)} files into {local}")
for p in paths[:10]:
    print(f"  - {p}")
if len(paths) > 10:
    print(f"  ... and {len(paths) - 10} more")

# 3. Clone only the raw tier (optional).
raw_only = pathlib.Path("./energy-mirror-raw")
raw_only.mkdir(parents=True, exist_ok=True)

raw_paths = cesdh.clone_branch(
    branch=BRANCH,
    destination=str(raw_only),
    repository=REPO,
    tier="raw",
)
print(f"\nCloned {len(raw_paths)} raw-tier files into {raw_only}")
