"""Demonstrate the identity primitive.

Every SDK call carries your user_id and display_name as X-ESDH-User-Id
and X-ESDH-User-Name headers. The gateway stamps these into PROV-O
attribution triples, LakeFS commit metadata, and the audit log.

Usage:
    python examples/05_user_identity.py
"""
from __future__ import annotations

import cesdh

# Set once at the top of every notebook or script.
cesdh.set_user("alice", "Alice from FEN-team")

# Read back at any time.
uid, name = cesdh.get_user()
print(f"Acting as: {name} ({uid})")

# Every subsequent call carries the identity headers automatically.
REPO = "energy-repository"
datasets = cesdh.list_datasets(repository=REPO, limit=5)
print(f"Listed {len(datasets)} dataset(s) as {uid}")
for ds in datasets:
    print(f"  - {ds.get('dataset_id', '?')}  {ds.get('title', '?')}")
