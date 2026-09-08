#!/usr/bin/env python3
"""
quickstart.py - your first five minutes with the Energy Systems Data Hub.

Upload a file, find it, get it back, prove the bytes survived. That is the whole
platform in one screen. Everything else - scenarios, lineage, multi-researcher
handoffs - is elaboration on these four calls.

    docker compose up -d          # or: make up
    python examples_cesdh/quickstart.py

Next, in order:
    examples_cesdh/raw_data_hub_lifecycle.py        real open data + DCAT search
    examples_cesdh/multi_researcher_cesdm_workflow.py   A -> B -> C handoff
"""

import hashlib
import tempfile
from pathlib import Path

import cesdh

REPOSITORY = "quickstart"  # your project; created on first upload
OWNER = "me"  # no spaces (it becomes a PROV agent IRI)

# ── A tiny CSV to stand in for your data ─────────────────────────────────────
workdir = Path(tempfile.mkdtemp())
sample = workdir / "hourly_demand.csv"
sample.write_text(
    "timestamp,demand_kw\n2024-01-01T00:00:00Z,3.4\n2024-01-01T01:00:00Z,2.9\n"
)

# ── 1. Upload ────────────────────────────────────────────────────────────────
# force_tier="generic" says "this is raw data, not a CESDM model" - skip schema
# validation. dcat_metadata is what makes it findable later.
result = cesdh.upload_raw(
    str(sample),
    owner=OWNER,
    repository=REPOSITORY,
    branch="main",
    description="Quickstart sample: two hours of household demand",
    force_tier="generic",
    force=True,  # safe to re-run this script
    dcat_metadata={
        "carrier": "electricity",
        "unit": "kW",
        "temporal_resolution": "PT1H",
        "spatial_coverage": "CH",
    },
)
dataset_id = result["dataset_id"]
print(f"1. uploaded   {sample.name} -> {dataset_id}")

# ── 2. Find it ───────────────────────────────────────────────────────────────
# Search by what it *is*, not what it was named.
hits = cesdh.sparql(
    """
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX energy:  <http://energy.ethz.ch/schema#>
SELECT ?dataset_id ?title WHERE {
  GRAPH <http://energy.ethz.ch/graph/catalog> {
    ?dataset a dcat:Dataset ;
             dcterms:identifier ?dataset_id ;
             dcat:title ?title ;
             energy:energyCarrier "electricity" .
    FILTER NOT EXISTS { ?dataset energy:supersededBy ?n }
  }
}""",
    repository=REPOSITORY,
).get("results", [])
print(f"2. found      {len(hits)} electricity dataset(s) in the catalog")

# ── 3. Get it back ───────────────────────────────────────────────────────────
frame = cesdh.download_to_dataframe(dataset_id, repository=REPOSITORY)
print(f"3. downloaded {len(frame)} row(s) into a DataFrame")

# ── 4. Prove nothing changed ─────────────────────────────────────────────────
copy = workdir / "roundtrip.csv"
cesdh.download_to_file(dataset_id, destination=str(copy), repository=REPOSITORY)
same = (
    hashlib.sha256(sample.read_bytes()).hexdigest()
    == hashlib.sha256(copy.read_bytes()).hexdigest()
)
print(f"4. verified   sha256 round-trip: {'identical' if same else 'MISMATCH'}")

print("\nThat's it. The file is versioned in LakeFS, catalogued in Fuseki,")
print("and reachable by anyone with the repository name and its dataset_id.")
