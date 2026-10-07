# ESDH SDK — Basic Functions Reference

Every example below assumes the SDK is installed and configured:

```bash
pip install ./sdk                                      # from the cesdh-client archive
export CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch     # or http://localhost:8080
```

Every function that touches the gateway takes a required `repository`
argument — the platform is multi-repository native, with no default.

---

## 0. Set your identity

Call once at the top of every notebook or script. Every subsequent SDK call
carries your identity in the `X-ESDH-User-Id` and `X-ESDH-User-Name`
headers. The gateway stamps it into PROV-O attribution and the audit log.

```python
import cesdh

cesdh.set_user("alice", "Alice from FEN-team")

# Read back at any time
uid, name = cesdh.get_user()
print(f"Acting as: {name} ({uid})")
```

---

## 1. Repositories

### List all repositories

```python
import requests, os

# This is a raw HTTP call — the SDK does not wrap it because the endpoint
# is deliberately unscoped (no X-CESDH-Repository header).
gateway = os.getenv("CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080")
resp = requests.get(f"{gateway}/repositories", timeout=10)
repos = resp.json()

for r in repos:
    print(f"  {r['name']:30s}  {r.get('title', '')}  ({r.get('dataset_count', 0)} datasets)")
```

### Create a repository

```python
import cesdh

cesdh.set_user("alice", "Alice from FEN-team")

# Strict: raises HTTP 409 if the repository already exists.
# Uploads autoprovision on first write — this is the opt-in pre-flight.
result = cesdh.create_repository(repository="my-new-project")
print(result)
```

### Set repository metadata

```python
cesdh.set_repository_metadata(
    repository="my-new-project",
    title="Swiss Grid Expansion Study",
    owner="FEN-team",
    spatial_coverage="CH",
    energy_carriers=["electricity", "hydro"],
    themes=["GridAnalysis", "Scenario"],
)
```

### Read repository metadata

```python
meta = cesdh.get_repository_metadata(repository="my-new-project")
print(meta)
# {'repository': 'my-new-project', 'title': 'Swiss Grid Expansion Study', ...}
```

### Delete a repository

```python
# Soft-delete: archives immediately, queues physical teardown.
# confirm=True is required to prevent accidental calls.
cesdh.delete_repository(repository="my-new-project", confirm=True)
```

---

## 2. Branches

### List all branches in a repository

```python
import cesdh

branches = cesdh.list_branches(repository="my-project")
for b in branches:
    print(f"  {b.get('branch', b.get('id', '?'))}")
```

### Create a branch

```python
cesdh.set_user("alice", "Alice from FEN-team")

result = cesdh.create_branch(
    "experiment-2030",
    repository="my-project",
    source="main",           # branch off main (default)
)
print(f"Created branch: {result['branch']} at {result.get('head', '?')[:12]}")
```

### Set branch metadata

```python
cesdh.set_branch_metadata(
    "experiment-2030",
    repository="my-project",
    title="2030 Expansion Scenario",
    hypothesis="High renewable penetration in CH",
    target_year="2030",
    energy_carriers=["electricity", "solar", "wind"],
)
```

### Read branch metadata

```python
meta = cesdh.get_branch_metadata("experiment-2030", repository="my-project")
print(meta)
```

### Delete a branch

```python
# The default branch (main) cannot be deleted — raises HTTP 409.
# Commits remain in LakeFS history until garbage collection.
cesdh.delete_branch("experiment-2030", repository="my-project")
```

---

## 3. Datasets

### List all datasets in a repository

```python
import cesdh

datasets = cesdh.list_datasets(repository="my-project")
print(f"Total: {len(datasets)} dataset(s)")
for ds in datasets[:5]:
    print(f"  {ds['dataset_id']}  {ds.get('title', '?')}  [{ds.get('format', '?')}]")
```

### List datasets with filters

```python
# Filter by branch, format, entity class, or scenario
datasets = cesdh.list_datasets(
    repository="my-project",
    branch="main",
    format="csv",
    limit=50,
)
print(f"Found {len(datasets)} CSV dataset(s) on main")
```

### Upload a dataset

```python
import cesdh

cesdh.set_user("alice", "Alice from FEN-team")

result = cesdh.upload_raw(
    "data/hourly_demand.csv",
    owner="FEN-team",
    repository="my-project",
    branch="experiment-2030",
    description="Hourly electricity demand, synthetic sample",
    dcat_metadata={
        "keywords": ["electricity", "demand"],
        "theme": "Grid",
        "carrier": "electricity",
        "spatial_coverage": "CH",
    },
)
print(f"dataset_id:  {result['dataset_id']}")
print(f"commit:      {result.get('lakefs_commit_id', '?')}")
print(f"validation:  {result.get('validation_status', '?')}")
```

### Download a dataset to a DataFrame

```python
import cesdh

df = cesdh.download_to_dataframe("dataset_abc123", repository="my-project")
print(df.head())
print(f"Shape: {df.shape}")
```

### Download a dataset to disk

Use this for large files (HDF5, Parquet) that exceed available memory:

```python
path = cesdh.download_to_file(
    "dataset_abc123",
    destination="./downloads",    # directory — original filename is resolved
    repository="my-project",
)
print(f"Saved to: {path}")
```

### Drop a dataset from a branch (soft-delete)

```python
cesdh.set_user("alice", "Alice from FEN-team")

# The physical file stays in LakeFS history. The catalog marks it
# as superseded — previous commits still reference it.
result = cesdh.delete_dataset(
    "dataset_abc123",
    repository="my-project",
    branch="experiment-2030",
)
print(result)
```

---

## 4. Update a dataset (stage + commit)

ESDH uses Git-like semantics: stage new bytes, inspect the diff, then
commit atomically. This replaces the mental model of "updating a file"
with the same workflow as `git add` + `git commit`.

### Stage

```python
import cesdh

cesdh.set_user("alice", "Alice from FEN-team")

stage = cesdh.stage_dataset_update(
    dataset_id="dataset_abc123",
    file_path="./updated_demand.csv",
    repository="my-project",
    branch="experiment-2030",
)
parent_sha = stage.get("parent_commit_sha", "")
print(f"Staged. Parent commit: {parent_sha[:12]}")
```

### Inspect staged changes

```python
diff = cesdh.get_staged_changes(
    dataset_id="dataset_abc123",
    repository="my-project",
    branch="experiment-2030",
)
print(diff)
```

### Commit

```python
result = cesdh.commit_dataset_update(
    dataset_id="dataset_abc123",
    repository="my-project",
    commit_message="Update demand profile for Q4 2025",
    branch="experiment-2030",
    parent_commit_sha=parent_sha,  # from the stage step
)
print(f"Committed: {result.get('commit_id', result)}")
```

### Discard staged changes

```python
# Changed your mind? Drop the staged bytes without committing.
cesdh.discard_staged_changes(
    "dataset_abc123",
    repository="my-project",
    branch="experiment-2030",
)
```

---

## 5. Clone a branch

Download every active dataset on a branch into a local directory. The
local copy is a snapshot at the branch HEAD — subsequent commits do not
change the local files.

```python
import cesdh

cesdh.set_user("alice", "Alice from FEN-team")

paths = cesdh.clone_branch(
    "main",
    "./local-mirror",
    repository="my-project",
)
print(f"Cloned {len(paths)} file(s)")
for p in paths[:5]:
    print(f"  {p}")
```

### Clone only one tier

```python
# Only the raw/ tier — skip transformed/ and analytics/
paths = cesdh.clone_branch(
    "main",
    "./raw-only",
    repository="my-project",
    tier="raw",
)
```

---

## 6. Search

### Keyword search

```python
import cesdh

hits = cesdh.search("solar irradiation CH", repository="my-project", limit=10)
for r in hits.get("results", []):
    print(f"  {r.get('dataset_id')}  {r.get('title')}")
```

### Direct SPARQL

```python
results = cesdh.sparql("""
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX energy:  <http://energy.ethz.ch/schema#>

SELECT ?dataset_id ?title ?format WHERE {
  GRAPH <http://energy.ethz.ch/graph/catalog> {
    ?dataset a dcat:Dataset ;
             dcterms:identifier ?dataset_id ;
             dcat:title ?title ;
             dcat:format ?format .
    FILTER NOT EXISTS { ?dataset energy:supersededBy ?newer }
  }
} ORDER BY DESC(?dataset_id) LIMIT 20
""", repository="my-project")

for row in results.get("results", []):
    print(f"  {row['dataset_id']}  [{row['format']}]  {row['title']}")
```

---

## 7. Tags

Tags are immutable references — once created, they never move. Use them
to pin a finished run so a paper can cite a permanent identifier.

### Create a tag

```python
cesdh.set_user("alice", "Alice from FEN-team")

result = cesdh.create_tag(
    "v1-ch-expansion-2030",
    repository="my-project",
    ref="experiment-2030",      # branch name or commit id
)
print(f"Tag created: {result['tag']} -> {result['commit_id'][:12]}")
```

### List tags

```python
tags = cesdh.list_tags(repository="my-project")
for t in tags:
    print(f"  {t['tag']:30s}  {t['commit_id'][:12]}")
```

---

## 8. Scenarios and lineage

### List scenarios

```python
scenarios = cesdh.list_scenarios(repository="my-project")
print(scenarios)

# Filter by branch
branch_scenarios = cesdh.list_scenarios(repository="my-project", branch="main")
```

### Scenario summary (DataFrame)

```python
df = cesdh.scenario_summary(repository="my-project")
print(df)
# Columns: scenario_id, target_year, geographic_region,
#           temporal_resolution, owner, description, version, dataset_count
```

### Link a dataset to a scenario after upload

```python
cesdh.relate_data_to_scenarios(
    "dataset_abc123",
    scenario_ids=["baseline_CH_2030", "sub_CH_RES_expansion"],
    repository="my-project",
)
```

### Unlink a dataset from a scenario

```python
cesdh.unlink_data_from_scenario(
    "dataset_abc123",
    "baseline_CH_2030",
    repository="my-project",
)
```

### Trace scenario lineage

```python
df = cesdh.get_scenario_lineage("baseline_CH_2030", repository="my-project")
print(df)
# Columns: dataset_id, title, branch, scenario_id,
#           parent_scenario_id, parent_dataset_ids, activity_id
```

---

## 9. Audit and history

### Branch changes

```python
df = cesdh.get_branch_changes("experiment-2030", repository="my-project")
print(df)
# Columns: path, type, size_bytes, last_modified
```

### File history across branches

```python
df = cesdh.get_file_branches_history(
    "raw/dataset_abc123/demand.csv",
    repository="my-project",
)
print(df)
# Columns: branch, path, owner, checksum, size_bytes, last_modified
```

---

## Quick reference

| Task | Call |
|---|---|
| Set identity | `cesdh.set_user("alice", "Alice from FEN")` |
| Create repository | `cesdh.create_repository(repository="my-project")` |
| List branches | `cesdh.list_branches(repository="my-project")` |
| Create branch | `cesdh.create_branch("dev", repository="my-project")` |
| List datasets | `cesdh.list_datasets(repository="my-project")` |
| Upload | `cesdh.upload_raw("file.csv", owner="you", repository="my-project")` |
| Download (DataFrame) | `cesdh.download_to_dataframe("id", repository="my-project")` |
| Download (file) | `cesdh.download_to_file("id", ".", repository="my-project")` |
| Stage + commit | `cesdh.stage_dataset_update(...)` then `cesdh.commit_dataset_update(...)` |
| Clone branch | `cesdh.clone_branch("main", "./mirror", repository="my-project")` |
| Search | `cesdh.search("query", repository="my-project")` |
| Create tag | `cesdh.create_tag("v1", repository="my-project", ref="main")` |
| Drop dataset | `cesdh.delete_dataset("id", repository="my-project")` |
