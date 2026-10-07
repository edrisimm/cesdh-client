# Energy Systems Data Hub — Researcher Testing Guide

Welcome to the remote trial of the **Energy Systems Data Hub** (the Data Hub).
This package contains everything you need to interact with the shared platform
from your local machine: the Python SDK, example scripts, and sample data.

---

## What's new

| Feature | SDK call | When to use |
|---|---|---|
| Identify yourself | `cesdh.set_user("alice", "Alice from FEN-team")` | Once at the top of every notebook or script. Every SDK call now carries `X-ESDH-User-Id` / `X-ESDH-User-Name`. |
| Clone a branch | `cesdh.clone_branch("main", "./mirror", repository="<repo>")` | Mirror every active dataset on a branch into a local directory. Git-like: the local copy is a snapshot at HEAD. |
| Commit an update | `cesdh.stage_dataset_update(...)` then `cesdh.commit_dataset_update(...)` | Replace "updating a dataset" with stage + commit, exactly like `git add` + `git commit`. |
| Delete a branch | `cesdh.delete_branch("old-branch", repository="<repo>")` | Remove a branch pointer (commits stay in LakeFS history). |
| Repository metadata | `cesdh.set_repository_metadata(repository, title, ...)` | Annotate a project with owner, themes, spatial coverage. |
| Branch metadata | `cesdh.set_branch_metadata(branch, repository, title, ...)` | Tag a branch with hypothesis, target year, keywords. |

See `examples/03_clone_branch.py`, `examples/04_commit_update.py`, and
`examples/05_user_identity.py` for runnable demos.

---

**Service endpoints (Infomaniak VPS):**

| Service | URL |
|---|---|
| Gateway API (Swagger) | <https://fen-esdh.ch/docs> |
| Dashboard | <https://dash.fen-esdh.ch> |
| Documentation | <https://docs.fen-esdh.ch> |
| LakeFS UI | <https://lakefs.fen-esdh.ch> |

---

## Section A: Quickstart & Prerequisites

### Requirements

- Python 3.9 or later
- `pip` (included with Python)
- Credentials from esdh manager (LakeFS access key + secret key)

### 1. Install the SDK

From the root of this extracted archive:

```bash
pip install ./sdk
```

This installs the `cesdh` package and its dependencies (pandas, h5py, requests,
pyyaml).

### 2. Configure your environment

Copy the template and fill in the credentials you received:

```bash
cp .env.template .env
```

Edit `.env` and replace the `<ask-esdh-manager>` placeholders:

```bash
CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch
LAKEFS_ACCESS_KEY_ID=your_access_key_here
LAKEFS_SECRET_ACCESS_KEY=your_secret_key_here
```

The SDK auto-loads `.env` from the current working directory when
`LAKEFS_ACCESS_KEY_ID` is not already in the environment.

Alternatively, export the variables directly in your shell:

```bash
export CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch
export LAKEFS_ACCESS_KEY_ID=your_access_key_here
export LAKEFS_SECRET_ACCESS_KEY=your_secret_key_here
```

### 3. Start a trial branch

Before uploading data, create an isolated branch in your repository:

```bash
python start_trial.py trial-<your-username> "my-experiment-name"
```

This creates a timestamped branch (e.g. `trial-20260908T093000Z-my-experiment-name`)
and tags the empty starting point. You can run multiple trials in parallel —
each gets its own branch.

### 4. Verify connectivity

```python
import cesdh

# This should return a list (possibly empty) without errors.
datasets = cesdh.list_datasets(repository="quickstart")
print(f"Connected! Found {len(datasets)} dataset(s).")
```

If you get a connection error, check that `https://fen-esdh.ch/health` is
reachable in your browser.

> **Local Mode vs Remote Mode**
>
> The SDK uses the same code for both. The only difference is the
> `CESDH_DATA_HUB_ENDPOINT` environment variable:
>
> | Mode | Value |
> |---|---|
> | Local (Docker stack) | `http://localhost:8080` (default) |
> | Remote (VPS trial) | `https://fen-esdh.ch` |

### VPN troubleshooting

If you connect through a VPN, split-tunnel, or SSH-over-HTTPS proxy,
SDK calls may fail in ways that look like gateway outages but are actually
your local network. The four most common patterns:

**1. `ConnectionError: HTTPSConnectionPool(...)`** — The VPN is not
routing traffic for `fen-esdh.ch`. Run
`curl -v https://fen-esdh.ch/health` to confirm. If it hangs, add
`*.fen-esdh.ch` to your split-tunnel include list, or disable
split-tunnel for this session.

**2. `SSLError: certificate verify failed`** — The VPN's HTTPS
interception certificate is not trusted by Python. Set
`export REQUESTS_CA_BUNDLE=/path/to/corp-ca.pem`.

**3. `socket.gaierror: Name or service not known`** — DNS does not
resolve the gateway hostname. Use the IP directly:
`export CESDH_DATA_HUB_ENDPOINT=http://10.42.0.17:8080`, or add the
hostname to your `/etc/hosts`.

**4. First request hangs for 60+ seconds, then succeeds** — This is
the gateway cold-start (Ollama loading its model). Wait it out, or
pre-warm with `cesdh.list_datasets(repository="quickstart", limit=1)`.

For the full playbook, see [`docs/vpn_troubleshooting.md`](docs/vpn_troubleshooting.md).

---

## Section B: Dashboard & Catalog Browsing

The web dashboard at **<https://dash.fen-esdh.ch>** lets you explore the
platform without writing any code.

### Catalog tab

The **Catalog** tab is the main entry point. Use the sidebar filters to narrow
results:

- **Repository** — select the project repository to browse (e.g. `quickstart`,
  `my-project`)
- **Branch** — filter by LakeFS branch (e.g. `main`, `dev-2024-update`)
- **Format** — filter by file type (`yaml`, `csv`, `json`, `h5`)
- **Entity Class** — filter by CESDM entity class (e.g. `ThermalGenerationUnit`)

Each row shows the dataset ID, title, format, branch, upload time, and
validation status. Click a dataset ID to see its full metadata.

### SDK snippet generator

The Catalog tab includes a **"Copy SDK snippet"** button for each dataset. It
generates a ready-to-paste Python snippet:

```python
import cesdh
df = cesdh.download_to_dataframe("dataset_abc123", repository="my-project")
df.head()
```

### Query Console tab

For advanced queries, use the **Query Console** tab:

- **"Ask in plain English"** — type a natural-language question (keyword-based
  matching during the trial; see the full documentation for supported patterns)
- **Direct SPARQL** — paste a SPARQL query directly against the knowledge graph

### Other tabs

- **Datasets** — full inventory with text search
- **Entity Classes** — bar chart of CESDM class distribution across datasets
- **Scenarios** — datasets grouped by scenario

---

## Section C: End-to-End Testing Workflows

All examples below assume you have configured your `.env` as described in
Section A. Run them from the directory where `.env` lives.

### Workflow 1: Upload a dataset

```python
import cesdh

result = cesdh.upload_raw(
    "sample_data/hourly_demand.csv",   # or any file on your machine
    owner="your-name",                 # no spaces (becomes a PROV agent IRI)
    repository="my-project",           # created automatically on first upload
    branch="main",
    description="Hourly electricity demand, 8 hours, synthetic sample",
    force_tier="generic",              # skip CESDM validation for raw data
    dcat_metadata={
        "carrier": "electricity",
        "unit": "kW",
        "temporal_resolution": "PT1H",
        "spatial_coverage": "CH",
    },
)
print(f"Uploaded! dataset_id = {result['dataset_id']}")
```

### Workflow 2: Search the catalog

```python
import cesdh

# Natural-language search (keyword-based during trial)
hits = cesdh.search("electricity CH", repository="my-project", limit=10)
print(hits)
```

### Workflow 3: Query with SPARQL

```python
import cesdh

results = cesdh.sparql("""
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX energy:  <http://energy.ethz.ch/schema#>
SELECT ?dataset_id ?title ?format WHERE {
  GRAPH <http://energy.ethz.ch/graph/catalog> {
    ?dataset a dcat:Dataset ;
             dcterms:identifier ?dataset_id ;
             dcat:title ?title ;
             energy:fileFormat ?format .
    FILTER NOT EXISTS { ?dataset energy:supersededBy ?newer }
  }
}
ORDER BY DESC(?dataset_id)
LIMIT 20
""", repository="my-project")

for row in results.get("results", []):
    print(f"  {row['dataset_id']}  {row['format']}  {row['title']}")
```

### Workflow 4: Download to a DataFrame

```python
import cesdh

# Use the dataset_id from a search or upload result
df = cesdh.download_to_dataframe("dataset_abc123", repository="my-project")
print(df.head())
print(f"Shape: {df.shape}")
```

For large binary files (HDF5, datapackage), download to disk instead:

```python
path = cesdh.download_to_file("dataset_abc123", destination=".", repository="my-project")
print(f"Saved to: {path}")
```

### Workflow 5: List branches, scenarios, and datasets

```python
import cesdh

REPO = "my-project"

# List all branches
branches = cesdh.list_branches(repository=REPO)
print("Branches:", [b["id"] for b in branches])

# List all scenarios
scenarios = cesdh.list_scenarios(repository=REPO)
print("Scenarios:", scenarios)

# List datasets with filters
datasets = cesdh.list_datasets(
    repository=REPO,
    branch="main",
    format="csv",
    limit=50,
)
print(f"Found {len(datasets)} CSV dataset(s) on main")
```

### Workflow 6: Run the quickstart example

The included `examples/quickstart.py` performs a full round-trip (upload,
search, download, verify SHA-256) in under a minute:

```bash
cd /path/to/cesdh-client
python examples/quickstart.py
```

---

## Advanced examples

The `examples/` directory includes four additional end-to-end workflows.
Sample data files are bundled under `data/`.

| Script | What it demonstrates |
|---|---|
| `03_clone_branch.py` | Clone every active dataset on a branch into a local working tree — Git-like snapshot at HEAD. |
| `04_commit_update.py` | Stage a new file version and commit it atomically — the Git-like stage + commit flow. |
| `05_user_identity.py` | Set and inspect the calling identity (`set_user` / `get_user`) — every call carries your name. |
| `raw_data_hub_lifecycle.py` | Stage, upload, annotate and search raw time-series data (CSV) with DCAT metadata. Self-contained — fetches open data or falls back to synthetic profiles. |
| `multi_researcher_cesdm_workflow.py` | Two-researcher handoff: A curates raw data, B builds a CESDM scenario and sub-scenario delta, then lineage traces back to A's inputs. |
| `project_tutorial_ch_neighbours.py` | Builds the CESDM "Switzerland + neighbours" 5-country model, exports in 7 formats, uploads all artifacts, and verifies the round-trip. |
| `fen_general_repository_linking.py` | Zero-copy cross-repository linking: a shared repository holds raw data once, two consumer projects reference it via link manifests. |

Run any of them from the `cesdh-client/` directory:

```bash
python examples/raw_data_hub_lifecycle.py
```

> **Note:** `multi_researcher_cesdm_workflow.py` and
> `project_tutorial_ch_neighbours.py` require the CESDM Toolbox
> (`cesdm/sweet-cosi-cesdm`) to be installed locally for scenario building
> and delta inflation. They will print a clear error if the toolbox is not
> found. `raw_data_hub_lifecycle.py` and `fen_general_repository_linking.py`
> work with the SDK alone.

---

## Section D: Feedback & Issue Reporting

Your feedback during this trial is essential. Please report:

### Bugs

- **What happened** — error message, traceback, or unexpected behavior
- **What you expected** — the correct outcome
- **How to reproduce** — the code or dashboard action that triggered it
- **Environment** — Python version, OS, SDK version (`pip show cesdh`)

### Query Console issues

- **The question you asked** (exact text)
- **The rule name** shown in the banner above the results
- **Whether the results were correct** — wrong rule fired? Missing results?

### Schema gaps

If your data does not fit the CESDM validation:

- **File format and structure** — what the file looks like
- **Expected tier** — should it be Tier 1 (generic), Tier 2 (CESDM), or Tier 3
  (scenario)?
- **What the validator reported** — the error or warning message

### How to submit

Send your report to esdh manager with the subject line
**"[CESDH Trial] ..."**. Include the dataset ID and repository name when
applicable.

---

## Quick reference

| Task | Command |
|---|---|
| Install SDK | `pip install ./sdk` |
| Set remote endpoint | `export CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch` |
| Set your identity | `cesdh.set_user("alice", "Alice from FEN-team")` |
| Upload a file | `cesdh.upload_raw("file.csv", owner="you", repository="my-project")` |
| Search | `cesdh.search("query", repository="my-project")` |
| Download to DataFrame | `cesdh.download_to_dataframe("dataset_id", repository="my-project")` |
| Clone a branch | `cesdh.clone_branch("main", "./mirror", repository="my-project")` |
| Stage + commit | `cesdh.stage_dataset_update(...)` then `cesdh.commit_dataset_update(...)` |
| List datasets | `cesdh.list_datasets(repository="my-project")` |
| Open dashboard | <https://dash.fen-esdh.ch> |
| Open docs | <https://docs.fen-esdh.ch> |
| API explorer | <https://fen-esdh.ch/docs> |
