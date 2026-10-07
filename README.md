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

> **Windows users:** Make sure Python is added to your `PATH` during
> installation (check "Add Python to PATH" in the installer). Use
> `python` and `pip` (not `python3` / `pip3`) in all commands below.

### 1. Install the SDK

From the root of this extracted archive:

**Linux / macOS:**

```bash
pip install ./sdk
```

**Windows (Command Prompt or PowerShell):**

```powershell
pip install .\sdk
```

This installs the `cesdh` package and its dependencies (pandas, h5py, requests,
pyyaml).

### 2. Configure your environment

Copy the template and fill in the credentials you received:

**Linux / macOS:**

```bash
cp .env.template .env
```

**Windows (Command Prompt):**

```cmd
copy .env.template .env
```

**Windows (PowerShell):**

```powershell
Copy-Item .env.template .env
```

Edit `.env` and replace the `<ask-esdh-manager>` placeholders:

```bash
CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch
LAKEFS_ACCESS_KEY_ID=your_access_key_here
LAKEFS_SECRET_ACCESS_KEY=your_secret_key_here
```

The SDK auto-loads `.env` from the current working directory when
`LAKEFS_ACCESS_KEY_ID` is not already in the environment.

Alternatively, set the variables directly in your shell:

**Linux / macOS:**

```bash
export CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch
export LAKEFS_ACCESS_KEY_ID=your_access_key_here
export LAKEFS_SECRET_ACCESS_KEY=your_secret_key_here
```

**Windows (Command Prompt):**

```cmd
set CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch
set LAKEFS_ACCESS_KEY_ID=your_access_key_here
set LAKEFS_SECRET_ACCESS_KEY=your_secret_key_here
```

**Windows (PowerShell):**

```powershell
$env:CESDH_DATA_HUB_ENDPOINT = "https://fen-esdh.ch"
$env:LAKEFS_ACCESS_KEY_ID = "your_access_key_here"
$env:LAKEFS_SECRET_ACCESS_KEY = "your_secret_key_here"
```

### 3. Start a trial branch

Before uploading data, create an isolated branch in your repository:

**Linux / macOS:**

```bash
python3 start_trial.py trial-<your-username> "my-experiment-name"
```

**Windows:**

```cmd
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
routing traffic for `fen-esdh.ch`. Verify connectivity:

- **Linux / macOS:** `curl -v https://fen-esdh.ch/health`
- **Windows (PowerShell):** `Invoke-WebRequest -Uri https://fen-esdh.ch/health`

If it hangs, add `*.fen-esdh.ch` to your split-tunnel include list, or
disable split-tunnel for this session.

**2. `SSLError: certificate verify failed`** — The VPN's HTTPS
interception certificate is not trusted by Python.

- **Linux / macOS:** `export REQUESTS_CA_BUNDLE=/path/to/corp-ca.pem`
- **Windows (Command Prompt):** `set REQUESTS_CA_BUNDLE=C:\path\to\corp-ca.pem`
- **Windows (PowerShell):** `$env:REQUESTS_CA_BUNDLE = "C:\path\to\corp-ca.pem"`

**3. `socket.gaierror: Name or service not known`** — DNS does not
resolve the gateway hostname. Use the IP directly:

- **Linux / macOS:** `export CESDH_DATA_HUB_ENDPOINT=http://10.42.0.17:8080`,
  or add the hostname to `/etc/hosts`.
- **Windows:** `set CESDH_DATA_HUB_ENDPOINT=http://10.42.0.17:8080`,
  or add the hostname to `C:\Windows\System32\drivers\etc\hosts` (run your
  editor as Administrator).

**4. First request hangs for 60+ seconds, then succeeds** — This is
the gateway cold-start (Ollama loading its model). Wait it out, or
pre-warm with `cesdh.list_datasets(repository="quickstart", limit=1)`.

For the full playbook, see [`docs/vpn_troubleshooting.md`](docs/vpn_troubleshooting.md).

---

## Section B: Dashboard

The web dashboard at **<https://dash.fen-esdh.ch>** lets you explore the
platform without writing any code.

### Welcome & identity

On your first visit the dashboard shows a welcome page. Enter a **Display
name** (shown in commit history and audit logs) and optionally a **User ID**
(auto-generated if left blank), then click **Enter the workspace**. This is
self-declared identity — the same model as `git config user.name`.

### Layout

After signing in, the dashboard has three regions:

| Region | Location | What it contains |
|---|---|---|
| **View switcher** | Top centre | Toggle between the two views below. |
| **Main panel** | Left | The active view's content (search results or dataset cards). |
| **Drawer** | Right | Repository and branch selectors, upload button, discover filters, gateway status. |

The **left sidebar** (collapsed by default — click the hamburger or swipe
from the left edge) shows a description of the active view and quick links
to the Gateway API docs, User Guide, and SDK Reference.

### View 1: Discovering & Searching

Search the entire catalog across one or all repositories. Type keywords in
the search bar (or leave it blank to browse everything), pick a repository
scope, and adjust the result limit.

**Drawer filters** (right panel) narrow results by:

- **Branch** — only datasets on this branch
- **Format** — `csv`, `yaml`, `json`, `hdf5`, etc.
- **Domain / theme** — topics assigned during upload
- **Include superseded** — toggle to show older revisions that have been
  replaced by a newer upload

Each result is a **dataset card** showing the format badge, title, dataset
ID, description, keywords, and a provenance strip (owner, repository,
branch, upload date, file size). Cards have four action buttons:

| Button | Action |
|---|---|
| 💻 | Copy a Python SDK snippet for this dataset |
| 📝 | Stage an update (upload a new version and commit) |
| ⬇ | Download the file to your browser |
| 🗑 | Drop the dataset from this branch (soft-delete) |

Below the cards, an **Export these results (CSV)** button downloads the
current result set as a spreadsheet.

### View 2: Working in a Repository

Browse the datasets in one specific repository and branch. The drawer on
the right controls which repository and branch you are looking at.

**Drawer actions:**

- **Repository selector** — pick an existing project or type a new name.
- **Branch selector** — pick a branch. Use ➕ to create one, 🗑 to delete
  one, 🧬 to clone the branch as a ZIP download.
- **New repository** / **Delete repository** — lifecycle buttons.
- **Upload dataset** — opens a dialog to upload a file to the selected
  branch (max ~200 MB via browser; for larger files the dialog offers a
  ready-to-run SDK snippet).

The main panel shows a breadcrumb (`Repositories › repo › ⎇ branch`), a
repository summary card, and the dataset grid. Use the filter/sort row
above the grid to narrow by title, keyword, or owner, and sort by newest,
name, or size.

### SDK snippet generator

Every dataset card includes a **💻** button that opens a dialog with a
copy-pasteable Python snippet:

```python
import cesdh

df = cesdh.download_to_dataframe(
    "dataset_abc123",
    repository="my-project",
)
```

### Large file upload handoff

When you select a file larger than 200 MB in the Upload dialog, the browser
cannot handle it. Click **Generate SDK upload snippet** to get a
personalised `cesdh.upload_raw(...)` call pre-filled with the repository,
branch, owner, title, description, and domain tags you already typed.

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

**Linux / macOS:**

```bash
cd /path/to/cesdh-client
python3 examples/quickstart.py
```

**Windows:**

```cmd
cd C:\path\to\cesdh-client
python examples\quickstart.py
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

**Linux / macOS:**

```bash
python3 examples/raw_data_hub_lifecycle.py
```

**Windows:**

```cmd
python examples\raw_data_hub_lifecycle.py
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

> Commands below use Linux/macOS syntax. On Windows, replace `pip3`/`python3`
> with `pip`/`python`, forward slashes with backslashes, and `export VAR=val`
> with `set VAR=val` (Command Prompt) or `$env:VAR = "val"` (PowerShell).

| Task | Command |
|---|---|
| Install SDK | `pip install ./sdk` (Linux/macOS) · `pip install .\sdk` (Windows) |
| Set remote endpoint | `export CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch` (Linux/macOS) · `set CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch` (Windows) |
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
