# Groupe-E Planning Process Provisioning Example

A beginner-friendly walkthrough of the script `groupe_e_planning_process_provisioning.py`.

This guide is aimed at **key users** who manage or need to work with data, such as that related to Groupe-E's network planning, without needing any programming knowledge or to use formal data management tools.

The purpose of the script is to serve as a checklist of *data management actions* rather than a wall of code.

---
## * Prerequisites

ESDH software development kit (SDK) must previously be installed. Installation steps can be done by following the ***Trial onboarding*** guidance (https://docs.fen-esdh.ch/trial/onboarding/).

---

## 1. Purpose of the Script

Groupe-E's scripts simulate the actions required in the data lifecycle process to perform a **multi-stage planning process analysis** that transforms raw data from the Swiss federal government into published  figures used in decisions related to the power grid.

In practice, this means that the files pass through five folders:

```
raw_input_data                     (original files downloaded from BFS and SOLKAT)
        │
        ▼
input_clean_data                   (cleaned, filtered versions of the above)
        │
        ▼
regionalization/all_pckls          (time-series data per region)
        │
        ▼
grid_planning_results              (expansion plan + cost tables)
        │
        ▼
grid_planning_publication/figures  (PNG charts for the paper)
```

Each step **depends on the previous one**. If a figure in the final paper ever looks wrong, somebody has to be able to answer:

> *"Which raw input, which cleaning run, and which grid-planning run produced this chart?"*

Manually tracking all of this through data records such as Excel spreadsheets, the data creator’s knowledge, and shared drives carries a high risk of errors. This script explains how a user uploads all files to the **Semantic Lakehouse**, which is a single, versioned, and searchable data hub. Furthermore, and more importantly, **it has the capability to record the parent-child relationship between each result and its entries** using the **PROV-O** standard for data provenance.

Once this example is completed (or the script is run), anyone, including those who will be working with Group E material in the future, will be able to:

- search the hub for a figure by name,
- ask *"where did this come from?"* and walk the chain back to the original BFS / SOLKAT files,
- ask *"what else did this input produce?"* and walk forward.

That is what the script sets up.

---

## 2. How to run it

```bash
# VPS development stack
CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch
    python examples_cesdh/groupe_e_planning_process_provisioning.py

# Keep the created repository and datasets after the run
KEEP=1 python examples_cesdh/groupe_e_planning_process_provisioning.py

# Skip local filesystem checks (paths differ on another machine)
SKIP_FILE_CHECKS=1 python examples_cesdh/groupe_e_planning_process_provisioning.py
```

Environment variables let you override every path so the script is portable across machines without editing code.

---

## 3. The script at a glance

The script is organized in three layers:

| Layer              | What it does                                              | You change it?             |
|--------------------|-----------------------------------------------------------|----------------------------|
| **Configuration**  | Paths, identifiers, repository name, run IDs.             | Often, via env vars.       |
| **Helpers**        | Tiny utilities (printing, file checks, single upload call).| Rarely.                    |
| **Main flow**      | The six pipeline phases plus the audit at the end.        | Only to add a new phase.   |

The **main flow** is what matters day-to-day. Read sections 4 and 5 first; come back to section 6 only when you need to tune the script.

---

## 4. Main functionalities (the pipeline phases)

Each phase is a small, named function. The phases run in order; each one returns the dataset IDs needed by the next, which is how the parent–child lineage is built.

### 4.1 `phase_0_setup()`.Configure identity and create the repository

**Purpose.** Tells the hub ***who*** is uploading (`yaman` / `Yaman`) and ***where*** (`groupe-e-planning-process` on branch `main`). Creates the repository if it does not exist; if it does exist, just reuse it.

**Why it matters.** All provenance records are written under this identity. Once the repository is set up, every later upload lives inside it.

---

### 4.2 `phase_1_raw()`. Upload `raw_input_data/`

**Purpose.** Uploads the two original source files:

- the **BFS** building register (Excel), and
- the **SOLKAT** solar-potential intersection (Excel).

**Returns.** The two new dataset IDs (`bfs_raw`, `solk_raw`).

**Why it matters.** These are the *root nodes* of the entire lineage graph. Nothing else in the pipeline is an ancestor of them.

---

### 4.3 `phase_2_clean()`. Upload `input_clean_data/`

**Purpose.** Uploads the three cleaned CSV inputs:

- `input_BFS_HAK_EGID.csv` - derived from the BFS raw file,
- `input_PV_HAK_EGID.csv`  - derived from the SOLKAT raw file,
- `input_scenarios.csv`    - hand-curated, **no** parent.

Each cleaned file is tagged with the PROV-O link `derived_from=<raw dataset id>` and a `run_id` of `clean_2024-04-30`, so the hub knows it was produced by a single cleaning activity.

**Why it matters.** This is the first place lineage is exercised: a later user can ask *"which raw file did this cleaned file come from?"* and get an immediate answer.

---

### 4.4 `phase_3_pckl()`. Upload `regionalization/all_pckls/`

**Purpose.** Walks the local `all_pckls/` folder, uploads every `*.pckl` file, and links **all three** cleaned inputs as parents of **each** pckl. All pckls share the same `run_id`: `regionalize_2024-04-30`.

**Why it matters.** Regionalization consumes all three cleaned inputs together; recording that shared run is what lets the hub show "3 inputs → 1 run → many outputs" as a single, queryable activity.

---

### 4.5 `phase_4_grid()`. Upload `grid_planning_results/`

**Purpose.** Uploads the two grid-planning outputs (results and costs), linking them to **every pckl** as a parent and tagging them with the `grid_planning_2024-04-30` run.

**Why it matters.** The results table and the cost table are both produced by the same grid-planning run from the same set of pckl files. Sharing the `run_id` makes that single run a queryable group, instead of two unrelated uploads.

---

### 4.6 `phase_5_figures()`. Upload `grid_planning_publication/figures/`

**Purpose.** Uploads each publication PNG (the list in `FIGURES`) and links both the results and costs tables as parents, under the `publication_2024-04-30` run.

**Why it matters.** This is the last step of this example data lifecycle. Once a figure is uploaded, *any* user can search for it by name and traverse back through every stage, all the way to the BFS and SOLKAT files in Phase 1.

**Note 1.** *When uploading data assets, the addition of **quality metadata** improves their discoverability.*

---

### 4.7 `phase_6_audit()`. Search and traverse the lineage

**Purpose.** Demonstrates the four questions the hub should now be able to answer for any dataset:

1. **Natural-language search** `cesdh.search("fig_PV2Dem_Cost_default_en.png")`.
2. **Ancestors of a single figure** `cesdh.get_lineage(id, direction="ancestors")` prints the chain *"where did this come from?"*.
3. **Full lineage (both directions)** ancestors + self + descendants of the same figure, to see what else was produced in the same chain.
4. **Pipeline view of the branch** `cesdh.get_pipeline(repo, branch)` lists every derivation edge in the repository, i.e. the whole activity graph at a glance.

**Why it matters.** This phase is the *acceptance test* of the provisioning: if it prints a non-empty lineage, the script has done its job. If it prints `(no ancestors …)`, a `derived_from=` argument was missed at upload time.

---

### 4.8 `main()`. The orchestrator

**Purpose.** Prints a banner, calls each phase in order, and prints a final summary table:

```
raw_input_data                : 2 datasets
input_clean_data              : 3 datasets
regionalization/all_pckls     : N datasets
grid_planning_results         : 2 datasets§
grid_planning_publication/fig : 7 datasets
```

**Note 2:** Additionally, it prints the cleanup command `cesdh.delete_repository('groupe-e-planning-process', confirm=True)` so you can wipe the demo state at any time by dropping down.

---

## 5. The `cesdh` SDK calls behind each phase

The user-facing primitives you will see in the audit phase are:

| SDK call                                       | What it does for a non-programmer                              |
|------------------------------------------------|----------------------------------------------------------------|
| `cesdh.set_user(owner, display)`               | Identifies the uploader in every provenance record.            |
| `cesdh.create_repository(repository=...)`      | Creates a new top-level container (like opening a new project).|
| `cesdh.upload(...)`                            | Uploads a single file with metadata and optional lineage.      |
| `cesdh.search(query, repository, limit)`       | Full-text search across datasets.                              |
| `cesdh.get_lineage(id, repository, direction)` | Walks the parent/child graph of a dataset.                     |
| `cesdh.get_pipeline(repository, branch)`       | Returns every derivation edge in the repository as a table.    |
| `cesdh.delete_repository(repository, confirm)` | Removes everything created in this script (idempotent cleanup).|

The single most important upload-level **argument for lineage** is `derived_from=[...]`: it is what tells the hub *"this dataset was produced from those datasets"*. Without it, it is not possible to record the lineage.

---

## 6. Configuration (read once, override as needed)

Everything in this section is a **default value** you can change through environment variables without touching the code.

| Variable         | Default                                                                 | Meaning                                            |
|------------------|-------------------------------------------------------------------------|----------------------------------------------------|
| `REPO`           | `groupe-e-planning-process`                                             | Repository name in the hub.                        |
| `BR`             | `main`                                                                  | Target branch.                                     |
| `OWNER`          | `yaman`                                                                 | Identity used for every upload.                    |
| `RAW_BFS`        | `…/raw_downloaded/BFS_VS_VD_FR_GE.xlsx`                                 | Raw BFS register.                                  |
| `RAW_SOLK`       | `…/raw_downloaded/SOLKAT_CH_DACH_Intersect_GroupE.xlsx`                 | Raw solar potential.                               |
| `CLEAN_BFS`      | `…/data_cleaned/input_BFS_HAK_EGID.csv`                                 | Cleaned BFS.                                       |
| `CLEAN_PV`       | `…/data_cleaned/input_PV_HAK_EGID.csv`                                  | Cleaned PV.                                        |
| `CLEAN_SCN`      | `…/data_cleaned/input_scenarios.csv`                                    | Scenario assumptions.                              |
| `PCKL_DIR`       | `…/all_pckls`                                                           | Folder of regionalized `.pckl` files.              |
| `OGP1`, `OGP2`   | `…/grid_planning/2024-04-30_expansion_results.xlsx` (and `_costs`)       | Grid-planning outputs.                             |
| `FIG_DIR`        | `…/paper/figures`                                                       | Folder of publication PNGs.                        |
| `RUN_CLEAN`      | `clean_2024-04-30`                                                      | PROV-O activity id for the cleaning step.          |
| `RUN_REGIONALIZE`| `regionalize_2024-04-30`                                                | PROV-O activity id for regionalization.            |
| `RUN_GP`         | `grid_planning_2024-04-30`                                              | PROV-O activity id for the grid-planning run.      |
| `RUN_PUB`        | `publication_2024-04-30`                                               | PROV-O activity id for figure publication.         |
| `KEEP`           | `0`                                                                     | `1` = keep the repository after the run.           |
| `SKIP_FILE_CHECKS`| `0`                                                                    | `1` = skip the "does this path exist locally?" check. |
| `FIGURES`        | 7 PNG file names                                                        | Exact list of figures to register.                 |
| `SP_RAW` … `SP_FIG` | Storage subpath strings                                             | Where in the repository each file lands.           |

**Note 3:** The four `RUN_*` values are the most semantically meaningful: they group datasets produced by the *same activity*, which is how a later user can ask the hub *"show me everything produced by the 2024-04-30 grid-planning run"*.

---

## 7. Helper functions (usually do not need to be modified)

| Function                 | Job                                                                                              |
|--------------------------|--------------------------------------------------------------------------------------------------|
| `say(msg)`               | Print one line, flushed immediately, so logs are visible during long uploads.                    |
| `banner(title)`          | Print a 72-character section header, so each phase is visually distinct in the terminal.         |
| `_status_code(exc)`      | Pull the HTTP status code out of an exception, used to recognize the "already exists" case.      |
| `require_file(path)`     | Abort the run early with a clear message if a local source file is missing.                      |
| `require_dir(path)`      | Same as `require_file` but for folders.                                                           |
| `ingest(file_path, ...)` | The *single* canonical call to `cesdh.upload`. Wraps the boilerplate so every phase reads clean. |

#### Arguments of `ingest()`

| Argument         | Required | What it is                                                                |
|------------------|----------|---------------------------------------------------------------------------|
| `file_path`      | yes      | Absolute path to the local file to upload.                                |
| `subpath`        | yes      | Where the file lands inside the repository (e.g. `raw_input_data`).        |
| `description`    | yes      | Human-readable description; appears in search results.                    |
| `derived_from`   | no       | List of parent dataset IDs **this is the lineage link**.                |
| `run_id`         | no       | PROV-O activity id under which this upload was produced.                 |
| `keywords`       | no       | Extra search tags; the subpath and repo name are added automatically.     |
| `theme`          | no       | DCAT theme (defaults to the first segment of `subpath`).                  |
| `commit_message` | no       | LakeFS commit message; defaults to `ingest: <filename>`.                  |

In practice, every phase fills in three things: the file, the `subpath`, and the *lineage* (`derived_from` + `run_id`). Everything else is metadata that makes the file easier to find later.

---

## 8. Where this fits in the larger data-management picture

Think of the script as the **onboarding step** for a new planning cycle. The wider workflow looks like this:

```
   ┌────────────────────┐
   │ External providers │  (BFS, SOLKAT, internal scenarios)
   └─────────┬──────────┘
             │  raw files
             ▼
   ┌────────────────────┐
   │  THIS SCRIPT       │  uploads + records PROV-O lineage
   └─────────┬──────────┘
             │  semantic lakehouse
             ▼
   ┌────────────────────┐
   │  Other users       │  search, browse, download, audit
   └────────────────────┘
```

Concretely, this script gives the team three things they did not have before:

1. **A single source of truth.** Every file related to a planning cycle lives in one repository, one branch, one URL.
2. **A reproducible chain of custody.** A figure uploaded today is queryable all the way back to its raw input, even months later and even by someone who was not there.
3. **A repeatable bootstrap.** When the next planning cycle starts (e.g. `2025-04-30`), the same script is re-run with updated paths and run IDs. The new cycle lives alongside the old one without overwriting it.

That is the *provisioning* the script's name refers to: not the computation of the grid plan, but the act of putting every artifact of the plan into a governed, queryable, lineage-aware home.
