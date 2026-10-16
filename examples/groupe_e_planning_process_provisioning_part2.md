# Groupe-E Planning Process — Part 2: Modify & Audit

A walkthrough of the script `groupe_e_planning_process_provisioning_part2.py`.

This guide picks up where Part 1 left off. Part 1 provisioned a repository with five folders of planning data and a complete PROV-O lineage chain. Part 2 answers the question that comes next: **what happens when an assumption changes?**

The scenario is concrete. A second researcher — Marie — suspects the 2024 cost estimates are too low. She wants to explore the impact of a 15 % increase without disturbing the published baseline. She branches, uploads a revised costs dataset with lineage back to the original, and then proves — in three queries — that both the baseline and the revision are fully auditable, side by side, with clear attribution.

---
## * Prerequisites

1. The **ESDH SDK** must be installed. Follow the [Trial onboarding](https://docs.fen-esdh.ch/trial/onboarding/) guide.
2. **Part 1 must have been run with `KEEP=1`** so that the repository `groupe-e-planning-process` still exists on the `main` branch with its full lineage intact:

```bash
KEEP=1 python examples_cesdh/groupe_e_planning_process_provisioning.py
```

If the repository is missing, Part 2 will abort with a clear error — it does not attempt to recreate it.

---

## 1. Purpose of the Script

Part 1 built the **create** half of the data lifecycle: uploading raw inputs, cleaned data, intermediate results, and publication figures, all linked with PROV-O provenance.

Part 2 builds the **modify + audit** half. In three phases, the script:

1. verifies that Part 1's repository exists, sets a new identity, and creates a branch,
2. downloads a dataset, applies a sensitivity adjustment, and uploads the revision with lineage, and
3. audits the result: compares branches, traces lineage, and shows who changed what.

Once this script completes, the evaluator can confirm that:

- the published baseline on `main` is untouched,
- the revision on `sensitivity-2025` is traceable all the way back to the raw BFS / SOLKAT inputs,
- two different researchers' contributions are clearly attributed in the provenance record.

---

## 2. How to run it

```bash
# Default: connect to the hub, revise costs by ×1.15
python examples_cesdh/groupe_e_planning_process_provisioning_part2.py

# Override the sensitivity factor
PART2_SCALE_FACTOR=1.25 python examples_cesdh/groupe_e_planning_process_provisioning_part2.py

# Target a different dataset for revision
PART2_TARGET_QUERY="expansion_results" python examples_cesdh/groupe_e_planning_process_provisioning_part2.py

# Point at a different hub
CESDH_DATA_HUB_ENDPOINT=https://fen-esdh.ch \
    python examples_cesdh/groupe_e_planning_process_provisioning_part2.py
```

---

## 3. The script at a glance

| Layer             | What it does                                             | You change it?          |
|-------------------|----------------------------------------------------------|-------------------------|
| **Configuration** | Branch name, identity, scale factor, target query.       | Often, via env vars.    |
| **Helpers**       | `say`, `banner`, `_status_code`, `_apply_sensitivity`.   | Rarely.                 |
| **Main flow**     | The three pipeline phases (7–9) plus a summary.          | Only to add a new step. |

---

## 4. Main functionalities (the pipeline phases)

### 4.1 `phase_7_verify_and_branch()`. Verify Part 1, create the sensitivity branch

**Purpose.** Confirms that the Part 1 repository exists and has data on `main`. Sets the Part 2 identity (`marie` / `Marie Dupont`), discovers the target dataset via search, and creates the `sensitivity-2025` branch.

**Returns.** The dataset ID of the costs dataset on `main` (the revision target).

**Why it matters.** This phase is the safety gate: if Part 1 was not run with `KEEP=1`, the script stops here with a clear message instead of producing confusing results on an empty repository. The branch creation is the key operation — it gives Marie a safe workspace where the baseline cannot be overwritten.

---

### 4.2 `phase_8_revise()`. Download, modify, and upload with lineage

**Purpose.** Downloads the costs dataset to a local working directory, applies the sensitivity multiplier (×1.15 by default) to all numeric columns, and uploads the revised file to the `sensitivity-2025` branch. The upload includes:

- `derived_from=[original_dataset_id]` — the PROV-O link back to the baseline,
- `run_id="sensitivity_rerun_2025"` — the activity identifier for this analysis run.

**Returns.** The revised dataset ID.

**Why it matters.** This is the same `upload()` call that Part 1 used, with two differences: it targets a branch instead of `main`, and it records a `derived_from` link to the original. The hub handles the rest — the revision becomes a first-class node in the lineage graph, queryable just like any other dataset.

---

### 4.3 `phase_9_audit()`. Compare the baseline and the revision

**Purpose.** Demonstrates four audit queries that the hub can now answer across branches:

1. **"What changed on the sensitivity branch?"** — `cesdh.get_branch_changes("sensitivity-2025", ...)` shows the revised costs dataset as the sole divergence from `main`.

2. **"Where did the revision come from?"** — `cesdh.get_lineage(revised_id, direction="ancestors")` walks the chain from the revised costs, through grid planning, through the regionalized pckls, all the way back to the raw BFS / SOLKAT inputs.

3. **"Who touched this file, on which branch?"** — `cesdh.get_file_branches_history(file_path, ...)` shows the same logical file on `main` (uploaded by `yaman`) and on `sensitivity-2025` (uploaded by `marie`), with different content hashes.

4. **"How do the two pipelines compare?"** — `cesdh.get_pipeline(...)` on each branch shows the sensitivity branch has one additional derivation edge: the lineage extended rather than breaking.

**Why it matters.** This phase is the payoff. Every question an evaluator would ask about revision safety, provenance, and multi-user attribution is answered by the hub's existing query surface — no custom code, no manual record-keeping.

---

### 4.4 `main()`. The orchestrator

**Purpose.** Prints a banner, calls each phase in order, and prints a summary:

```
  branch 'main'             : baseline (untouched)
  branch 'sensitivity-2025' : +1 revised dataset
  original dataset           : <id>
  revised dataset            : <id>
  derived_from               : original → revision (PROV-O)
  owner                      : marie
```

It also prints cleanup commands for the branch and the full repository.

---

## 5. The `cesdh` SDK calls behind each phase

| SDK call                                                  | What it does                                               | New in Part 2? |
|-----------------------------------------------------------|------------------------------------------------------------|----------------|
| `cesdh.set_user(owner, display)`                          | Sets the identity for provenance records.                  | No             |
| `cesdh.list_branches(repository)`                         | Lists all branches and their dataset counts.               | **Yes**        |
| `cesdh.search(query, repository, limit)`                  | Full-text search across datasets.                          | No             |
| `cesdh.create_branch(branch, repository, source)`         | Creates a new branch from a source branch.                 | **Yes**        |
| `cesdh.download_to_file(dataset_id, destination, repository)` | Downloads a dataset to a local file.                   | **Yes**        |
| `cesdh.upload(...)`                                       | Uploads a file with metadata and lineage.                  | No             |
| `cesdh.get_branch_changes(branch, repository)`            | Shows datasets that differ from main.                      | **Yes**        |
| `cesdh.get_lineage(dataset_id, repository, direction)`    | Walks the ancestor/descendant graph.                       | No             |
| `cesdh.get_file_branches_history(file_path, repository)`  | Traces a file across all branches with content hashes.     | **Yes**        |
| `cesdh.get_pipeline(repository, branch)`                  | Returns every derivation edge on a branch.                 | No             |

The single most important argument remains `derived_from=[...]`: it is what connects the revision to the original. Without it, the revision would be an orphan with no traceable provenance.

---

## 6. Configuration

| Variable              | Default                         | Meaning                                       |
|-----------------------|---------------------------------|-----------------------------------------------|
| `REPO`                | `groupe-e-planning-process`     | Repository name (must match Part 1).          |
| `PART2_BRANCH`        | `sensitivity-2025`              | Branch for the what-if scenario.              |
| `PART2_OWNER`         | `marie`                         | Identity used for Part 2 uploads.             |
| `PART2_DISPLAY`       | `Marie Dupont`                  | Human-readable display name.                  |
| `PART2_RUN_ID`        | `sensitivity_rerun_2025`        | PROV-O activity ID for the revision.          |
| `PART2_WORK_DIR`      | `./part2_workdir`               | Local directory for downloaded/modified files. |
| `PART2_SCALE_FACTOR`  | `1.15`                          | Multiplicative cost adjustment.               |
| `PART2_TARGET_QUERY`  | `expansion_costs`               | Search query to find the dataset to revise.   |
| `SKIP_FILE_CHECKS`    | `0`                             | Inherited from Part 1 (unused in Part 2).     |

Part 2 does not offer a `KEEP` variable because it never tears down the repository — it did not create it.

---

## 7. Helper functions

| Function                        | Job                                                                   |
|---------------------------------|-----------------------------------------------------------------------|
| `say(msg)`                      | Print one line, flushed. Reused from Part 1.                          |
| `banner(title)`                 | Print a 72-character section header. Reused from Part 1.              |
| `_status_code(exc)`             | Extract HTTP status from an exception. Reused from Part 1.            |
| `_apply_sensitivity(path, scale)` | Multiply numeric columns by a scale factor. Supports xlsx/csv/json. |

The `ingest()` helper from Part 1 is **not** reused — Part 2 calls `cesdh.upload()` directly because there is only one upload and the boilerplate saved by a wrapper is not worth the indirection.

---

## 8. Where this fits in the larger data-management picture

```
   ┌──────────────────────────────────┐
   │  Part 1: THIS SCRIPT             │  uploads + records PROV-O lineage
   │  (create the baseline)           │
   └────────────────┬─────────────────┘
                    │
                    ▼
   ┌──────────────────────────────────┐
   │  Part 2: THIS SCRIPT             │  branch + revise + audit
   │  (explore a what-if)             │
   └────────────────┬─────────────────┘
                    │
                    ▼
   ┌──────────────────────────────────┐
   │  Other users                     │  search, browse, compare branches
   └──────────────────────────────────┘
```

Together, the two scripts give the team three things:

1. **Safe what-if exploration.** A researcher can branch off the published baseline, revise assumptions, and upload the results — all without touching `main`. If the sensitivity analysis is rejected, the branch can be deleted with no trace on the baseline.

2. **Automatic provenance across revisions.** The revised costs dataset is linked to the original via `derived_from`. The hub records this as a PROV-O `prov:wasDerivedFrom` edge, so any future user can walk the chain from the revision all the way back to the raw inputs — across branches, across researchers.

3. **Multi-user attribution.** Part 1 was uploaded by `yaman`; Part 2 by `marie`. The hub's provenance records show both contributors without manual bookkeeping. The `get_file_branches_history` query makes this visible in a single table: same file, two branches, two owners, two content hashes.

That is the *modify & audit* cycle the script's name refers to: not the computation itself, but the act of safely exploring a revision and proving that the audit trail remained intact.
