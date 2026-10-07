#!/usr/bin/env python3
"""
fen_users_daily_work.py
========================

Demonstrates daily collaborative work at FEN with the Energy Systems Data Hub.
Three users (FEN-A, FEN-B, FEN-C) work across two repositories and three
branches. The script exercises the full data lifecycle: file generation,
upload with rich DCAT metadata, cross-branch discovery, download with
integrity verification, derived-output lineage tracking, cross-repository
querying, and acceptance verification.

Users and topology
------------------
    FEN-A   uploads raw input data to   repository-a / main
    FEN-B   discovers FEN-A's data, downloads it, computes analytics,
            and uploads derived outputs to   repository-a / analysis
    FEN-C   uploads optimization models to   repository-b / main

Cross-repo querying proves that repository isolation holds (FEN-C's files
are invisible inside repository-a) while the platform-wide catalog endpoint
surfaces everything for legitimate cross-project discovery.

Run
---
    docker compose up -d          # or: make up
    python examples_cesdh/fen_users_daily_work.py

    # Environment overrides:
    FEN_GATEWAY_URL=http://localhost:8080 python examples_cesdh/fen_users_daily_work.py

    # Smaller HDF5 for quick test runs (~30 MB instead of ~2.5 GB):
    FEN_HDF5_BUSES=500 FEN_HDF5_HOURS=876 python examples_cesdh/fen_users_daily_work.py

Naming conventions
------------------
LakeFS rejects uppercase letters, underscores, and slashes in repository
and branch names. All identifiers here are lowercase-alphanumeric-with-hyphens:
    REPOSITORY_A  ->  repository-a
    REPOSITORY_B  ->  repository-b
    ANALYSIS      ->  analysis
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ── Dependency checks ────────────────────────────────────────────────────────

try:
    import numpy
except ImportError:
    raise SystemExit("numpy is required: pip install numpy")

try:
    import pandas as pd
except ImportError:
    raise SystemExit("pandas is required: pip install pandas")

try:
    import h5py
except ImportError:
    raise SystemExit("h5py is required: pip install h5py")

try:
    import openpyxl  # noqa: F401 — needed by pandas .to_excel()
except ImportError:
    raise SystemExit("openpyxl is required for .xlsx export: pip install openpyxl")

try:
    from cesdh import CESDHClient
except ImportError as exc:
    raise SystemExit(
        "Could not import the CESDH SDK.\n"
        "Install it in editable mode from the repository root:\n"
        "    pip install -e src/sdk\n"
        f"(original error: {exc})"
    ) from exc

import cesdh

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Concepts and definitions
# ═══════════════════════════════════════════════════════════════════════════════
#
# Asset categories that appear in everyday FEN workflows:
#
#   1a. Personal input data
#       Uploaded by one user, stored in their repo/branch. Hourly time series,
#       grid profiles, weather data. Not yet shared.
#
#   1b. Shared input data
#       Referenced cross-repo. The physical file lives in one repository; other
#       users discover it through the platform-wide catalog and download it.
#       Never duplicated — the catalog URI is the reference.
#
#   2a. Local analytical output
#       Written by an analyst in their own branch. Daily aggregates, congestion
#       summaries, feature extractions. Carries lineage back to the inputs.
#
#   2b. Shared/published output
#       Published for downstream consumers. Merged to main or tagged so the
#       reference is stable and citable.
#
#   3. Analytical model files
#       Code, scripts, GAMS formulations. Stored alongside data so the catalog
#       links the model to its inputs and outputs.


@dataclass
class DatasetRecord:
    """Tracks one uploaded dataset through the script for verification."""

    dataset_id: str
    title: str
    owner: str
    repository: str
    branch: str
    file_format: str
    domain_category: list[str]
    input_dependencies: list[str]
    description: str
    sha256: str


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Configuration
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Config:
    """Connection settings, user identities, and tuning knobs."""

    gateway_url: str = os.environ.get("FEN_GATEWAY_URL", "https://fen-esdh.ch")
    lakefs_url: str = os.environ.get("FEN_LAKEFS_URL", "https://lakefs.fen-esdh.ch")

    repo_a: str = "repository-a"
    repo_b: str = "repository-b"
    branch_main: str = "main"
    branch_analysis: str = "analysis"

    fen_a: str = "fen-user-a"
    fen_b: str = "fen-user-b"
    fen_c: str = "fen-user-c"

    workdir: Path = REPO_ROOT / "data" / "fen_daily_work"

    # Size of the large HDF5 in buses x hours. Default targets ~2.5 GB.
    # Override with FEN_HDF5_BUSES=500 FEN_HDF5_HOURS=876 for a ~30 MB test run.
    hdf5_buses: int = int(os.environ.get("FEN_HDF5_BUSES", "5000"))
    hdf5_hours: int = int(os.environ.get("FEN_HDF5_HOURS", "8760"))

    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def stamp(self) -> str:
        return self.started_at.strftime("%Y%m%dT%H%M%SZ")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Helpers
# ═══════════════════════════════════════════════════════════════════════════════


def say(message: str = "") -> None:
    """Single output channel: plain standard output, flushed so piping stays ordered."""
    print(message, flush=True)


def banner(title: str) -> None:
    say("")
    say("=" * 78)
    say(f"  {title}")
    say("=" * 78)


def sha256_file(path: Path) -> str:
    """Streaming SHA-256: reads in 1 MB chunks, never loads the whole file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1_048_576), b""):
            h.update(chunk)
    return h.hexdigest()


def _status_code(exc: Exception) -> int | None:
    """HTTP status behind an SDK error, or None if not HTTP-related."""
    return getattr(getattr(exc, "response", None), "status_code", None)


def human_bytes(n: float) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def cross_repo_datasets(gateway_url: str, limit: int = 200) -> list[dict]:
    """Call GET /catalog/datasets/all — the one endpoint not in the SDK.

    The SDK is single-repository by design (every call carries X-CESDH-Repository).
    The platform-wide catalog listing at /catalog/datasets/all is deliberately
    unscoped — it requires no repository header — so it cannot be expressed through
    the SDK's per-repo client. This is the sole place in this script that reaches
    the gateway directly.
    """
    resp = requests.get(f"{gateway_url}/catalog/datasets/all", params={"limit": limit})
    resp.raise_for_status()
    return resp.json()


def _upload_and_record(
    file_path: Path,
    owner: str,
    cfg_repo: str,
    branch: str,
    description: str,
    dcat_metadata: dict[str, Any],
    domain_category: list[str],
    derived_from: list[str] | None = None,
    allow_main: bool = False,
) -> DatasetRecord:
    """Upload a file and return a DatasetRecord. Reduces per-upload boilerplate."""
    file_sha = sha256_file(file_path)
    result = cesdh.upload_raw(
        str(file_path),
        owner=owner,
        repository=cfg_repo,
        branch=branch,
        description=description,
        force_tier="generic",
        force=True,
        allow_direct_main=allow_main,
        derived_from=derived_from,
        dcat_metadata=dcat_metadata,
    )
    did = result["dataset_id"]
    fmt = file_path.suffix.lstrip(".")
    say(f"  uploaded {file_path.name} -> {did}")
    return DatasetRecord(
        dataset_id=did,
        title=file_path.name,
        owner=owner,
        repository=cfg_repo,
        branch=branch,
        file_format=fmt,
        domain_category=domain_category,
        input_dependencies=derived_from or [],
        description=description,
        sha256=file_sha,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Repository provisioning (PHASE 1)
# ═══════════════════════════════════════════════════════════════════════════════


def provision(cfg: Config) -> None:
    """Create both repositories and the analysis branch in an identical way."""
    banner("PHASE 1 -- Repository and branch provisioning")

    for repo in [cfg.repo_a, cfg.repo_b]:
        try:
            cesdh.create_repository(repository=repo)
            say(f"  created  {repo}")
        except Exception as exc:
            if _status_code(exc) == 409:
                say(f"  exists   {repo}")
            else:
                raise

    # Create the analysis branch on repository-a (off main)
    try:
        cesdh.create_branch(cfg.branch_analysis, repository=cfg.repo_a, source="main")
        say(f"  branch   {cfg.branch_analysis} (off main) on {cfg.repo_a}")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  branch   {cfg.branch_analysis} already exists on {cfg.repo_a}")
        else:
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — PHASE A: User FEN-A (REPO A / MAIN)
# ═══════════════════════════════════════════════════════════════════════════════


def _generate_csv_timeseries(cfg: Config) -> Path:
    """Generate ~50 MB CSV: 8760 rows x ~600 float columns."""
    path = cfg.workdir / "fen_a_timeseries_2025.csv"
    rng = numpy.random.default_rng(seed=42)
    n_hours = 8760
    n_cols = 600
    timestamps = pd.date_range("2025-01-01", periods=n_hours, freq="h")
    data = rng.standard_normal((n_hours, n_cols)).astype(numpy.float32)
    df = pd.DataFrame(data, columns=[f"node_{i:04d}" for i in range(n_cols)])
    df.insert(0, "timestamp", timestamps)
    df.to_csv(path, index=False)
    say(f"  generated {path.name} ({human_bytes(path.stat().st_size)})")
    return path


def _generate_hdf5_profiles(cfg: Config, seed: int = 42) -> Path:
    """Generate large HDF5 with 3 datasets of shape (hours, buses)."""
    path = cfg.workdir / "fen_a_grid_profiles.h5"
    rng = numpy.random.default_rng(seed=seed)
    hours, buses = cfg.hdf5_hours, cfg.hdf5_buses
    chunk_rows = 1000
    with h5py.File(path, "w") as f:
        for name in ["p_injection", "q_injection", "vm_pu"]:
            ds = f.create_dataset(name, shape=(hours, buses), dtype="float64")
            for start in range(0, hours, chunk_rows):
                end = min(start + chunk_rows, hours)
                ds[start:end] = rng.standard_normal((end - start, buses))
    say(f"  generated {path.name} ({human_bytes(path.stat().st_size)}, seed={seed})")
    return path


def _generate_script(cfg: Config) -> Path:
    """Generate a small Python script as an analytical model file."""
    path = cfg.workdir / "fen_a_baseline_execution.py"
    path.write_text(
        '"""Baseline execution script for FEN-A grid analysis.\n\n'
        "This script loads the time series and grid profiles, runs a\n"
        "power-flow calculation, and writes summary statistics.\n"
        '"""\n\n'
        'print("FEN-A baseline execution complete.")\n',
        encoding="utf-8",
    )
    say(f"  generated {path.name} ({human_bytes(path.stat().st_size)})")
    return path


def _generate_metadata_index(cfg: Config) -> Path:
    """Generate a small CSV describing the other files."""
    path = cfg.workdir / "fen_a_metadata_index.csv"
    rows = [
        ["filename", "format", "description", "rows_or_shape"],
        ["fen_a_timeseries_2025.csv", "csv", "Hourly node time series", "8760x601"],
        [
            "fen_a_grid_profiles.h5",
            "h5",
            "Bus injection profiles",
            f"{cfg.hdf5_hours}x{cfg.hdf5_buses}x3",
        ],
        ["fen_a_baseline_execution.py", "py", "Baseline execution script", "n/a"],
        ["fen_a_metadata_index.csv", "csv", "This file", "4"],
    ]
    path.write_text("\n".join(",".join(row) for row in rows) + "\n", encoding="utf-8")
    say(f"  generated {path.name} ({human_bytes(path.stat().st_size)})")
    return path


def phase_a_fen_a(cfg: Config) -> list[DatasetRecord]:
    """FEN-A uploads 5 files to repository-a / main."""
    banner("PHASE A -- FEN-A uploads raw input data (repository-a / main)")
    records: list[DatasetRecord] = []
    cfg.workdir.mkdir(parents=True, exist_ok=True)
    up = lambda p, desc, dcat, cat: _upload_and_record(
        p,
        cfg.fen_a,
        cfg.repo_a,
        cfg.branch_main,
        desc,
        dcat,
        cat,
        allow_main=True,
    )

    # 1. Large CSV time series
    csv_path = _generate_csv_timeseries(cfg)
    records.append(
        up(
            csv_path,
            "Hourly time series for 2025, 600 nodes",
            {
                "keywords": ["timeseries", "grid", "2025"],
                "theme": "GridAnalysis",
                "temporal_resolution": "PT1H",
                "temporal_start": "2025-01-01",
                "temporal_end": "2025-12-31",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["1a-personal-input"],
        )
    )

    # 2. Large HDF5 grid profiles (first upload, seed=42)
    h5_path = _generate_hdf5_profiles(cfg, seed=42)
    records.append(
        up(
            h5_path,
            "Bus injection profiles, 3 datasets (p, q, vm)",
            {
                "keywords": ["grid-profiles", "hdf5", "injection"],
                "theme": "GridAnalysis",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["1a-personal-input"],
        )
    )
    h5_id_v1 = records[-1].dataset_id

    # 3. Small Python script
    py_path = _generate_script(cfg)
    records.append(
        up(
            py_path,
            "Baseline execution script for grid analysis",
            {
                "keywords": ["script", "baseline", "execution"],
                "theme": "GridAnalysis",
                "publisher": "FEN-ETH",
            },
            ["3-model-file"],
        )
    )

    # 4. Metadata index CSV
    meta_path = _generate_metadata_index(cfg)
    records.append(
        up(
            meta_path,
            "Metadata index describing the uploaded file set",
            {
                "keywords": ["metadata", "index", "manifest"],
                "theme": "Documentation",
                "publisher": "FEN-ETH",
            },
            ["1a-personal-input"],
        )
    )

    # 5. Re-upload HDF5 with different data (seed=43) to demonstrate versioning
    say("")
    say("  Re-uploading HDF5 with updated data (seed 43) to demonstrate versioning...")
    h5_path_v2 = _generate_hdf5_profiles(cfg, seed=43)
    records.append(
        up(
            h5_path_v2,
            "Bus injection profiles v2 (updated seed)",
            {
                "keywords": ["grid-profiles", "hdf5", "injection", "v2"],
                "theme": "GridAnalysis",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["1a-personal-input"],
        )
    )
    h5_id_v2 = records[-1].dataset_id

    # Verify version tracking: the two uploads must produce different dataset IDs
    assert h5_id_v1 != h5_id_v2, (
        f"Version tracking failure: v1 ({h5_id_v1}) == v2 ({h5_id_v2})"
    )
    say(f"  version tracking OK: v1={h5_id_v1} != v2={h5_id_v2}")
    say(f"  FEN-A: {len(records)} datasets uploaded to {cfg.repo_a}/{cfg.branch_main}")
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — PHASE B: User FEN-B (REPO A / analysis)
# ═══════════════════════════════════════════════════════════════════════════════


def phase_b_fen_b(
    cfg: Config, fen_a_records: list[DatasetRecord]
) -> list[DatasetRecord]:
    """FEN-B discovers FEN-A's data, downloads, computes analytics, uploads derived files."""
    banner(
        "PHASE B -- FEN-B discovers, downloads, and derives (repository-a / analysis)"
    )
    records: list[DatasetRecord] = []

    # ── 1. Discover FEN-A's data on main ─────────────────────────────────────
    say("  Discovering FEN-A's uploads on main...")
    main_datasets = cesdh.list_datasets(repository=cfg.repo_a, branch=cfg.branch_main)
    say(f"  {'TITLE':<42s} {'FORMAT':<8s} {'OWNER':<14s} DATASET_ID")
    say(f"  {'-' * 42} {'-' * 8} {'-' * 14} {'-' * 20}")
    for d in main_datasets:
        say(
            f"  {str(d.get('title', '?'))[:42]:<42s} "
            f"{d.get('format', '?')!s:<8s} "
            f"{d.get('owner', '?')!s:<14s} "
            f"{d.get('dataset_id', '?')}"
        )

    # Identify the CSV and latest HDF5 from FEN-A's records
    fen_a_csv = next(
        r for r in fen_a_records if r.file_format == "csv" and "timeseries" in r.title
    )
    fen_a_h5 = [r for r in fen_a_records if r.file_format == "h5"][-1]  # latest version

    # ── 2. Download + verify ─────────────────────────────────────────────────
    say("")
    say("  Downloading FEN-A's CSV and HDF5 for analysis...")
    dl_dir = cfg.workdir / "fen_b_downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)

    csv_dl_path = Path(
        cesdh.download_to_file(
            fen_a_csv.dataset_id,
            str(dl_dir / "fen_a_timeseries_2025.csv"),
            repository=cfg.repo_a,
        )
    )
    csv_dl_sha = sha256_file(csv_dl_path)
    csv_match = csv_dl_sha == fen_a_csv.sha256
    say(f"    CSV sha256 {'OK' if csv_match else 'MISMATCH'}: {csv_dl_sha[:16]}...")

    h5_dl_path = Path(
        cesdh.download_to_file(
            fen_a_h5.dataset_id,
            str(dl_dir / "fen_a_grid_profiles.h5"),
            repository=cfg.repo_a,
        )
    )
    h5_dl_sha = sha256_file(h5_dl_path)
    h5_match = h5_dl_sha == fen_a_h5.sha256
    say(f"    HDF5 sha256 {'OK' if h5_match else 'MISMATCH'}: {h5_dl_sha[:16]}...")

    # ── 3. Compute analytics ─────────────────────────────────────────────────
    say("")
    say("  Computing daily aggregates from CSV...")
    df_csv = pd.read_csv(csv_dl_path, parse_dates=["timestamp"], index_col="timestamp")
    daily_agg = df_csv.resample("D").agg(["mean", "max", "min"])
    # Flatten multi-level columns for export
    daily_agg.columns = [f"{col}_{stat}" for col, stat in daily_agg.columns]
    daily_agg = daily_agg.reset_index()
    say(
        f"    daily aggregates: {len(daily_agg)} days, {len(daily_agg.columns)} columns"
    )

    say("  Computing congestion summary from HDF5...")
    with h5py.File(h5_dl_path, "r") as f:
        p_inj = f["p_injection"][:]
    congestion_stats = {
        "metric": ["mean", "std", "p5", "p25", "p50", "p75", "p95"],
        "value": [
            float(numpy.mean(p_inj)),
            float(numpy.std(p_inj)),
            float(numpy.percentile(p_inj, 5)),
            float(numpy.percentile(p_inj, 25)),
            float(numpy.percentile(p_inj, 50)),
            float(numpy.percentile(p_inj, 75)),
            float(numpy.percentile(p_inj, 95)),
        ],
    }
    congestion_df = pd.DataFrame(congestion_stats)
    say(f"    congestion summary: {len(congestion_df)} metrics")

    # Hourly feature extraction: per-hour mean and std across all buses
    say("  Computing hourly features from HDF5...")
    hourly_features = pd.DataFrame(
        {
            "hour": range(p_inj.shape[0]),
            "p_mean": numpy.mean(p_inj, axis=1),
            "p_std": numpy.std(p_inj, axis=1),
            "p_min": numpy.min(p_inj, axis=1),
            "p_max": numpy.max(p_inj, axis=1),
        }
    )
    say(
        f"    hourly features: {len(hourly_features)} rows, {len(hourly_features.columns)} columns"
    )

    # ── 4. Upload 5 derived files to analysis ─────────────────────────────────
    say("")
    say("  Uploading derived outputs to analysis branch...")
    deps = [fen_a_csv.dataset_id, fen_a_h5.dataset_id]
    output_dir = cfg.workdir / "fen_b_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    up = lambda p, desc, dcat, cat: _upload_and_record(
        p,
        cfg.fen_b,
        cfg.repo_a,
        cfg.branch_analysis,
        desc,
        dcat,
        cat,
        derived_from=deps,
    )

    # 4a. Daily aggregates as Excel
    xlsx_path = output_dir / "fen_b_daily_aggregates.xlsx"
    daily_agg.to_excel(xlsx_path, index=False, engine="openpyxl")
    records.append(
        up(
            xlsx_path,
            "Daily mean/max/min aggregates from time series",
            {
                "keywords": ["derived", "analytics", "daily-aggregates", "excel"],
                "theme": "Analytics",
                "temporal_resolution": "P1D",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["2a-local-output"],
        )
    )

    # 4b. Daily aggregates as Parquet
    pq_agg_path = output_dir / "fen_b_daily_aggregates.parquet"
    daily_agg.to_parquet(pq_agg_path, index=False)
    records.append(
        up(
            pq_agg_path,
            "Daily mean/max/min aggregates (Parquet)",
            {
                "keywords": ["derived", "analytics", "daily-aggregates", "parquet"],
                "theme": "Analytics",
                "temporal_resolution": "P1D",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["2a-local-output"],
        )
    )

    # 4c. Congestion summary as Excel
    cong_path = output_dir / "fen_b_congestion_summary.xlsx"
    congestion_df.to_excel(cong_path, index=False, engine="openpyxl")
    records.append(
        up(
            cong_path,
            "Congestion summary from p_injection profiles",
            {
                "keywords": ["derived", "analytics", "congestion", "summary"],
                "theme": "Analytics",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["2a-local-output"],
        )
    )

    # 4d. Hourly features as Parquet
    hourly_path = output_dir / "fen_b_hourly_features.parquet"
    hourly_features.to_parquet(hourly_path, index=False)
    records.append(
        up(
            hourly_path,
            "Hourly feature extraction from p_injection",
            {
                "keywords": ["derived", "analytics", "features", "hourly"],
                "theme": "Analytics",
                "temporal_resolution": "PT1H",
                "spatial_coverage": "CH",
                "publisher": "FEN-ETH",
            },
            ["2a-local-output"],
        )
    )

    # 4e. Post-processing script
    pp_path = output_dir / "fen_b_post_processing.py"
    pp_path.write_text(
        '"""FEN-B analytics pipeline: daily aggregates, congestion stats, features."""\n'
        'print("FEN-B post-processing pipeline complete.")\n',
        encoding="utf-8",
    )
    records.append(
        up(
            pp_path,
            "Post-processing script documenting FEN-B pipeline",
            {
                "keywords": ["script", "post-processing", "documentation"],
                "theme": "Analytics",
                "publisher": "FEN-ETH",
            },
            ["3-model-file"],
        )
    )

    say(
        f"  FEN-B: {len(records)} derived datasets uploaded to {cfg.repo_a}/{cfg.branch_analysis}"
    )
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — PHASE C: User FEN-C (REPO B / MAIN)
# ═══════════════════════════════════════════════════════════════════════════════


def phase_c_fen_c(cfg: Config) -> list[DatasetRecord]:
    """FEN-C uploads 4 optimization model files to repository-b / main."""
    banner("PHASE C -- FEN-C uploads optimization models (repository-b / main)")
    records: list[DatasetRecord] = []
    output_dir = cfg.workdir / "fen_c_models"
    output_dir.mkdir(parents=True, exist_ok=True)
    dcat: dict[str, Any] = {
        "keywords": ["Optimization", "PowerFlow", "GAMS"],
        "theme": "Optimization",
        "publisher": "FEN-ETH",
    }
    up = lambda p, desc, d=dcat: _upload_and_record(
        p,
        cfg.fen_c,
        cfg.repo_b,
        cfg.branch_main,
        desc,
        d,
        ["3-model-file"],
        allow_main=True,
    )

    # 1. GAMS-like optimization model
    gams_path = output_dir / "fen_c_optimization_model.gms"
    gams_path.write_text(
        "$title FEN-C Optimal Power Flow Model\n"
        "Sets  t /t1*t8760/  n /bus1*bus100/  g /gen1*gen20/  l /line1*line50/;\n"
        "Parameters  demand(t,n), pmax(g), cost(g), ptdf(l,n), fmax(l);\n"
        "Variables   z, p(t,g), theta(t,n);\n"
        "Equations   objective, balance(t,n), lineflow(t,l);\n"
        "objective..  z =e= sum((t,g), cost(g)*p(t,g));\n"
        "balance(t,n)..  sum(g$mapgn(g,n), p(t,g)) =e= demand(t,n);\n"
        "lineflow(t,l)..  sum(n, ptdf(l,n)*theta(t,n)) =l= fmax(l);\n"
        "Model opf /all/; Solve opf using lp minimizing z;\n",
        encoding="utf-8",
    )
    records.append(up(gams_path, "DC-OPF GAMS formulation with network constraints"))

    # 2. Generator set definition
    gen_path = output_dir / "fen_c_set_generators.txt"
    gen_specs = [
        "Nuclear_Goesgen:1010",
        "Nuclear_Leibstadt:990",
        "Nuclear_Beznau_1:365",
        "Nuclear_Beznau_2:365",
        "Hydro_Grande_Dixence:200",
        "Hydro_Mauvoisin:150",
        "Hydro_Emosson:180",
        "Wind_Jura_North:50",
        "Wind_Jura_South:45",
        "Solar_Plateau:120",
        "Solar_Valais:200",
        "Gas_Chavalon:300",
        "CCGT_Reserve_1:400",
        "CCGT_Reserve_2:400",
        "Pumped_Linth_Limmern:480",
        "Pumped_Nant_de_Drance:900",
        "Hydro_Oberhasli:300",
        "Hydro_KW_Bern:120",
        "Wind_Gotthard:37",
        "Biomass_various:80",
    ]
    gen_path.write_text(
        "/ Generator set for FEN-C OPF model /\n"
        + "\n".join(
            f"gen{i + 1:<3d} {s.split(':')[0]:<26s} {s.split(':')[1]:>4s} MW"
            for i, s in enumerate(gen_specs)
        )
        + "\n",
        encoding="utf-8",
    )
    records.append(up(gen_path, "Generator set listing with capacities"))

    # 3. Set membership definitions
    mem_path = output_dir / "fen_c_set_membership.txt"
    mem_path.write_text(
        "/ Generator-to-bus mapping /\n"
        "gen1.bus12 gen2.bus15 gen3.bus8 gen4.bus8 gen5.bus45 gen6.bus47\n"
        "gen7.bus48 gen8.bus22 gen9.bus23 gen10.bus30 gen11.bus55 gen12.bus60\n"
        "gen13.bus62 gen14.bus63 gen15.bus70 gen16.bus72 gen17.bus40 gen18.bus35\n"
        "gen19.bus50 gen20.bus28\n"
        "/ Fuel-type sets /\n"
        "nuclear: gen1-gen4 | hydro: gen5-gen7,gen15-gen18 | wind: gen8,gen9,gen19\n"
        "solar: gen10,gen11 | gas: gen12-gen14 | biomass: gen20\n",
        encoding="utf-8",
    )
    records.append(up(mem_path, "Generator-to-bus mapping and fuel-type sets"))

    # 4. Scenario definitions CSV
    sc_path = output_dir / "fen_c_scenario_definitions.csv"
    sc_path.write_text(
        "scenario_name,co2_price_eur_t,gas_price_eur_mwh,demand_growth_pct,wind_mw,solar_mw\n"
        "base_2025,50,35,0.0,95,320\n"
        "high_co2_2030,120,40,1.5,200,600\n"
        "low_gas_2030,80,20,1.0,150,500\n"
        "high_renewables_2035,100,30,2.0,500,1200\n"
        "stress_test_2030,150,60,0.5,95,320\n",
        encoding="utf-8",
    )
    records.append(
        up(
            sc_path,
            "Scenario parameter definitions for OPF studies",
            {
                **dcat,
                "keywords": ["Optimization", "PowerFlow", "GAMS", "scenarios"],
            },
        )
    )

    say(f"  FEN-C: {len(records)} datasets uploaded to {cfg.repo_b}/{cfg.branch_main}")
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — Cross-repository querying
# ═══════════════════════════════════════════════════════════════════════════════


def _print_table(
    rows: list[dict], columns: list[str], widths: list[int], title: str
) -> None:
    """Print a formatted table of dicts."""
    say(f"\n  {title}")
    header = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    say(f"  {header}")
    say(f"  {'  '.join('-' * w for w in widths)}")
    for row in rows:
        line = "  ".join(
            str(row.get(c, "?"))[:w].ljust(w) for c, w in zip(columns, widths)
        )
        say(f"  {line}")
    if not rows:
        say("  (no results)")


def cross_repo_queries(
    cfg: Config,
    fen_a_records: list[DatasetRecord],
    fen_b_records: list[DatasetRecord],
    fen_c_records: list[DatasetRecord],
) -> None:
    """FEN-B performs same-repo, cross-repo, and filtered queries."""
    banner("PHASE D -- Cross-repository querying")

    # 1. Same-repo, different branch: see FEN-A's files from FEN-B's perspective
    say("  Query 1: Same-repo, FEN-A's main branch (from FEN-B's perspective)")
    main_ds = cesdh.list_datasets(repository=cfg.repo_a, branch=cfg.branch_main)
    _print_table(
        main_ds,
        ["dataset_id", "title", "format", "owner"],
        [24, 35, 8, 14],
        f"repository-a / main ({len(main_ds)} datasets)",
    )

    # 2. Cross-repo: all datasets from both repos
    say("")
    say("  Query 2: Platform-wide catalog (cross-repo)")
    all_datasets = cross_repo_datasets(cfg.gateway_url)
    _print_table(
        all_datasets,
        ["dataset_id", "title", "repository", "owner"],
        [24, 30, 16, 14],
        f"All repositories ({len(all_datasets)} datasets total)",
    )

    # Filter for repository-b entries
    repo_b_datasets = [d for d in all_datasets if d.get("repository") == cfg.repo_b]
    say(f"\n  Filtered to {cfg.repo_b}: {len(repo_b_datasets)} dataset(s)")
    for d in repo_b_datasets:
        say(
            f"    - {d.get('title', '?')} [{d.get('format', '?')}] by {d.get('owner', '?')}"
        )

    # 3. Filter by keyword
    say("")
    say("  Query 3: Filter by keyword 'Optimization'")
    optimization_hits = [
        d
        for d in all_datasets
        if "Optimization" in str(d.get("keywords", ""))
        or "Optimization" in str(d.get("description", ""))
        or "Optimization" in str(d.get("theme", ""))
    ]
    say(f"    {len(optimization_hits)} dataset(s) match 'Optimization':")
    for d in optimization_hits:
        say(f"      - {d.get('title', '?')} (repo: {d.get('repository', '?')})")

    # 4. Filter by format
    say("")
    say("  Query 4: Filter by format")
    format_groups: dict[str, int] = {}
    for d in all_datasets:
        fmt = str(d.get("format", "unknown"))
        format_groups[fmt] = format_groups.get(fmt, 0) + 1
    for fmt, count in sorted(format_groups.items()):
        say(f"    format={fmt:<10s} {count} dataset(s)")

    # 5. Filter by owner
    say("")
    say("  Query 5: Filter by owner = fen-user-c")
    fen_c_hits = [d for d in all_datasets if d.get("owner") == cfg.fen_c]
    say(f"    {len(fen_c_hits)} dataset(s) owned by {cfg.fen_c}:")
    for d in fen_c_hits:
        say(f"      - {d.get('title', '?')} (repo: {d.get('repository', '?')})")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — Acceptance verification
# ═══════════════════════════════════════════════════════════════════════════════


def verify_acceptance(
    cfg: Config,
    fen_a_records: list[DatasetRecord],
    fen_b_records: list[DatasetRecord],
    fen_c_records: list[DatasetRecord],
) -> bool:
    """Run acceptance criteria and report pass/fail."""
    banner("ACCEPTANCE VERIFICATION")
    results: list[tuple[str, bool, str]] = []

    # 1. Multi-repo isolation: FEN-C's datasets must NOT appear in repository-a
    say("  checking multi-repo isolation...")
    repo_a_datasets = cesdh.list_datasets(repository=cfg.repo_a, limit=200)
    repo_a_ids = {d.get("dataset_id") for d in repo_a_datasets}
    fen_c_ids = {r.dataset_id for r in fen_c_records}
    leaks = repo_a_ids & fen_c_ids
    results.append(
        (
            "Multi-repo isolation",
            len(leaks) == 0,
            f"{len(leaks)} cross-repo leaks",
        )
    )

    # 2. Branch collaboration: FEN-B's derived_from references must point to FEN-A's IDs
    say("  checking branch collaboration lineage...")
    fen_a_source_ids = {r.dataset_id for r in fen_a_records}
    fen_b_deps: set[str] = set()
    for r in fen_b_records:
        fen_b_deps.update(r.input_dependencies)
    dep_match = fen_b_deps.issubset(fen_a_source_ids)
    results.append(
        (
            "Branch collaboration",
            dep_match,
            f"{len(fen_b_deps)} input deps, {len(fen_b_deps & fen_a_source_ids)} match",
        )
    )

    # 3. Metadata search accuracy
    say("  checking metadata search accuracy...")
    all_datasets = cross_repo_datasets(cfg.gateway_url)
    optimization_hits = [
        d
        for d in all_datasets
        if "Optimization" in str(d.get("keywords", ""))
        or "Optimization" in str(d.get("description", ""))
        or "Optimization" in str(d.get("theme", ""))
    ]
    hit_ids = {d.get("dataset_id") for d in optimization_hits}
    has_gams = any(r.dataset_id in hit_ids for r in fen_c_records)
    results.append(
        (
            "Metadata search accuracy",
            has_gams,
            f"{len(optimization_hits)} match(es) for 'Optimization'",
        )
    )

    # 4. Large file transfer: re-download the latest HDF5 and verify checksum
    say("  checking large file transfer integrity...")
    h5_record = [r for r in fen_a_records if r.file_format == "h5"][-1]
    tmp = Path(tempfile.mkdtemp())
    try:
        dl_path = cesdh.download_to_file(
            h5_record.dataset_id,
            str(tmp / "verify_h5.h5"),
            repository=cfg.repo_a,
        )
        dl_sha = sha256_file(Path(dl_path))
        sha_ok = dl_sha == h5_record.sha256
        results.append(
            (
                "Large file transfer",
                sha_ok,
                f"sha256 {'verified' if sha_ok else 'MISMATCH'}",
            )
        )
    except Exception as exc:
        logger.exception("Large file download failed during verification")
        results.append(("Large file transfer", False, f"download failed: {exc}"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    # Print summary
    say("")
    say(f"  {'ACCEPTANCE CRITERIA':<30s} {'STATUS':<10s} DETAIL")
    say(f"  {'-' * 30} {'-' * 10} {'-' * 38}")
    all_pass = True
    for name, passed, detail in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        say(f"  {name:<30s} {status:<10s} {detail}")
    say("")
    return all_pass


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("FEN_LOG_LEVEL", "WARNING"),
        format="%(levelname)s %(name)s: %(message)s",
    )
    cfg = Config()
    cfg.workdir.mkdir(parents=True, exist_ok=True)

    banner("FEN Users Daily Work")
    say(f"  gateway:   {cfg.gateway_url}")
    say(f"  lakefs:    {cfg.lakefs_url}")
    say(f"  repo A:    {cfg.repo_a}")
    say(f"  repo B:    {cfg.repo_b}")
    say(f"  HDF5 dim:  {cfg.hdf5_hours} hours x {cfg.hdf5_buses} buses")
    say(f"  timestamp: {cfg.stamp}")

    t_total = time.perf_counter()

    provision(cfg)
    fen_a_records = phase_a_fen_a(cfg)
    fen_b_records = phase_b_fen_b(cfg, fen_a_records)
    fen_c_records = phase_c_fen_c(cfg)
    cross_repo_queries(cfg, fen_a_records, fen_b_records, fen_c_records)
    ok = verify_acceptance(cfg, fen_a_records, fen_b_records, fen_c_records)

    elapsed = time.perf_counter() - t_total

    banner("DONE")
    say(
        f"  total datasets: {len(fen_a_records) + len(fen_b_records) + len(fen_c_records)}"
    )
    say(f"    FEN-A: {len(fen_a_records)} ({cfg.repo_a}/{cfg.branch_main})")
    say(f"    FEN-B: {len(fen_b_records)} ({cfg.repo_a}/{cfg.branch_analysis})")
    say(f"    FEN-C: {len(fen_c_records)} ({cfg.repo_b}/{cfg.branch_main})")
    say(f"  elapsed: {elapsed:.1f}s")
    say(f"  result:  {'ALL CHECKS PASSED' if ok else 'SOME CHECKS FAILED'}")
    say("")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
