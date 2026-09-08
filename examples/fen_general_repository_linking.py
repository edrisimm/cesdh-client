#!/usr/bin/env python3
"""
fen_general_repository_linking.py — zero-copy linking to a shared repository
============================================================================

Two research projects need the same open time series. The obvious move is to
copy the CSVs into each project. This example shows why you should not, and
what the Data Hub offers instead.

A shared **FEN General Repository** holds the raw open data once. Two consumer
projects — **Repository X** (solar study) and **Repository Y** (grid
integration study) — discover those datasets over DCAT, define *virtual
slices* of them, and store nothing but a small `dataset_link_manifest.json`
pinning each slice to a `dataset_id` and a content hash. The bytes never move.
At analysis time a pointer resolves, streams, and lands in memory as a
DataFrame — no second copy on disk, in either project's repository.

Five steps:

    0. Shared repository   stage 3 open datasets into `fen-general`
    1. Discovery           X and Y query the shared DCAT catalog
    2. Virtual slices      declare bounds and columns — no bytes read
    3. Zero-copy linking   write + upload a link manifest per project
    4. Consumption         resolve -> stream -> slice -> analyse, in memory

Then a verification pass measures what each repository actually stores.

Run
---
    docker compose up -d                      # or: make up
    python examples_cesdh/fen_general_repository_linking.py

    # offline / air-gapped (synthetic profiles, everything else identical):
    FEN_LINK_OFFLINE=1 python examples_cesdh/fen_general_repository_linking.py

What you'll see
---------------
    * three datasets ingested once, into one repository
    * two DCAT discovery queries run *against another project's* repository
    * slice specs measured in bytes, next to the series they stand for
    * a manifest per project, and its size next to the raw data it points at
    * two analyses computed from streamed bytes
    * a duplication audit: raw bytes stored per repository, on disk and in the
      catalog — expected to be zero for X and Y

Why the source data is honest
-----------------------------
Staging is delegated to `raw_data_hub_lifecycle`, which fetches from Zenodo,
OPSD and Harvard Dataverse and falls back to a deterministic synthetic profile
when a source is unreachable. The true origin is recorded either way. This
example adds no new fetching and no new claims about provenance.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

try:
    from cesdh import CESDHClient
except ImportError as exc:  # pragma: no cover - setup guidance
    raise SystemExit(
        "Could not import the CESDH SDK.\n"
        "    pip install -e src/sdk\n"
        f"(original error: {exc})"
    ) from exc

REPO_ROOT = Path(__file__).resolve().parents[1]

# Staging (Zenodo / OPSD / Dataverse fetch chains + synthetic fallback) already
# exists and is exercised by its own example. Importing it rather than copying
# it is the same principle this file is about: one source of truth, referenced.
sys.path.insert(0, str(REPO_ROOT / "examples_cesdh"))
import raw_data_hub_lifecycle as rawhub  # noqa: E402

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Config:
    """Endpoints, the three repositories, and the local workspace."""

    gateway_url: str = os.environ.get("CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080")
    lakefs_url: str = os.environ.get("LAKEFS_URL", "http://localhost:8000")

    # The shared repository. Raw bytes live here and nowhere else.
    general_repository: str = os.environ.get("FEN_GENERAL_REPOSITORY", "fen-general")
    general_branch: str = os.environ.get("FEN_GENERAL_BRANCH", "open-data-v1")
    general_owner: str = "FEN-data-stewardship"

    # Consumer project A — a solar yield study.
    repo_x: str = os.environ.get("FEN_LINK_REPO_X", "project-solar-ch")
    repo_x_branch: str = "study-summer-yield"
    repo_x_owner: str = "solar-research-team"

    # Consumer project B — cross-border grid integration.
    repo_y: str = os.environ.get("FEN_LINK_REPO_Y", "project-grid-integration")
    repo_y_branch: str = "study-cross-border"
    repo_y_owner: str = "grid-integration-team"

    workspace: Path = REPO_ROOT / "data" / "fen_linking"
    http_timeout: int = int(os.environ.get("FEN_LINK_HTTP_TIMEOUT", "180"))
    offline: bool = os.environ.get("FEN_LINK_OFFLINE", "").strip() not in ("", "0", "false")

    def project_dir(self, repository: str) -> Path:
        return self.workspace / repository


say = rawhub.say
banner = rawhub.banner
human_bytes = rawhub.human_bytes
_status_code = rawhub._status_code


CATALOG_PREFIXES = """\
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX energy:  <http://energy.ethz.ch/schema#>
PREFIX qudt:    <http://qudt.org/schema/qudt/>
"""
CATALOG_GRAPH = "http://energy.ethz.ch/graph/catalog"


# ═════════════════════════════════════════════════════════════════════════════
# The link primitives — what a consumer project stores instead of the data
# ═════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class VirtualSlice:
    """A structural selection over a dataset: bounds and columns, never bytes.

    Declaring a slice reads nothing. It is a specification that travels in the
    manifest and is applied at resolution time, after the source bytes have
    been streamed into memory. Two projects can hold irreconcilable slices of
    the same series without either one forking it.
    """

    time_start: Optional[str] = None
    time_end: Optional[str] = None
    columns: tuple[str, ...] = ()
    label: str = ""

    def as_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.time_start:
            out["time_start"] = self.time_start
        if self.time_end:
            out["time_end"] = self.time_end
        if self.columns:
            out["columns"] = list(self.columns)
        if self.label:
            out["label"] = self.label
        return out

    def apply(self, frame: pd.DataFrame, time_column: str = "timestamp") -> pd.DataFrame:
        """Materialise the slice against an in-memory frame."""
        sliced = frame
        if (self.time_start or self.time_end) and time_column in sliced.columns:
            stamps = pd.to_datetime(sliced[time_column], utc=True, errors="coerce")
            mask = stamps.notna()
            if self.time_start:
                mask &= stamps >= pd.Timestamp(self.time_start)
            if self.time_end:
                mask &= stamps <= pd.Timestamp(self.time_end)
            sliced = sliced.loc[mask]
        if self.columns:
            keep = [c for c in self.columns if c in sliced.columns]
            sliced = sliced.loc[:, keep]
        return sliced.reset_index(drop=True)


@dataclass(frozen=True)
class DatasetPointer:
    """One zero-copy link from a consumer project to a shared dataset.

    `content_sha256` is the pin. It is the checksum the gateway computed when
    the source was ingested, so a later correction to the shared dataset does
    not silently change what this project analysed — the mismatch surfaces on
    the next resolution instead.
    """

    linked_repository: str
    dataset_id: str
    esdh_uri: str
    file_name: str
    lakefs_uri: str
    branch: str
    commit_hash: str
    content_sha256: str
    size_bytes: int
    carrier: str
    unit: str
    # What the pinned bytes actually cover. Slot names say 2024; a live source
    # may not. A pointer that omits this invites a slice against a period the
    # data never had — which is silently empty rather than loudly wrong.
    observed_period: str = "unknown"
    provenance: str = "unknown"
    virtual_slice: Optional[VirtualSlice] = None
    local_duplication: bool = False

    def as_json(self) -> dict[str, Any]:
        """Serialise to the link-manifest entry shape."""
        return {
            "linked_repository": self.linked_repository,
            "dataset_id": self.esdh_uri,
            "resolved_dataset_id": self.dataset_id,
            "file_name": self.file_name,
            "lakefs_uri": self.lakefs_uri,
            "branch": self.branch,
            "commit_hash": self.commit_hash,
            "content_sha256": self.content_sha256,
            "source_size_bytes": self.size_bytes,
            "carrier": self.carrier,
            "unit": self.unit,
            "observed_period": self.observed_period,
            "provenance": self.provenance,
            "virtual_slice": self.virtual_slice.as_json() if self.virtual_slice else None,
            "local_duplication": self.local_duplication,
        }


@dataclass(frozen=True)
class CompositeView:
    """A derived structure joining columns from several pointers.

    Also purely declarative — the join key and the column mapping, not the
    joined table. Repository Y's residual-load frame exists only in memory,
    for as long as an analysis needs it.
    """

    name: str
    join_on: str
    members: tuple[tuple[str, str, str], ...]   # (esdh_uri, alias, observed_period)
    description: str = ""
    # Why the join key is what it is. Two open datasets rarely share a calendar
    # year, so the key is often derived rather than taken as-is — and a reader
    # of the manifest is entitled to know which.
    key_derivation: str = ""

    def as_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "join_on": self.join_on,
            "key_derivation": self.key_derivation,
            "members": [
                {"dataset_id": uri, "as": alias, "observed_period": period}
                for uri, alias, period in self.members
            ],
            "description": self.description,
            "materialised": False,
        }


@dataclass
class LinkManifest:
    """`dataset_link_manifest.json` — everything a project keeps locally."""

    consumer_repository: str
    linked_repository: str
    generated_at: str
    pointers: list[DatasetPointer] = field(default_factory=list)
    composite_views: list[CompositeView] = field(default_factory=list)
    path: Optional[Path] = None

    def as_json(self) -> dict[str, Any]:
        return {
            "manifest_version": "1.0",
            "consumer_repository": self.consumer_repository,
            "linked_repository": self.linked_repository,
            "generated_at": self.generated_at,
            "local_duplication": False,
            "resolution": {
                "scheme": "esdh://{repository}/{dataset_id}",
                "gateway_endpoint": "GET /datasets/{dataset_id}/download",
                "required_header": "X-CESDH-Repository: <repository from the URI>",
            },
            "links": [p.as_json() for p in self.pointers],
            "composite_views": [v.as_json() for v in self.composite_views],
        }

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / "dataset_link_manifest.json"
        self.path.write_text(json.dumps(self.as_json(), indent=2) + "\n", encoding="utf-8")
        return self.path


# ═════════════════════════════════════════════════════════════════════════════
# STEP 0 — The shared FEN General Repository
# ═════════════════════════════════════════════════════════════════════════════


def ensure_repository_and_branch(client: CESDHClient, repository: str, branch: str) -> None:
    """Pre-create a repository and branch; 409 means it is already there.

    Both calls are strict by design — uploads would autoprovision anyway, so
    this only exists to make a mistyped project name loud rather than silent.
    """
    try:
        client.create_repository()
        say(f"  created repository '{repository}' (+ its MinIO bucket)")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  repository '{repository}' already exists — reusing")
        else:
            logger.exception("Repository pre-creation failed")
            say(f"  !! could not pre-create '{repository}': {str(exc)[:70]}")

    try:
        head = client.create_branch(branch, source="main").get("head", "")
        say(f"  created branch     '{branch}' at {str(head)[:12]}...")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  branch '{branch}' already exists — reusing")
        else:
            logger.exception("Branch pre-creation failed")
            say(f"  !! could not pre-create branch '{branch}': {str(exc)[:70]}")


@dataclass
class SharedDataset:
    """A dataset as it now exists inside the shared repository."""

    spec: "rawhub.RawDataset"
    dataset_id: str
    commit_hash: str
    checksum: str
    size_bytes: int
    lakefs_uri: str


def populate_general_repository(cfg: Config, run_id: str) -> list[SharedDataset]:
    """Stage the three open datasets and ingest them into `fen-general` once.

    Re-runs supersede rather than collide (`force=True`), so this whole example
    is idempotent: the shared repository ends up holding exactly one active
    copy of each series however many times you run it.
    """
    staging_cfg = rawhub.Config(
        repository=cfg.general_repository,
        branch=cfg.general_branch,
        owner=cfg.general_owner,
        offline=cfg.offline,
    )
    specs = rawhub.stage_datasets(staging_cfg)

    client = CESDHClient(
        repository=cfg.general_repository,
        gateway_url=cfg.gateway_url,
        lakefs_url=cfg.lakefs_url,
    )
    ensure_repository_and_branch(client, cfg.general_repository, cfg.general_branch)

    shared: list[SharedDataset] = []
    for spec in specs:
        try:
            result = client.upload_model(
                str(spec.path),
                owner=cfg.general_owner,
                branch=cfg.general_branch,
                version="1.0",
                description=spec.dcat_description(),
                # Raw measurements are not CESDM system models — Tier 1 stores
                # the bytes and skips schema validation instead of failing them.
                force_tier="generic",
                scenario_metadata=spec.scenario_metadata(),
                dcat_metadata=spec.dcat_metadata(),
                force=True,
                run_id=run_id,
                commit_message=f"{run_id}: publish {spec.filename} to the shared repository",
            )
        except Exception as exc:
            logger.exception("Upload failed for %s", spec.filename)
            say(f"  !! failed  {spec.filename}: {str(exc)[:60]}")
            continue

        if result.get("validation_hard_errors"):
            say(f"  !! rejected {spec.filename}: {result['validation_hard_errors'][0][:60]}")
            continue

        entry = SharedDataset(
            spec=spec,
            dataset_id=result.get("dataset_id", ""),
            commit_hash=result.get("lakefs_commit_id", ""),
            checksum=result.get("checksum", ""),
            size_bytes=int(result.get("file_size_bytes") or 0),
            lakefs_uri=result.get("lakefs_uri", ""),
        )
        shared.append(entry)
        say(
            f"  OK {spec.filename:38s} {human_bytes(entry.size_bytes):>9s}"
            f"  -> {entry.dataset_id}  @ {entry.commit_hash[:12]}"
        )
    return shared


# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 — Discovery: a consumer project queries the shared catalog
# ═════════════════════════════════════════════════════════════════════════════


def _reader_for(repository: str, cfg: Config) -> CESDHClient:
    """A client bound to another project's repository, for reading.

    Every gateway call carries `X-CESDH-Repository`, so reaching into the
    shared repository is explicit: you name it, you do not inherit it.
    """
    return CESDHClient(
        repository=repository, gateway_url=cfg.gateway_url, lakefs_url=cfg.lakefs_url
    )


def discover(
    client: CESDHClient, branch: str, carriers: list[str], country: Optional[str] = None
) -> list[dict[str, Any]]:
    """Find datasets in the bound repository by carrier, optionally by country.

    Filters are exact equality on typed predicates — `energy:energyCarrier` and
    `dcterms:spatial` — not substring matching on free text, so a description
    that merely mentions solar does not match a query for solar data.

    The subject must be `?dataset`: the gateway splices
    `?dataset energy:inRepository "…"` into every catalog GRAPH block, and any
    other variable name would cross-join the whole project.
    """
    wanted = " ".join(f'"{c.lower()}"' for c in carriers)
    country_filter = (
        f'    FILTER(LCASE(STR(?spatial)) = "{country.lower()}")\n' if country else ""
    )
    query = f"""{CATALOG_PREFIXES}
SELECT DISTINCT ?dataset_id ?title ?fileName ?carrier ?spatial ?unit ?resolution ?licence ?fileSize
WHERE {{
  GRAPH <{CATALOG_GRAPH}> {{
    ?dataset a dcat:Dataset ;
             dcterms:identifier ?dataset_id ;
             dcat:title ?title ;
             energy:fileName ?fileName ;
             energy:energyCarrier ?carrier ;
             dcterms:spatial ?spatial ;
             energy:lakefsBranch "{branch}" .
    OPTIONAL {{ ?dataset qudt:hasUnit ?unit . }}
    OPTIONAL {{ ?dataset dcat:temporalResolution ?resolution . }}
    OPTIONAL {{ ?dataset dcterms:license ?licence . }}
    OPTIONAL {{ ?dataset energy:fileSize ?fileSize . }}
    VALUES ?wanted {{ {wanted} }}
    FILTER(LCASE(STR(?carrier)) = ?wanted)
{country_filter}    FILTER NOT EXISTS {{ ?dataset energy:supersededBy ?newer . }}
  }}
}} ORDER BY ?fileName"""
    try:
        return client.sparql(query).get("results", []) or []
    except Exception as exc:
        logger.exception("Discovery query failed")
        say(f"  !! discovery failed: {str(exc)[:70]}")
        return []


def report_hits(hits: list[dict[str, Any]]) -> None:
    if not hits:
        say("    no matches")
        return
    for hit in hits:
        size = hit.get("fileSize")
        size_text = human_bytes(int(size)) if size else "?"
        say(
            f"    - {hit.get('fileName', '?'):38s} "
            f"[{hit.get('carrier', '?')} · {hit.get('spatial', '?')} · "
            f"{hit.get('unit', '?')} · {hit.get('resolution', '?')}]  {size_text}"
        )


# ═════════════════════════════════════════════════════════════════════════════
# STEP 2/3 — Virtual slices and the link manifest
# ═════════════════════════════════════════════════════════════════════════════


def build_pointer(
    shared: SharedDataset, cfg: Config, virtual_slice: Optional[VirtualSlice]
) -> DatasetPointer:
    """Turn a shared dataset into a pointer. Reads no data — metadata only."""
    return DatasetPointer(
        linked_repository=cfg.general_repository,
        dataset_id=shared.dataset_id,
        esdh_uri=f"esdh://{cfg.general_repository}/{shared.dataset_id}",
        file_name=shared.spec.filename,
        lakefs_uri=shared.lakefs_uri,
        branch=cfg.general_branch,
        commit_hash=shared.commit_hash,
        content_sha256=shared.checksum,
        size_bytes=shared.size_bytes,
        carrier=shared.spec.carrier,
        unit=shared.spec.units,
        observed_period=shared.spec.observed_period,
        provenance=shared.spec.provenance,
        virtual_slice=virtual_slice,
    )


def _observed_bounds(observed_period: str) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Parse the `start..end` string staging recorded, or (None, None)."""
    start_text, _, end_text = observed_period.partition("..")
    start = pd.to_datetime(start_text, utc=True, errors="coerce")
    end = pd.to_datetime(end_text, utc=True, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return None, None
    return start, end


def summer_slice_for(shared: SharedDataset) -> VirtualSlice:
    """A June-August window over the year this series *actually* covers.

    The slot name says 2024. PVGIS serves a typical meteorological year, OPSD
    starts in 2015, and a synthetic fallback is generated for 2024 — so a
    hardcoded 2024 window silently selects nothing from two of the three. The
    window is therefore derived from the observed period, and falls back to the
    middle third of the range when the coverage does not reach summer at all.
    """
    start, end = _observed_bounds(shared.spec.observed_period)
    columns = ("timestamp", shared.spec.value_column)
    if start is None or end is None:
        return VirtualSlice(columns=columns, label="full-period (coverage unknown)")

    jja_start = pd.Timestamp(year=start.year, month=6, day=1, tz="UTC")
    jja_end = pd.Timestamp(year=start.year, month=8, day=31, hour=23, tz="UTC")
    lo, hi = max(start, jja_start), min(end, jja_end)
    if lo <= hi:
        return VirtualSlice(
            time_start=lo.strftime("%Y-%m-%dT%H:%M:%SZ"),
            time_end=hi.strftime("%Y-%m-%dT%H:%M:%SZ"),
            columns=columns,
            label=f"summer-{start.year}-jja",
        )

    span = (end - start) / 3
    return VirtualSlice(
        time_start=(start + span).strftime("%Y-%m-%dT%H:%M:%SZ"),
        time_end=(end - span).strftime("%Y-%m-%dT%H:%M:%SZ"),
        columns=columns,
        label="mid-period fallback (coverage excludes Jun-Aug)",
    )


def publish_manifest(
    manifest: LinkManifest, cfg: Config, repository: str, branch: str, owner: str, run_id: str
) -> Optional[str]:
    """Write the manifest into the project directory and ingest it into the
    project's own repository.

    Uploading it is the point of the audit that follows: the consumer
    repository ends up holding a manifest and nothing else, which is a claim
    the catalog can be asked to confirm rather than one this script asserts.
    """
    path = manifest.write(cfg.project_dir(repository))
    say(f"  wrote  {path.relative_to(REPO_ROOT)}  ({human_bytes(path.stat().st_size)})")

    client = _reader_for(repository, cfg)
    ensure_repository_and_branch(client, repository, branch)
    try:
        result = client.upload_model(
            str(path),
            owner=owner,
            branch=branch,
            version="1.0",
            description=(
                f"Zero-copy link manifest · {len(manifest.pointers)} pointer(s) into "
                f"{manifest.linked_repository} · no raw data duplicated"
            ),
            force_tier="generic",
            dcat_metadata={
                "theme": "data-governance",
                "source": f"esdh://{manifest.linked_repository}",
                "keywords": ["link-manifest", "zero-copy", manifest.linked_repository],
            },
            force=True,
            run_id=run_id,
            commit_message=f"{run_id}: link {repository} to {manifest.linked_repository}",
        )
    except Exception as exc:
        logger.exception("Manifest upload failed for %s", repository)
        say(f"  !! manifest upload failed: {str(exc)[:70]}")
        return None

    dataset_id = result.get("dataset_id", "")
    say(
        f"  stored in '{repository}' as {dataset_id} "
        f"({human_bytes(int(result.get('file_size_bytes') or 0))})"
    )
    return dataset_id


# ═════════════════════════════════════════════════════════════════════════════
# STEP 4 — Resolution: pointer -> bytes -> in-memory frame
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class Resolution:
    """The outcome of resolving one pointer, and what it cost."""

    pointer: DatasetPointer
    frame: pd.DataFrame
    sliced: pd.DataFrame
    bytes_streamed: int
    hash_verified: bool


def resolve_pointer(pointer: DatasetPointer, cfg: Config) -> Resolution:
    """Stream the pinned bytes into memory and apply the slice. Writes nothing.

    The documented download endpoint is called directly rather than through
    `download_to_dataframe()` because these open-data CSVs carry a `#`
    provenance preamble, which `pd.read_csv` would otherwise read as a header
    row. The repository comes from the URI, not from any ambient default.
    """
    source_repository = pointer.esdh_uri.split("//", 1)[1].split("/", 1)[0]
    resp = requests.get(
        f"{cfg.gateway_url}/datasets/{pointer.dataset_id}/download",
        headers={"X-CESDH-Repository": source_repository},
        timeout=cfg.http_timeout,
    )
    resp.raise_for_status()
    payload = resp.content

    verified = hashlib.sha256(payload).hexdigest() == pointer.content_sha256
    frame = pd.read_csv(io.BytesIO(payload), comment="#")
    sliced = pointer.virtual_slice.apply(frame) if pointer.virtual_slice else frame

    say(
        f"  resolved {pointer.esdh_uri}"
    )
    say(
        f"           streamed {human_bytes(len(payload)):>9s} · {len(frame):,} rows"
        f"  ->  slice {len(sliced):,} rows"
        f"  · sha256 {'pinned' if verified else 'MISMATCH'}"
    )
    return Resolution(
        pointer=pointer,
        frame=frame,
        sliced=sliced,
        bytes_streamed=len(payload),
        hash_verified=verified,
    )


def analyse_solar_summer(resolution: Resolution) -> dict[str, Any]:
    """Repository X's analysis: summer yield from the sliced irradiation series."""
    column = resolution.pointer.virtual_slice.columns[-1] if (
        resolution.pointer.virtual_slice and resolution.pointer.virtual_slice.columns
    ) else resolution.sliced.columns[-1]
    values = pd.to_numeric(resolution.sliced[column], errors="coerce").dropna()
    if values.empty:
        return {"hours": 0, "note": "slice covered no rows of the observed period"}
    daylight = values[values > 0]
    return {
        "hours": int(values.size),
        "mean_w_m2": round(float(values.mean()), 1),
        "peak_w_m2": round(float(values.max()), 1),
        "daylight_hours": int(daylight.size),
        "energy_kwh_m2": round(float(values.sum()) / 1000.0, 1),
    }


HOUR_OF_YEAR_DERIVATION = (
    "hour_of_year = (dayofyear - 1) * 24 + hour, computed per member at "
    "resolution time. The members cover different calendar years, so they are "
    "aligned by position in the year (the standard weather-year alignment) "
    "rather than by absolute timestamp, which would intersect to nothing."
)


def _with_hour_of_year(frame: pd.DataFrame, value_as: str) -> pd.DataFrame:
    """Add the derived join key and normalise the value column name."""
    out = frame.rename(columns={frame.columns[-1]: value_as}).copy()
    stamps = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["hour_of_year"] = (stamps.dt.dayofyear - 1) * 24 + stamps.dt.hour
    out[value_as] = pd.to_numeric(out[value_as], errors="coerce")
    return out.dropna(subset=["hour_of_year", value_as])


def build_residual_load(
    wind: Resolution, demand: Resolution, view: CompositeView
) -> pd.DataFrame:
    """Repository Y's composite: join two shared series in memory.

    This is the materialisation of `view` — it exists for the lifetime of the
    call and is never written anywhere.

    The join key is derived, not taken as-is: OPSD's wind actuals and the Swiss
    demand profile come from different calendar years, so joining on absolute
    timestamps would produce an empty frame. Aligning on hour-of-year is what a
    capacity-expansion model does with a reference weather year, and the
    manifest says so in `key_derivation` rather than leaving it implicit.
    """
    left = _with_hour_of_year(wind.sliced, "wind_mw")
    right = _with_hour_of_year(demand.sliced, "demand_kw")
    joined = pd.merge(
        left, right, on=view.join_on, how="inner", suffixes=("_wind", "_demand")
    )
    if joined.empty:
        return joined

    # Per-unit availability is the shape PyPSA and Calliope both want as an
    # input profile; the absolute MW figure is a national total and would be
    # meaningless attached to a single household demand series.
    peak = joined["wind_mw"].max()
    joined["wind_pu"] = joined["wind_mw"] / peak if peak else 0.0
    joined["residual_kw"] = joined["demand_kw"] - joined["wind_pu"] * joined["demand_kw"].max()
    return joined.sort_values("hour_of_year").reset_index(drop=True)


# ═════════════════════════════════════════════════════════════════════════════
# Verification — what does each repository actually hold?
# ═════════════════════════════════════════════════════════════════════════════

RAW_DATA_SUFFIXES = {".csv", ".h5", ".hdf5", ".parquet", ".nc", ".xlsx", ".zip"}


@dataclass
class RepositoryAudit:
    repository: str
    on_disk_raw_files: list[str]
    on_disk_manifest_bytes: int
    catalog_files: list[tuple[str, int]]

    @property
    def catalog_raw_bytes(self) -> int:
        return sum(
            size for name, size in self.catalog_files
            if Path(name).suffix.lower() in RAW_DATA_SUFFIXES
        )

    @property
    def catalog_total_bytes(self) -> int:
        return sum(size for _, size in self.catalog_files)

    @property
    def clean(self) -> bool:
        return not self.on_disk_raw_files and self.catalog_raw_bytes == 0


def audit_repository(repository: str, branch: str, cfg: Config) -> RepositoryAudit:
    """Measure duplication two ways: on the local filesystem and in the catalog.

    The on-disk check is what a researcher would notice; the catalog check is
    what the platform can prove. Both have to come back empty for a consumer
    project before "zero-copy" is more than a slogan.
    """
    project_dir = cfg.project_dir(repository)
    raw_files: list[str] = []
    manifest_bytes = 0
    if project_dir.exists():
        for item in sorted(project_dir.rglob("*")):
            if not item.is_file():
                continue
            if item.suffix.lower() in RAW_DATA_SUFFIXES:
                raw_files.append(str(item.relative_to(project_dir)))
            else:
                manifest_bytes += item.stat().st_size

    query = f"""{CATALOG_PREFIXES}
SELECT ?fileName ?fileSize WHERE {{
  GRAPH <{CATALOG_GRAPH}> {{
    ?dataset a dcat:Dataset ;
             energy:fileName ?fileName ;
             energy:lakefsBranch "{branch}" .
    OPTIONAL {{ ?dataset energy:fileSize ?fileSize . }}
    FILTER NOT EXISTS {{ ?dataset energy:supersededBy ?newer . }}
  }}
}} ORDER BY ?fileName"""
    catalog_files: list[tuple[str, int]] = []
    try:
        for row in _reader_for(repository, cfg).sparql(query).get("results", []) or []:
            catalog_files.append(
                (row.get("fileName", "?"), int(row.get("fileSize") or 0))
            )
    except Exception as exc:
        logger.exception("Audit query failed for %s", repository)
        say(f"  !! audit query failed for '{repository}': {str(exc)[:60]}")

    return RepositoryAudit(
        repository=repository,
        on_disk_raw_files=raw_files,
        on_disk_manifest_bytes=manifest_bytes,
        catalog_files=catalog_files,
    )


def report_audit(audit: RepositoryAudit, role: str) -> None:
    say(f"  {audit.repository}  ({role})")
    for name, size in audit.catalog_files:
        marker = "RAW " if Path(name).suffix.lower() in RAW_DATA_SUFFIXES else "link"
        say(f"      [{marker}] {name:42s} {human_bytes(size):>9s}")
    if not audit.catalog_files:
        say("      (no active datasets on this branch)")
    say(
        f"      catalog: {human_bytes(audit.catalog_raw_bytes)} raw"
        f" of {human_bytes(audit.catalog_total_bytes)} total"
        f"   ·   on disk: {len(audit.on_disk_raw_files)} raw file(s),"
        f" {human_bytes(audit.on_disk_manifest_bytes)} of manifests"
    )


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("FEN_LINK_LOG_LEVEL", "WARNING"),
        format="%(levelname)s %(name)s: %(message)s",
    )
    cfg = Config()
    run_id = f"fen_link_{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    timings: dict[str, float] = {}
    t_total = time.perf_counter()

    banner("Zero-copy linking to the FEN General Repository — setup")
    say(f"  gateway              {cfg.gateway_url}")
    say(f"  shared repository    {cfg.general_repository}  (branch {cfg.general_branch})")
    say(f"  Repository X         {cfg.repo_x}  (branch {cfg.repo_x_branch})")
    say(f"  Repository Y         {cfg.repo_y}  (branch {cfg.repo_y_branch})")
    say(f"  workspace            {cfg.workspace.relative_to(REPO_ROOT)}")
    say(f"  mode                 {'OFFLINE (synthetic profiles)' if cfg.offline else 'live fetch'}")

    # ── STEP 0 ───────────────────────────────────────────────────────────────
    banner("STEP 0 — Publish the open datasets into the shared repository, once")
    t0 = time.perf_counter()
    shared = populate_general_repository(cfg, run_id)
    timings["publish"] = time.perf_counter() - t0
    if len(shared) < 3:
        say("")
        say(f"  !! only {len(shared)}/3 datasets are available in the shared repository.")
        say("     Nothing downstream can be demonstrated honestly. Is the stack up?")
        return 1
    by_file = {s.spec.filename: s for s in shared}
    raw_total = sum(s.size_bytes for s in shared)
    say(f"  shared repository now holds {human_bytes(raw_total)} of raw time series")

    # ── STEP 1 ───────────────────────────────────────────────────────────────
    banner("STEP 1 — Discovery: two projects query the shared catalog")
    t0 = time.perf_counter()
    general_reader = _reader_for(cfg.general_repository, cfg)

    say(f"  Repository X ({cfg.repo_x}) asks: carrier = solar, spatial = CH")
    x_hits = discover(general_reader, cfg.general_branch, ["solar"], country="CH")
    report_hits(x_hits)

    say("")
    say(f"  Repository Y ({cfg.repo_y}) asks: carrier in (wind, electricity), any country")
    say("    ('electricity_demand' is the research term; the catalog vocabulary")
    say("     records the carrier as 'electricity' and the measurement as demand_kw)")
    y_hits = discover(general_reader, cfg.general_branch, ["wind", "electricity"])
    report_hits(y_hits)
    timings["discover"] = time.perf_counter() - t0

    if not x_hits or len(y_hits) < 2:
        say("")
        say("  !! discovery did not return the expected datasets — stopping before")
        say("     building links that would point at nothing.")
        return 1

    # ── STEP 2 ───────────────────────────────────────────────────────────────
    banner("STEP 2 — Virtual slices: declare structure, move no bytes")
    t0 = time.perf_counter()
    solar = by_file["CH_solar_irradiation_2024.csv"]
    wind = by_file["DE_wind_onshore_generation_2024.csv"]
    demand = by_file["CH_household_demand_profiles.csv"]

    say("  observed coverage of each shared series (not the slot name):")
    for entry in (solar, wind, demand):
        say(f"      {entry.spec.filename:38s} {entry.spec.observed_period}")
    say("")

    summer = summer_slice_for(solar)
    wind_slice = VirtualSlice(
        columns=("timestamp", wind.spec.value_column), label="full-period-wind"
    )
    demand_slice = VirtualSlice(
        columns=("timestamp", demand.spec.value_column), label="full-period-demand"
    )
    residual_view = CompositeView(
        name="ch_de_residual_load",
        join_on="hour_of_year",
        members=(
            (
                f"esdh://{cfg.general_repository}/{wind.dataset_id}",
                "wind_mw",
                wind.spec.observed_period,
            ),
            (
                f"esdh://{cfg.general_repository}/{demand.dataset_id}",
                "demand_kw",
                demand.spec.observed_period,
            ),
        ),
        description=(
            "Inner join of German onshore wind actuals and Swiss household demand, "
            "used to derive a per-unit wind availability and a residual load series"
        ),
        key_derivation=HOUR_OF_YEAR_DERIVATION,
    )

    spec_bytes = len(json.dumps(summer.as_json()).encode())
    say(f"  X · slice '{summer.label}'")
    say(f"      window {summer.time_start} .. {summer.time_end}")
    say(
        f"      the spec is {spec_bytes} B; the series it selects from is "
        f"{human_bytes(solar.size_bytes)} — and stays where it is"
    )
    say("")
    say(f"  Y · composite view '{residual_view.name}' over 2 datasets")
    say(f"      join key '{residual_view.join_on}' — derived, because the members")
    say(f"      cover {wind.spec.observed_period[:10]}.. and {demand.spec.observed_period[:10]}..")
    say("      declared, not materialised: the joined table is built in memory in")
    say("      step 4 and discarded when the analysis returns")
    timings["slice"] = time.perf_counter() - t0

    # ── STEP 3 ───────────────────────────────────────────────────────────────
    banner("STEP 3 — Zero-copy linking: one manifest per project")
    t0 = time.perf_counter()
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    manifest_x = LinkManifest(
        consumer_repository=cfg.repo_x,
        linked_repository=cfg.general_repository,
        generated_at=generated_at,
        pointers=[build_pointer(solar, cfg, summer)],
    )
    manifest_y = LinkManifest(
        consumer_repository=cfg.repo_y,
        linked_repository=cfg.general_repository,
        generated_at=generated_at,
        pointers=[
            build_pointer(wind, cfg, wind_slice),
            build_pointer(demand, cfg, demand_slice),
        ],
        composite_views=[residual_view],
    )

    say(f"  Repository X — 1 pointer")
    publish_manifest(manifest_x, cfg, cfg.repo_x, cfg.repo_x_branch, cfg.repo_x_owner, run_id)
    say("")
    say(f"  Repository Y — 2 pointers + 1 composite view")
    publish_manifest(manifest_y, cfg, cfg.repo_y, cfg.repo_y_branch, cfg.repo_y_owner, run_id)

    manifest_bytes = sum(
        m.path.stat().st_size for m in (manifest_x, manifest_y) if m.path
    )
    say("")
    say(
        f"  {human_bytes(manifest_bytes)} of manifests now stand in for "
        f"{human_bytes(raw_total)} of raw data across both projects"
    )
    say("  first entry of Repository X's manifest:")
    for line in json.dumps(manifest_x.as_json()["links"][0], indent=2).splitlines():
        say(f"      {line}")
    timings["link"] = time.perf_counter() - t0

    # ── STEP 4 ───────────────────────────────────────────────────────────────
    banner("STEP 4 — Consumption: resolve, stream, analyse — nothing written")
    t0 = time.perf_counter()

    say(f"  Repository X — summer solar yield")
    solar_res = resolve_pointer(manifest_x.pointers[0], cfg)
    yield_stats = analyse_solar_summer(solar_res)
    for key, value in yield_stats.items():
        say(f"      {key:16s} {value}")

    say("")
    say(f"  Repository Y — cross-border residual load")
    wind_res = resolve_pointer(manifest_y.pointers[0], cfg)
    demand_res = resolve_pointer(manifest_y.pointers[1], cfg)
    residual = build_residual_load(wind_res, demand_res, residual_view)
    if residual.empty:
        say("      the two members share no hour-of-year positions this run —")
        say("      the composite view resolves but selects no rows")
        correlation = float("nan")
    else:
        correlation = float(residual["wind_pu"].corr(residual["demand_kw"]))
        say(f"      joined rows      {len(residual):,}  (aligned on hour_of_year)")
        say(f"      wind/demand corr {correlation:+.3f}")
        say(f"      peak residual    {residual['residual_kw'].max():,.1f} kW")
        say(f"      mean wind p.u.   {residual['wind_pu'].mean():.3f}")
        say("")
        say("      PyPSA-shaped input profile (head), built in memory:")
        preview = residual[["hour_of_year", "wind_pu", "demand_kw"]].head(3)
        for line in preview.to_string(index=False).splitlines():
            say(f"        {line}")

    resolutions = [solar_res, wind_res, demand_res]
    streamed = sum(r.bytes_streamed for r in resolutions)
    hashes_ok = all(r.hash_verified for r in resolutions)
    timings["consume"] = time.perf_counter() - t0

    # ── Verification ─────────────────────────────────────────────────────────
    banner("VERIFICATION — where do the bytes actually live?")
    audits = [
        (audit_repository(cfg.general_repository, cfg.general_branch, cfg), "shared source"),
        (audit_repository(cfg.repo_x, cfg.repo_x_branch, cfg), "consumer · solar study"),
        (audit_repository(cfg.repo_y, cfg.repo_y_branch, cfg), "consumer · grid study"),
    ]
    for audit, role in audits:
        report_audit(audit, role)
        say("")

    consumers = [a for a, _ in audits[1:]]
    no_duplication = all(a.clean for a in consumers)
    consumer_link_bytes = sum(a.catalog_total_bytes for a in consumers)

    # ── Summary ──────────────────────────────────────────────────────────────
    banner("SUMMARY")
    rows = [
        ("shared repository", f"{cfg.general_repository} · {len(shared)} datasets"),
        ("raw bytes stored once", f"{raw_total:,} ({human_bytes(raw_total)})"),
        ("consumer repositories", f"{cfg.repo_x}, {cfg.repo_y}"),
        ("raw bytes duplicated", f"{sum(a.catalog_raw_bytes for a in consumers):,}"),
        ("bytes consumers do store", f"{consumer_link_bytes:,} ({human_bytes(consumer_link_bytes)}) of manifests"),
        ("storage avoided", f"{human_bytes(raw_total)} — {raw_total / max(consumer_link_bytes, 1):.0f}x the link cost"),
        ("pointers resolved", f"{len(resolutions)} · {human_bytes(streamed)} streamed to memory"),
        ("content hashes pinned", "all matched" if hashes_ok else "MISMATCH — source changed"),
        ("virtual slice applied", f"{len(solar_res.frame):,} rows -> {len(solar_res.sliced):,} rows"),
        ("composite view rows", f"{len(residual):,} (in memory, never written)"),
        ("zero-copy verified", "YES" if no_duplication else "NO — raw data found in a consumer repo"),
    ]
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        say(f"  {key.ljust(width)} : {value}")

    say("")
    say("  timings")
    for phase in ("publish", "discover", "slice", "link", "consume"):
        say(f"    {phase.ljust(9)} {timings.get(phase, 0.0):6.2f}s")
    say(f"    {'total'.ljust(9)} {time.perf_counter() - t_total:6.2f}s")

    say("")
    if no_duplication and hashes_ok:
        say("  Both projects analysed the shared data. Neither one copied it.")
        return 0
    say("  !! the zero-copy guarantee did not hold this run — see the audit above.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
