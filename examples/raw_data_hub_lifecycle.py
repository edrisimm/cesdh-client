#!/usr/bin/env python3
"""
raw_data_hub_lifecycle.py - raw time-series lifecycle on the Energy Systems Data Hub
=====================================================================================

Researchers usually have raw profiles long before they have a CESDM system model:
an irradiation series from Zenodo, a national generation curve from OPSD, a load
profile from a Dataverse deposit. This example shows the full ESDH lifecycle for
that raw material - stage, upload, annotate, search, fetch - *without* building a
CESDM model first.

Four steps, mirroring how the data actually moves:

    1. Upload & store      repository + research branch, then the raw CSVs
    2. Annotate            DCAT metadata attached at ingest (title, coverage,
                           resolution, carrier, units, licence, provenance)
    3. Search              query the catalog by DCAT attributes
                           ("country = CH  AND  carrier = solar")
    4. Fetch               download a selected dataset back and verify it byte-for-byte

Run
---
    docker compose up -d                      # or: make up
    python examples_cesdh/raw_data_hub_lifecycle.py

    # offline / air-gapped:
    RAW_HUB_OFFLINE=1 python examples_cesdh/raw_data_hub_lifecycle.py

What you'll see
---------------
    * a staging report per dataset, naming its true origin (live fetch or fallback)
    * an upload line per file with its dataset_id
    * two catalog searches - one deterministic, one natural-language
    * a SHA-256 round-trip check on the downloaded file
    * a timing + verification summary

Data honesty
------------
Live sources are real and are fetched over their public APIs. When a source is
unreachable, rate-limited, or serves nothing usable under the size cap, the slot
falls back to a deterministic synthetic profile. Either way the true origin is
recorded in the CSV header, in the DCAT description, and in the final summary -
the catalog never claims a synthetic series came from Zenodo.

Filenames are fixed slot names. Where a live source covers a different period
than the slot name suggests, the real coverage is recorded as `observed_period`
in the DCAT description rather than being silently relabelled.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import math
import os
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from cesdh import CESDHClient
except ImportError as exc:  # pragma: no cover - setup guidance
    raise SystemExit(
        "Could not import the CESDH SDK.\n"
        "    pip install -e src/sdk\n"
        f"(original error: {exc})"
    ) from exc

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]


# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Config:
    """Endpoints, identity and I/O locations. All overridable by environment."""

    gateway_url: str = os.environ.get(
        "CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080"
    )
    lakefs_url: str = os.environ.get("LAKEFS_URL", "http://localhost:8000")

    # Bucket-safe: lowercase alphanumeric + hyphens (gateway rejects anything else).
    repository: str = os.environ.get("RAW_HUB_REPOSITORY", "raw-energy-timeseries")
    # LakeFS branch ids accept letters, digits, underscores and dashes only -
    # "dev/2024-update" is rejected by LakeFS itself, not by this platform.
    branch: str = os.environ.get("RAW_HUB_BRANCH", "dev-2024-update")
    owner: str = os.environ.get("RAW_HUB_OWNER", "FEN-team")

    staging_dir: Path = REPO_ROOT / "data" / "raw_staging"
    download_dir: Path = REPO_ROOT / "data" / "downloaded_from_esdh"

    # Streaming cap. OPSD's hourly series is ~130 MB; a PoC needs a few thousand
    # rows, not the whole archive, so every fetch is capped and cut at the last
    # complete line. This is the "selective stream" the comparison table claims.
    max_fetch_bytes: int = int(os.environ.get("RAW_HUB_MAX_BYTES", str(1_500_000)))
    http_timeout: int = int(os.environ.get("RAW_HUB_HTTP_TIMEOUT", "30"))
    offline: bool = os.environ.get("RAW_HUB_OFFLINE", "").strip() not in (
        "",
        "0",
        "false",
    )
    nl_search_timeout: int = int(os.environ.get("RAW_HUB_NL_TIMEOUT", "180"))


def say(message: str = "") -> None:
    """Single output channel - plain stdout, flushed so piping stays ordered."""
    print(message, flush=True)


def banner(title: str) -> None:
    say("")
    say("=" * 78)
    say(f"  {title}")
    say("=" * 78)


def human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB"):
        if size < 1024 or unit == "MB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} MB"


# ═════════════════════════════════════════════════════════════════════════════
# Dataset descriptors - the DCAT facts we intend to publish
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class RawDataset:
    """One raw time-series slot and the DCAT metadata it will carry.

    `filename` is a stable slot name; `observed_period` is filled in from the
    data that actually lands, so the description stays truthful even when a live
    source covers a different span than the slot name implies.
    """

    filename: str
    title: str
    carrier: str
    units: str
    temporal_resolution: str  # ISO-8601 duration, e.g. PT1H
    country: str  # ISO-3166 alpha-2
    spatial_coverage: str  # full name - becomes the searchable keyword
    licence: str
    source_label: str
    value_column: str

    # Filled during staging
    path: Path | None = None
    rows: int = 0
    observed_period: str = "unknown"
    provenance: str = "pending"
    is_live: bool = False
    sha256: str = ""

    def dcat_description(self) -> str:
        """Flatten the DCAT facts into the one free-text field the catalog indexes.

        Since the catalog gained first-class predicates for carrier, units,
        resolution and licence (see dcat_metadata()), this string no longer has
        to carry them for querying. The `key: value` pairs are kept because they
        remain the most readable one-line summary in a listing - but searches
        should filter on the predicates, not on this text.
        """
        return " · ".join(
            [
                self.title,
                f"carrier: {self.carrier}",
                f"units: {self.units}",
                f"temporal_resolution: {self.temporal_resolution}",
                f"spatial_coverage: {self.country}",
                f"licence: {self.licence}",
                f"observed_period: {self.observed_period}",
                f"provenance: {self.provenance}",
            ]
        )

    def dcat_metadata(self) -> dict[str, Any]:
        """Domain metadata as first-class DCAT / QUDT predicates.

        Each key maps onto a published vocabulary in the catalog:
        carrier -> energy:energyCarrier, unit -> qudt:hasUnit,
        temporal_resolution -> dcat:temporalResolution (xsd:duration),
        license -> dcterms:license, spatial_coverage -> dcterms:spatial,
        temporal_start/end -> a dcterms:PeriodOfTime node. Searching these is
        exact, unlike the substring matching on dcat:description this replaces.
        """
        start, _, end = self.observed_period.partition("..")
        meta: dict[str, Any] = {
            "carrier": self.carrier,
            "unit": self.units,
            "temporal_resolution": self.temporal_resolution,
            "spatial_coverage": self.country,
            "license": self.licence,
            "source": self.provenance,
            "theme": "energy",
            "measurement_type": self.value_column,
            "keywords": [self.carrier, self.country, self.spatial_coverage],
        }
        # Only assert a covered period once real data has been staged.
        if start.endswith("Z") and end.endswith("Z"):
            meta["temporal_start"], meta["temporal_end"] = start, end
        return meta

    def scenario_metadata(self) -> dict[str, Any]:
        """Metadata the gateway turns into dcat:keyword tags.

        geographic_region is tokenised by the catalog, so "Switzerland" becomes
        the keyword "switzerland" - that is what makes country filtering work.
        target_year is deliberately omitted: the slot names say 2024 but a live
        source may cover another period, and asserting a year we have not
        observed would put a false fact in the catalog.
        """
        return {"geographic_region": self.spatial_coverage}


DATASETS: list[RawDataset] = [
    RawDataset(
        filename="CH_solar_irradiation_2024.csv",
        title="Swiss hourly global horizontal irradiation",
        carrier="solar",
        units="W/m2",
        temporal_resolution="PT1H",
        country="CH",
        spatial_coverage="Switzerland",
        licence="CC-BY-4.0",
        source_label="zenodo",
        value_column="ghi_w_per_m2",
    ),
    RawDataset(
        filename="DE_wind_onshore_generation_2024.csv",
        title="German onshore wind generation, hourly actuals",
        carrier="wind",
        units="MW",
        temporal_resolution="PT1H",
        country="DE",
        spatial_coverage="Germany",
        licence="CC-BY-4.0",
        source_label="opsd",
        value_column="wind_onshore_mw",
    ),
    RawDataset(
        filename="CH_household_demand_profiles.csv",
        title="Swiss residential household electricity demand profile",
        carrier="electricity",
        units="kW",
        temporal_resolution="PT1H",
        country="CH",
        spatial_coverage="Switzerland",
        licence="CC-BY-4.0",
        source_label="dataverse",
        value_column="demand_kw",
    ),
]


# ═════════════════════════════════════════════════════════════════════════════
# PART 1a - Open-data fetchers (Zenodo / OPSD / Dataverse) with offline fallback
# ═════════════════════════════════════════════════════════════════════════════


class FetchError(RuntimeError):
    """A live source could not supply usable data; the caller falls back."""


def _stream_capped(url: str, cfg: Config, *, cap: int | None = None) -> str:
    """GET `url`, stopping after `cap` bytes, truncated at the last complete line.

    Large open-data CSVs are streamed rather than downloaded whole: we want a few
    thousand rows, not a 130 MB archive. Cutting at the final newline guarantees
    the result is still parseable CSV.
    """
    limit = cap or cfg.max_fetch_bytes
    buf = io.BytesIO()
    with requests.get(url, stream=True, timeout=cfg.http_timeout) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=65_536):
            buf.write(chunk)
            if buf.tell() >= limit:
                break
    raw = buf.getvalue()
    if len(raw) >= limit:
        raw = raw[: raw.rfind(b"\n") + 1] or raw
    text = raw.decode("utf-8", errors="replace")
    if not text.strip():
        raise FetchError(f"empty payload from {url}")
    return text


def _parse_two_columns(
    text: str, time_keys: tuple[str, ...], value_keys: tuple[str, ...]
) -> list[tuple[str, float]]:
    """Pull (timestamp, value) pairs out of a CSV whose schema we do not control.

    Column names are matched case-insensitively by substring, because open
    portals rarely agree on headers. Unparseable rows are skipped, not guessed at.
    """
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise FetchError("no CSV header")

    def _find(candidates: tuple[str, ...]) -> str | None:
        for field_name in reader.fieldnames or []:
            low = (field_name or "").lower()
            if any(c in low for c in candidates):
                return field_name
        return None

    t_col, v_col = _find(time_keys), _find(value_keys)
    if not t_col or not v_col:
        raise FetchError(
            f"no timestamp/value columns (looked for {time_keys}/{value_keys} "
            f"in {(reader.fieldnames or [])[:8]})"
        )

    rows: list[tuple[str, float]] = []
    for record in reader:
        stamp, raw_value = record.get(t_col), record.get(v_col)
        if not stamp or raw_value in (None, ""):
            continue
        try:
            rows.append((str(stamp), float(raw_value)))
        except (TypeError, ValueError):
            continue
    if not rows:
        raise FetchError(f"no numeric rows in column {v_col!r}")
    return rows


def _strip_preamble(text: str, header_marker: str) -> str:
    """Drop provider preamble lines above the real CSV header.

    PVGIS (and several portals) prepend free-text metadata before the header
    row; csv.DictReader would otherwise take the first preamble line as the
    header and find no usable columns.
    """
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.lower().startswith(header_marker.lower()):
            return "\n".join(lines[index:])
    raise FetchError(f"header starting {header_marker!r} not found")


def fetch_pvgis(cfg: Config) -> tuple[list[tuple[str, float]], str]:
    """Hourly global horizontal irradiation for Bern from the JRC PVGIS API.

    Used as the second attempt for the solar slot: Zenodo's generic search can
    surface records whose CSVs are not time series at all, whereas PVGIS is a
    stable, key-free open API that always answers with a real modelled series.
    With slope=0 the three components sum to global horizontal irradiance.
    """
    url = (
        "https://re.jrc.ec.europa.eu/api/v5_2/seriescalc"
        "?lat=46.95&lon=7.45&startyear=2020&endyear=2020"
        "&outputformat=csv&components=1"
    )
    body = _strip_preamble(_stream_capped(url, cfg, cap=cfg.max_fetch_bytes), "time,")

    rows: list[tuple[str, float]] = []
    for record in csv.DictReader(io.StringIO(body)):
        stamp = (record.get("time") or "").strip()
        if len(stamp) < 13 or not stamp[:8].isdigit():
            continue  # trailing provider footer lines
        try:
            total = sum(
                float(record.get(component) or 0.0)
                for component in ("Gb(i)", "Gd(i)", "Gr(i)")
            )
        except (TypeError, ValueError):
            continue
        iso = f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}T{stamp[9:11]}:{stamp[11:13]}:00Z"
        rows.append((iso, round(total, 3)))

    if not rows:
        raise FetchError("PVGIS returned no parseable hourly rows")
    return rows, "pvgis:JRC/seriescalc lat=46.95 lon=7.45 (PVGIS-SARAH2, 2020)"


def fetch_zenodo(cfg: Config) -> tuple[list[tuple[str, float]], str]:
    """Resolve a small open CSV through the Zenodo REST search API.

    Searches records rather than pinning one DOI, so the example keeps working as
    deposits come and go; the smallest CSV under the cap wins.
    """
    search = requests.get(
        "https://zenodo.org/api/records",
        params={"q": "solar irradiation hourly", "type": "dataset", "size": "10"},
        timeout=cfg.http_timeout,
    )
    search.raise_for_status()

    candidates: list[tuple[int, str, str, str]] = []
    for hit in search.json().get("hits", {}).get("hits", []):
        record_id = str(hit.get("id"))
        licence = ((hit.get("metadata") or {}).get("license") or {}).get(
            "id", "unknown"
        )
        for entry in hit.get("files", []) or []:
            key = str(entry.get("key", ""))
            size = int(entry.get("size") or 0)
            link = (entry.get("links") or {}).get("self", "")
            if (
                key.lower().endswith(".csv")
                and 0 < size <= cfg.max_fetch_bytes
                and link
            ):
                candidates.append((size, link, record_id, licence))
    if not candidates:
        raise FetchError("no Zenodo CSV under the size cap")

    # Open deposits use wildly different schemas, so try each candidate
    # (cheapest first) and keep the first one we can actually parse.
    last: str = "no candidate parsed"
    for _, link, record_id, licence in sorted(candidates):
        try:
            rows = _parse_two_columns(
                _stream_capped(link, cfg),
                ("time", "date", "stamp", "hour", "period", "utc"),
                ("ghi", "irrad", "solar", "radiation", "glob", "value", "power", "kw"),
            )
        except (FetchError, requests.RequestException) as exc:
            last = str(exc)
            continue
        return rows, f"zenodo:record/{record_id} (licence {licence})"
    raise FetchError(last)


def fetch_opsd(cfg: Config) -> tuple[list[tuple[str, float]], str]:
    """Stream German onshore wind actuals from the OPSD time-series package.

    The resource list is read from the package's datapackage.json - the portal's
    own machine-readable index - then the CSV is byte-capped mid-stream.
    """
    base = "https://data.open-power-system-data.org/time_series/2020-10-06"
    manifest = requests.get(f"{base}/datapackage.json", timeout=cfg.http_timeout)
    manifest.raise_for_status()

    target = next(
        (
            r.get("path")
            for r in manifest.json().get("resources", [])
            if str(r.get("path", "")).endswith("60min_singleindex.csv")
        ),
        None,
    )
    if not target:
        raise FetchError("no 60-minute resource in the OPSD datapackage")

    rows = _parse_two_columns(
        _stream_capped(f"{base}/{target}", cfg),
        ("utc_timestamp", "timestamp", "time"),
        ("de_wind_onshore_generation_actual", "wind_onshore", "wind"),
    )
    return rows, f"opsd:time_series/2020-10-06/{target} (byte-capped stream)"


def fetch_dataverse(cfg: Config) -> tuple[list[tuple[str, float]], str]:
    """Resolve a load-profile CSV through the Harvard Dataverse search API."""
    items: list[dict] = []
    for phrase in (
        "household electricity load profile",
        "residential load profile hourly csv",
        "electricity demand time series",
    ):
        try:
            search = requests.get(
                "https://dataverse.harvard.edu/api/search",
                params={"q": phrase, "type": "file", "per_page": "20"},
                timeout=cfg.http_timeout,
            )
            search.raise_for_status()
            items.extend(search.json().get("data", {}).get("items", []))
        except requests.RequestException:
            continue
    if not items:
        raise FetchError("Dataverse search returned nothing")

    for item in items:
        name = str(item.get("name", ""))
        file_id = item.get("file_id") or item.get("entity_id")
        size = int(item.get("size_in_bytes") or 0)
        if not (name.lower().endswith(".csv") and file_id):
            continue
        if size and size > cfg.max_fetch_bytes * 8:
            continue
        try:
            rows = _parse_two_columns(
                _stream_capped(
                    f"https://dataverse.harvard.edu/api/access/datafile/{file_id}", cfg
                ),
                ("time", "date", "stamp", "hour", "period"),
                ("load", "demand", "consumption", "kw", "value", "power"),
            )
        except (FetchError, requests.RequestException):
            continue
        return rows, f"dataverse:harvard/datafile/{file_id}"

    raise FetchError("no usable Dataverse load-profile CSV")


# Each slot names an ordered chain of live sources. The first that yields a
# parseable series wins; synthetic generation is only reached when all of them
# fail. Chains exist because open portals are not uniformly reliable: Zenodo's
# search can return records whose CSVs are not time series, so the solar slot
# falls through to PVGIS, a key-free API that always answers.
FETCH_CHAINS: dict[
    str, list[Callable[[Config], tuple[list[tuple[str, float]], str]]]
] = {
    "zenodo": [fetch_zenodo, fetch_pvgis],
    "opsd": [fetch_opsd],
    "dataverse": [fetch_dataverse],
}


# ── Deterministic fallback generators ────────────────────────────────────────


def _synthetic(dataset: RawDataset, hours: int = 8_760) -> list[tuple[str, float]]:
    """Generate a physically plausible year of hourly values for one carrier.

    Seeded on the filename, so an offline run is byte-identical every time and
    the SHA-256 round-trip check stays meaningful. These are stand-ins, never
    presented as measurements: every consumer of this list marks the dataset
    `synthetic-fallback`.
    """
    rng = random.Random(dataset.filename)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    out: list[tuple[str, float]] = []

    for hour in range(hours):
        moment = start + timedelta(hours=hour)
        hour_of_day = moment.hour
        day_of_year = moment.timetuple().tm_yday
        season = math.sin(math.pi * (day_of_year - 80) / 365.0)  # peaks midsummer

        if dataset.carrier == "solar":
            daylight = math.sin(math.pi * (hour_of_day - 6) / 12.0)
            value = max(0.0, 950 * daylight * (0.55 + 0.45 * season))
            value *= rng.uniform(0.55, 1.0)  # cloud cover
        elif dataset.carrier == "wind":
            base = 14_000 + 9_000 * -season  # windier in winter
            value = max(0.0, base * rng.betavariate(2.0, 2.4) * 2.0)
        else:  # residential electricity
            morning = math.exp(-((hour_of_day - 7.5) ** 2) / 4.0)
            evening = math.exp(-((hour_of_day - 19.5) ** 2) / 5.0)
            value = 0.25 + 1.5 * morning + 2.4 * evening
            value *= 1.0 + 0.18 * -season  # winter heating
            value *= rng.uniform(0.85, 1.15)

        out.append((moment.strftime("%Y-%m-%dT%H:%M:%SZ"), round(value, 3)))
    return out


# ═════════════════════════════════════════════════════════════════════════════
# PART 1b - Stage the datasets into ./data/raw_staging/
# ═════════════════════════════════════════════════════════════════════════════


def _write_csv(dataset: RawDataset, rows: list[tuple[str, float]], cfg: Config) -> Path:
    """Write one staged CSV, with provenance in a comment header.

    The header travels with the file, so origin survives even if the CSV is
    copied out of ESDH and emailed around.
    """
    cfg.staging_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.staging_dir / dataset.filename
    with path.open("w", newline="", encoding="utf-8") as handle:
        handle.write(f"# title: {dataset.title}\n")
        handle.write(f"# provenance: {dataset.provenance}\n")
        handle.write(f"# carrier: {dataset.carrier} | units: {dataset.units}\n")
        handle.write(
            f"# spatial_coverage: {dataset.country} | "
            f"temporal_resolution: {dataset.temporal_resolution}\n"
        )
        handle.write(f"# licence: {dataset.licence}\n")
        writer = csv.writer(handle)
        writer.writerow(["timestamp", dataset.value_column])
        writer.writerows(rows)
    return path


def stage_datasets(cfg: Config) -> list[RawDataset]:
    """Fetch (or synthesise) every slot and write it to the staging directory."""
    for dataset in DATASETS:
        rows: list[tuple[str, float]] = []

        if cfg.offline:
            dataset.provenance = "synthetic-fallback (RAW_HUB_OFFLINE=1)"
        else:
            last_error = "no source attempted"
            for fetcher in FETCH_CHAINS[dataset.source_label]:
                name = fetcher.__name__.replace("fetch_", "")
                try:
                    started = time.perf_counter()
                    rows, provenance = fetcher(cfg)
                    dataset.provenance = provenance
                    dataset.is_live = True
                    say(
                        f"  fetched  {dataset.filename:38s} {len(rows):>6,} rows "
                        f"in {time.perf_counter() - started:4.1f}s  <- {name}"
                    )
                    break
                except (FetchError, requests.RequestException, ValueError) as exc:
                    last_error = f"{name}: {exc}"
                    logger.info("Live fetch failed (%s)", name, exc_info=True)
                    say(
                        f"  retry    {dataset.filename:38s} {name} failed: {str(exc)[:46]}"
                    )
            if not rows:
                dataset.provenance = f"synthetic-fallback ({last_error[:60]})"

        if not rows:
            rows = _synthetic(dataset)
            if dataset.is_live:  # pragma: no cover - defensive
                dataset.is_live = False
            say(f"  staged   {dataset.filename:38s} {len(rows):>6,} rows  <- synthetic")

        dataset.rows = len(rows)
        dataset.observed_period = f"{rows[0][0]}..{rows[-1][0]}"
        dataset.path = _write_csv(dataset, rows, cfg)
        dataset.sha256 = hashlib.sha256(dataset.path.read_bytes()).hexdigest()

    return DATASETS


# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 - Repository and research branch
# ═════════════════════════════════════════════════════════════════════════════


def _status_code(exc: Exception) -> int | None:
    """HTTP status behind an SDK error, or None - read without importing the
    transport, so this example stays on the documented SDK surface."""
    return getattr(getattr(exc, "response", None), "status_code", None)


def ensure_repository_and_branch(client: CESDHClient, cfg: Config) -> None:
    """Create the repository and research branch; treat 409 as 'already there'.

    Both calls are strict by design (they raise 409 rather than silently
    provisioning), which is what makes a mistyped repository name visible. Uploads
    would autoprovision anyway, so this is a pre-flight check, never a hard gate.
    """
    try:
        client.create_repository()
        say(f"  created repository '{cfg.repository}' (+ its MinIO bucket)")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  repository '{cfg.repository}' already exists - reusing")
        else:
            logger.exception("Repository pre-creation failed")
            say(f"  !! could not pre-create repository: {exc}")
            say("     continuing - the first upload autoprovisions it")

    try:
        head = client.create_branch(cfg.branch, source="main").get("head", "")
        say(f"  created branch '{cfg.branch}' at {str(head)[:12]}...")
    except Exception as exc:
        if _status_code(exc) == 409:
            say(f"  branch '{cfg.branch}' already exists - reusing")
        else:
            logger.exception("Branch pre-creation failed")
            say(f"  !! could not pre-create branch: {exc}")


# ═════════════════════════════════════════════════════════════════════════════
# STEP 2 - Upload with DCAT annotation
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class UploadOutcome:
    dataset: RawDataset
    dataset_id: str | None = None
    status: str = "pending"
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "uploaded"


def upload_datasets(
    client: CESDHClient, cfg: Config, datasets: list[RawDataset], run_id: str
) -> list[UploadOutcome]:
    """Upload each staged CSV with its DCAT metadata; never abort on one failure."""
    outcomes: list[UploadOutcome] = []

    for dataset in datasets:
        outcome = UploadOutcome(dataset=dataset)
        try:
            result = client.upload_model(
                str(dataset.path),
                owner=cfg.owner,
                branch=cfg.branch,
                version="1.0",
                description=dataset.dcat_description(),
                # Raw measurements are not CESDM system models: Tier 1 stores the
                # bytes and skips schema validation instead of failing them.
                force_tier="generic",
                # Becomes dcat:keyword - this is what country filtering matches.
                scenario_metadata=dataset.scenario_metadata(),
                # First-class DCAT/QUDT predicates - exact, typed, queryable.
                dcat_metadata=dataset.dcat_metadata(),
                force=True,  # re-runs supersede rather than collide
                run_id=run_id,
                commit_message=f"{run_id}: stage {dataset.filename} ({dataset.provenance})",
            )
            status = result.get("status", "success")
            outcome.dataset_id = result.get("dataset_id")
            if status == "validation_failed" or result.get("validation_hard_errors"):
                errs = result.get("validation_hard_errors") or []
                outcome.status = "rejected"
                outcome.detail = errs[0] if errs else "validation failed"
                say(f"  !! rejected {dataset.filename}: {outcome.detail[:60]}")
            else:
                outcome.status = "uploaded"
                say(
                    f"  OK {dataset.filename:38s} "
                    f"{human_bytes(dataset.path.stat().st_size):>9s} -> {outcome.dataset_id}"
                )
        except Exception as exc:
            logger.exception("Upload failed for %s", dataset.filename)
            outcome.status = "failed"
            outcome.detail = str(exc)
            say(f"  !! failed   {dataset.filename}: {str(exc)[:60]}")
        outcomes.append(outcome)

    return outcomes


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3 - Programmatic search over DCAT attributes
# ═════════════════════════════════════════════════════════════════════════════


def search_catalog(
    client: CESDHClient, cfg: Config, country: str, carrier: str
) -> list[dict[str, Any]]:
    """Find datasets matching country AND carrier, deterministically.

    Both filters are exact equality on typed predicates - energy:energyCarrier
    and dcterms:spatial - rather than substring matching on free text. That
    removes the old failure mode where a description merely *mentioning* solar
    matched a query for solar datasets.

    The subject is named ?dataset deliberately: the gateway splices
    `?dataset energy:inRepository "…"` into every catalog GRAPH block, and an
    unbound variable there would cross-join every dataset in the project.
    """
    query = f"""
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX energy:  <http://energy.ethz.ch/schema#>
PREFIX qudt:    <http://qudt.org/schema/qudt/>
SELECT DISTINCT ?dataset_id ?title ?carrier ?unit ?resolution ?licence WHERE {{
  GRAPH <http://energy.ethz.ch/graph/catalog> {{
    ?dataset a dcat:Dataset ;
             dcterms:identifier ?dataset_id ;
             dcat:title ?title ;
             energy:energyCarrier ?carrier ;
             dcterms:spatial ?spatial ;
             energy:lakefsBranch "{cfg.branch}" .
    OPTIONAL {{ ?dataset qudt:hasUnit ?unit . }}
    OPTIONAL {{ ?dataset dcat:temporalResolution ?resolution . }}
    OPTIONAL {{ ?dataset dcterms:license ?licence . }}
    FILTER(LCASE(?carrier) = "{carrier.lower()}")
    FILTER(LCASE(?spatial) = "{country.lower()}")
    FILTER NOT EXISTS {{ ?dataset energy:supersededBy ?newer . }}
  }}
}} ORDER BY ?title"""
    try:
        return client.sparql(query).get("results", []) or []
    except Exception as exc:
        logger.exception("Catalog search failed")
        say(f"  !! search failed: {exc}")
        return []


def natural_language_search(client: CESDHClient, cfg: Config, question: str) -> int:
    """Same question through the LLM translator; best-effort by nature.

    A local 8B model does not always emit valid, repository-scopable SPARQL, so a
    failure here is reported rather than allowed to end the run.
    """
    say(f"  NL query: {question!r}")
    try:
        hits = (
            client.search(question, limit=5, timeout=cfg.nl_search_timeout).get(
                "results", []
            )
            or []
        )
        say(f"    {len(hits)} record(s):")
        for hit in hits:
            say(f"      - {hit.get('title') or hit.get('dataset')}")
        return len(hits)
    except Exception as exc:
        say(f"    unavailable this run: {str(exc)[:64]}")
        say("    (the deterministic query above is the reliable path)")
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# STEP 4 - Fetch back and verify
# ═════════════════════════════════════════════════════════════════════════════


def download_and_verify(
    client: CESDHClient, cfg: Config, dataset_id: str, expected: RawDataset
) -> bool:
    """Download one dataset into the clean workspace and check it byte-for-byte."""
    cfg.download_dir.mkdir(parents=True, exist_ok=True)
    destination = cfg.download_dir / expected.filename
    try:
        client.download_to_file(dataset_id, str(destination))
    except Exception as exc:
        logger.exception("Download failed for %s", dataset_id)
        say(f"  !! download failed: {exc}")
        return False

    got = hashlib.sha256(destination.read_bytes()).hexdigest()
    match = got == expected.sha256
    say(f"  saved     {destination.relative_to(REPO_ROOT)}")
    say(f"  staged    sha256 = {expected.sha256}")
    say(f"  retrieved sha256 = {got}")
    say(f"  {'round-trip is lossless' if match else '!! CHECKSUM MISMATCH'}")
    return match


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("RAW_HUB_LOG_LEVEL", "WARNING"),
        format="%(levelname)s %(name)s: %(message)s",
    )
    cfg = Config()
    timings: dict[str, float] = {}
    t_total = time.perf_counter()

    banner("ESDH raw data lifecycle - setup")
    say(f"  gateway     {cfg.gateway_url}")
    say(f"  repository  {cfg.repository}")
    say(f"  branch      {cfg.branch}")
    say(f"  staging     {cfg.staging_dir.relative_to(REPO_ROOT)}")
    say(f"  downloads   {cfg.download_dir.relative_to(REPO_ROOT)}")
    say(
        f"  mode        {'OFFLINE (synthetic only)' if cfg.offline else 'live fetch'}"
        f", cap {human_bytes(cfg.max_fetch_bytes)}/source"
    )

    banner("PART 1 - Stage open datasets")
    t0 = time.perf_counter()
    datasets = stage_datasets(cfg)
    timings["stage"] = time.perf_counter() - t0
    for dataset in datasets:
        say(
            f"  {dataset.filename:38s} {dataset.rows:>6,} rows  {dataset.provenance[:44]}"
        )

    client = CESDHClient(
        repository=cfg.repository,
        gateway_url=cfg.gateway_url,
        lakefs_url=cfg.lakefs_url,
    )

    banner("STEP 1 - Upload & store: repository and research branch")
    t0 = time.perf_counter()
    ensure_repository_and_branch(client, cfg)
    timings["setup"] = time.perf_counter() - t0

    banner("STEP 2 - Upload with DCAT annotation")
    run_id = f"raw_hub_{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    t0 = time.perf_counter()
    outcomes = upload_datasets(client, cfg, datasets, run_id)
    timings["upload"] = time.perf_counter() - t0
    uploaded = [o for o in outcomes if o.ok]
    if not uploaded:
        say("  !! nothing was stored - cannot search or fetch. Is the stack up?")
        return 1
    say("  DCAT description attached to each dataset, e.g.:")
    say(f"    {uploaded[0].dataset.dcat_description()[:110]}...")

    banner("STEP 3 - Programmatic search over the DCAT catalog")
    t0 = time.perf_counter()
    say("  deterministic query: country = CH  AND  carrier = solar")
    hits = search_catalog(client, cfg, "CH", "solar")
    say(f"    {len(hits)} hit(s):")
    for hit in hits:
        say(
            f"      - {hit.get('title', '')[:52]}  "
            f"[{hit.get('carrier', '?')} · {hit.get('unit', '?')} · {hit.get('resolution', '?')}]"
        )

    say("")
    say("  control query: country = DE  AND  carrier = wind")
    de_hits = search_catalog(client, cfg, "DE", "wind")
    say(f"    {len(de_hits)} hit(s)")

    say("")
    say("  negative control: country = DE  AND  carrier = solar (expect 0)")
    none_hits = search_catalog(client, cfg, "DE", "solar")
    say(f"    {len(none_hits)} hit(s)")

    say("")
    nl_hits = natural_language_search(
        client, cfg, "solar irradiation datasets for Switzerland"
    )
    timings["search"] = time.perf_counter() - t0

    banner("STEP 4 - Fetch a selected dataset back")
    t0 = time.perf_counter()
    if hits:
        chosen_id = hits[0]["dataset_id"]
        chosen = next(
            (o.dataset for o in uploaded if o.dataset_id == chosen_id),
            uploaded[0].dataset,
        )
    else:
        say("  search returned nothing - falling back to the first upload")
        chosen_id, chosen = uploaded[0].dataset_id or "", uploaded[0].dataset
    say(f"  selected {chosen.filename} ({chosen_id})")
    round_trip_ok = download_and_verify(client, cfg, chosen_id, chosen)
    timings["fetch"] = time.perf_counter() - t0

    # ── Summary ──────────────────────────────────────────────────────────────
    banner("SUMMARY")
    live = sum(1 for d in datasets if d.is_live)
    total_rows = sum(d.rows for d in datasets)
    total_bytes = sum(d.path.stat().st_size for d in datasets if d.path)

    rows = [
        ("repository", cfg.repository),
        ("branch", cfg.branch),
        (
            "datasets staged",
            f"{len(datasets)} ({live} live-fetched, {len(datasets) - live} synthetic)",
        ),
        ("time-series rows", f"{total_rows:,}"),
        ("bytes staged", f"{total_bytes:,} ({human_bytes(total_bytes)})"),
        ("uploaded", f"{len(uploaded)} / {len(outcomes)}"),
        ("search CH+solar", f"{len(hits)} hit(s)"),
        ("search DE+wind", f"{len(de_hits)} hit(s)"),
        ("search DE+solar", f"{len(none_hits)} hit(s) (negative control)"),
        ("NL search", f"{nl_hits} hit(s)"),
        ("round-trip SHA-256", "OK - lossless" if round_trip_ok else "FAILED"),
    ]
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        say(f"  {key.ljust(width)} : {value}")

    say("")
    say("  timings")
    for phase in ("stage", "setup", "upload", "search", "fetch"):
        say(f"    {phase.ljust(8)} {timings.get(phase, 0.0):6.2f}s")
    say(f"    {'total'.ljust(8)} {time.perf_counter() - t_total:6.2f}s")

    say("")
    say("  provenance of every staged file")
    for dataset in datasets:
        say(f"    {dataset.filename:38s} {dataset.provenance}")

    checks_ok = (
        bool(uploaded) and round_trip_ok and len(hits) >= 1 and len(none_hits) == 0
    )
    say("")
    say(f"  verification: {'ALL CHECKS PASSED' if checks_ok else 'SOME CHECKS FAILED'}")
    return 0 if checks_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
