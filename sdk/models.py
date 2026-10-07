"""Multi-level metadata models for enriched dataset search.

These dataclasses describe one search hit as a *hierarchy* rather than a flat
row, so a caller can see - before querying anything further - which project a
dataset belongs to, which branch it was produced on, what shape it has, and
whether it can be trusted.

Why dataclasses and not Pydantic
--------------------------------
`cesdh` declares only pandas / h5py / requests / pyyaml / openpyxl / pyarrow
(``src/sdk/pyproject.toml``). Adding Pydantic would push a new runtime
dependency onto every SDK consumer for what is, here, pure data transport.
Stdlib dataclasses give the same ergonomics for this use.

Vocabulary mapping
------------------
The five levels below are the ESDH equivalents of a generic catalogue's
platform / environment / entity / schema / quality tiers. Each maps onto RDF
this platform already writes (see ``src/gateway/services/rdf_builder.py``):

===================  ==========================================================
Level                Source
===================  ==========================================================
RepositoryLevel      ``energy:repository/{repo}`` (dcat:Catalog node)
BranchLevel          ``energy:branch/{repo}/{branch}`` (energy:Branch node)
DatasetLevel         ``dcat:Dataset`` node in the catalog graph
SchemaLevel          ``energy:containsEntityClass`` / ``energy:entityCount``
QualitySummary       ``energy:validationStatus``, ``energy:checksum``,
                     ``energy:supersededBy``, ``energy:lakefsCommitId``
===================  ==========================================================

Every field degrades to ``"N/A"`` or ``"Not Configured"`` rather than raising,
because a repository that has never had ``set_repository_metadata()`` called on
it is a normal state, not an error.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ── Sentinels ────────────────────────────────────────────────────────────────
# Two distinct "missing" meanings, kept apart because they need different UI
# treatment: NA means "this level has no such fact", NOT_CONFIGURED means "an
# operator never filled this in and could".
NA = "N/A"
NOT_CONFIGURED = "Not Configured"

# ── Environment classification ───────────────────────────────────────────────
# ESDH has no environment tag the way a warehouse catalogue does. What it has is
# LakeFS branches, and branch role is conventional: `main` holds curated,
# citable data; experiment/reanalysis branches hold work in progress. Deriving
# the environment from the branch name keeps the model honest - it is an
# inference, and `environment_inferred` records that it was one.
PRODUCTION_BRANCHES = frozenset({"main", "master", "prod", "production"})
STAGING_PREFIXES = ("staging", "stg-", "stage-", "release-", "rc-")

ENV_PRODUCTION = "PROD"
ENV_STAGING = "STG"
ENV_DEVELOPMENT = "DEV"


def classify_environment(branch: str) -> str:
    """Map a LakeFS branch name onto a PROD / STG / DEV environment label.

    `main` is the curated trunk every published release is tagged on, so it is
    the only thing treated as production by default. Everything else is an
    experiment until proven otherwise - the safe direction to be wrong in.
    """
    if not branch:
        return NA
    ref = branch.strip().lower()
    if ref in PRODUCTION_BRANCHES:
        return ENV_PRODUCTION
    if ref.startswith(STAGING_PREFIXES):
        return ENV_STAGING
    return ENV_DEVELOPMENT


def _clean(value: Optional[Any], default: str = NA) -> str:
    """Normalise a possibly-missing SPARQL literal to a display string."""
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _as_int(value: Optional[Any]) -> Optional[int]:
    """Parse an integer literal, returning None rather than raising."""
    if value is None or value == "":
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


# ── Level 1: Platform / Repository ───────────────────────────────────────────
@dataclass
class RepositoryLevel:
    """Which project holds this dataset, and how that project describes itself."""

    repository: str
    # ESDH's storage substrate is fixed, unlike a multi-platform catalogue where
    # this would vary per asset (Snowflake / BigQuery / S3).
    platform: str = "LakeFS over S3 (MinIO)"
    title: str = NOT_CONFIGURED
    description: str = NOT_CONFIGURED
    owner: str = NA
    publisher: str = NA
    funding_program: str = NA
    themes: List[str] = field(default_factory=list)
    energy_carriers: List[str] = field(default_factory=list)
    spatial_coverage: List[str] = field(default_factory=list)
    model_frameworks: List[str] = field(default_factory=list)
    #: False when no ``energy:repository/{repo}`` node exists yet.
    configured: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "repository": self.repository,
            "platform": self.platform,
            "title": self.title,
            "description": self.description,
            "owner": self.owner,
            "publisher": self.publisher,
            "funding_program": self.funding_program,
            "themes": list(self.themes),
            "energy_carriers": list(self.energy_carriers),
            "spatial_coverage": list(self.spatial_coverage),
            "model_frameworks": list(self.model_frameworks),
            "configured": self.configured,
        }


# ── Level 2: Branch / Environment ────────────────────────────────────────────
@dataclass
class BranchLevel:
    """Which LakeFS branch produced this dataset, and what that branch is for."""

    branch: str
    environment: str = NA
    #: True when `environment` was derived from the branch name rather than
    #: declared. Callers that must not guess should check this.
    environment_inferred: bool = True
    title: str = NOT_CONFIGURED
    description: str = NOT_CONFIGURED
    owner: str = NA
    target_year: str = NA
    hypothesis: str = NA
    themes: List[str] = field(default_factory=list)
    energy_carriers: List[str] = field(default_factory=list)
    #: False when no ``energy:branch/{repo}/{branch}`` node exists yet.
    configured: bool = False

    @property
    def is_production(self) -> bool:
        """True for the curated trunk - drives the UI's PROD/non-PROD styling."""
        return self.environment == ENV_PRODUCTION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "branch": self.branch,
            "environment": self.environment,
            "environment_inferred": self.environment_inferred,
            "is_production": self.is_production,
            "title": self.title,
            "description": self.description,
            "owner": self.owner,
            "target_year": self.target_year,
            "hypothesis": self.hypothesis,
            "themes": list(self.themes),
            "energy_carriers": list(self.energy_carriers),
            "configured": self.configured,
        }


# ── Level 3: Dataset ─────────────────────────────────────────────────────────
@dataclass
class DatasetLevel:
    """The asset itself: identity, description and discovery facets."""

    dataset_id: str
    file_name: str = NA
    title: str = NA
    description: str = NA
    file_format: str = NA
    version: str = NA
    owner: str = NA
    issued: str = NA
    modified: str = NA
    keywords: List[str] = field(default_factory=list)
    themes: List[str] = field(default_factory=list)
    energy_carriers: List[str] = field(default_factory=list)
    #: Scenario ids this dataset is bound to via ``energy:belongsToScenario``.
    scenarios: List[str] = field(default_factory=list)
    #: Scenario id this dataset *defines* (Tier 3 blueprints only).
    defines_scenario: str = NA
    storage_uri: str = NA

    @property
    def urn(self) -> str:
        """Stable, repository-scoped identifier for this dataset.

        Mirrors ``rdf_builder.build_repo_aware_uri`` so the value printed by the
        SDK is the same IRI the catalog stores, not a second invented scheme.
        """
        return f"esdh://{self.dataset_id}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "urn": self.urn,
            "file_name": self.file_name,
            "title": self.title,
            "description": self.description,
            "format": self.file_format,
            "version": self.version,
            "owner": self.owner,
            "issued": self.issued,
            "modified": self.modified,
            "keywords": list(self.keywords),
            "themes": list(self.themes),
            "energy_carriers": list(self.energy_carriers),
            "scenarios": list(self.scenarios),
            "defines_scenario": self.defines_scenario,
            "storage_uri": self.storage_uri,
        }


# ── Level 4: Schema & Columns ────────────────────────────────────────────────
@dataclass
class SchemaLevel:
    """Structural shape: CESDM entity classes standing in for columns.

    A CESDM asset's structure is a set of typed entity classes, not a column
    list, so `entity_classes` is the closest true analogue of a schema. For
    Tier 1 generic assets it is legitimately empty.
    """

    entity_classes: List[str] = field(default_factory=list)
    entity_count: Optional[int] = None
    conforms_to: str = NA
    #: Tier 1 generic / Tier 2 CESDM-aligned / Tier 3 scenario blueprint.
    tier: str = NA

    @property
    def class_count(self) -> int:
        return len(self.entity_classes)

    @property
    def has_schema(self) -> bool:
        return bool(self.entity_classes) or bool(self.entity_count)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_classes": list(self.entity_classes),
            "class_count": self.class_count,
            "entity_count": self.entity_count if self.entity_count is not None else NA,
            "conforms_to": self.conforms_to,
            "tier": self.tier,
            "has_schema": self.has_schema,
        }


# ── Level 5: Quality & Governance ────────────────────────────────────────────
@dataclass
class QualitySummary:
    """Trust signals, expressed as a small set of checks that pass or fail.

    ESDH has no assertion framework. What it does have are four facts the
    ingestion pipeline records on every upload, each of which is a real,
    checkable claim. Presenting them as a pass rate gives the same at-a-glance
    signal without inventing data:

    1. **validated**  - CESDM validation did not fail
    2. **checksummed** - a SHA-256 was recorded, so drift is detectable
    3. **committed**   - the bytes reached a LakeFS commit, not just staging
    4. **current**     - no ``energy:supersededBy`` pointer to a newer revision

    `checks` keeps the individual outcomes so a UI can explain the score rather
    than only display it.
    """

    validation_status: str = NA
    validation_warning_count: Optional[int] = None
    checksum: str = NA
    file_size_bytes: Optional[int] = None
    commit_id: str = NA
    owner: str = NA
    last_modified: str = NA
    superseded: bool = False
    checks: Dict[str, bool] = field(default_factory=dict)

    @property
    def checks_total(self) -> int:
        return len(self.checks)

    @property
    def checks_passed(self) -> int:
        return sum(1 for ok in self.checks.values() if ok)

    @property
    def pass_rate(self) -> Optional[float]:
        """Fraction of checks passed, or None when nothing could be checked."""
        if not self.checks:
            return None
        return self.checks_passed / len(self.checks)

    @property
    def pass_rate_display(self) -> str:
        rate = self.pass_rate
        return NA if rate is None else "{:.0%}".format(rate)

    @property
    def status_label(self) -> str:
        """Coarse health label for badge rendering."""
        rate = self.pass_rate
        if rate is None:
            return NOT_CONFIGURED
        if self.superseded:
            return "Superseded"
        if rate == 1.0:
            return "Passing"
        if rate >= 0.5:
            return "Warnings"
        return "Failing"

    @property
    def file_size_display(self) -> str:
        """Human-readable size, or N/A when the catalog holds no fileSize."""
        size = self.file_size_bytes
        if size is None:
            return NA
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024 or unit == "GB":
                return "{:.0f} {}".format(size, unit) if unit == "B" else "{:.1f} {}".format(size, unit)
            size /= 1024.0
        return NA

    def to_dict(self) -> Dict[str, Any]:
        return {
            "validation_status": self.validation_status,
            "validation_warning_count": (
                self.validation_warning_count
                if self.validation_warning_count is not None
                else NA
            ),
            "checksum": self.checksum,
            "file_size_bytes": self.file_size_bytes,
            "file_size_display": self.file_size_display,
            "commit_id": self.commit_id,
            "owner": self.owner,
            "last_modified": self.last_modified,
            "superseded": self.superseded,
            "checks": dict(self.checks),
            "checks_passed": self.checks_passed,
            "checks_total": self.checks_total,
            "pass_rate": self.pass_rate,
            "pass_rate_display": self.pass_rate_display,
            "status_label": self.status_label,
        }


# ── Composition ──────────────────────────────────────────────────────────────
@dataclass
class MetadataHierarchy:
    """The four structural levels of one search hit, innermost last."""

    repository: RepositoryLevel
    branch: BranchLevel
    dataset: DatasetLevel
    schema: SchemaLevel

    @property
    def breadcrumb(self) -> str:
        """``repository / branch (ENV) > dataset`` - the UI's top line."""
        return "{} / {} ({}) > {}".format(
            self.repository.repository,
            self.branch.branch,
            self.branch.environment,
            self.dataset.file_name
            if self.dataset.file_name != NA
            else self.dataset.dataset_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "repository": self.repository.to_dict(),
            "branch": self.branch.to_dict(),
            "dataset": self.dataset.to_dict(),
            "schema": self.schema.to_dict(),
            "breadcrumb": self.breadcrumb,
        }


@dataclass
class EnhancedSearchResult:
    """One search hit, resolved across every metadata level.

    This is what ``search_datasets_with_summary()`` returns. It is deliberately
    self-contained: everything a result card needs to render is present, so the
    UI never has to issue a follow-up request per row.
    """

    hierarchy: MetadataHierarchy
    quality: QualitySummary

    # Convenience passthroughs so callers need not reach through two levels
    # for the fields they use most.
    @property
    def dataset_id(self) -> str:
        return self.hierarchy.dataset.dataset_id

    @property
    def title(self) -> str:
        return self.hierarchy.dataset.title

    @property
    def breadcrumb(self) -> str:
        return self.hierarchy.breadcrumb

    @property
    def is_production(self) -> bool:
        return self.hierarchy.branch.is_production

    def to_dict(self) -> Dict[str, Any]:
        """Flatten to plain JSON-safe types (for st.json, APIs, or a DataFrame)."""
        return {
            "dataset_id": self.dataset_id,
            "breadcrumb": self.breadcrumb,
            "hierarchy": self.hierarchy.to_dict(),
            "quality": self.quality.to_dict(),
        }

    def to_row(self) -> Dict[str, Any]:
        """Flat one-line summary, suitable for a pandas DataFrame."""
        ds = self.hierarchy.dataset
        return {
            "dataset_id": ds.dataset_id,
            "title": ds.title,
            "file_name": ds.file_name,
            "format": ds.file_format,
            "repository": self.hierarchy.repository.repository,
            "branch": self.hierarchy.branch.branch,
            "environment": self.hierarchy.branch.environment,
            "owner": ds.owner,
            "entity_classes": self.hierarchy.schema.class_count,
            "quality": self.quality.status_label,
            "pass_rate": self.quality.pass_rate_display,
            "size": self.quality.file_size_display,
            "issued": ds.issued,
        }


__all__ = [
    "NA",
    "NOT_CONFIGURED",
    "ENV_PRODUCTION",
    "ENV_STAGING",
    "ENV_DEVELOPMENT",
    "classify_environment",
    "RepositoryLevel",
    "BranchLevel",
    "DatasetLevel",
    "SchemaLevel",
    "QualitySummary",
    "MetadataHierarchy",
    "EnhancedSearchResult",
]
