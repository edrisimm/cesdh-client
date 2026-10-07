"""Pluggable metadata backends for enriched search.

The SDK exposes one search contract - :meth:`MetadataProvider.search_with_summary`
- and resolves it against whichever catalogue backs the platform. Today that is
ESDH's own Fuseki triple store (:class:`EsdhMetadataProvider`). The interface
exists so a different catalogue can be added later without changing
``EnergyDLMClient``, the ``cesdh`` functional API, or any caller.

Adding a second backend
-----------------------
Subclass :class:`MetadataProvider`, implement ``search_with_summary`` so it
returns the same :class:`~.models.EnhancedSearchResult` objects, and pass an
instance as ``provider=`` to ``EnergyDLMClient``. A DataHub-backed provider, for
example, would translate the same ``filters`` dict into a GraphQL
``searchAcrossEntities`` call requesting ``datasetProperties``, ``ownership``,
``globalTags``, ``glossaryTerms``, ``schemaMetadata`` and ``assertions`` in one
request, then map those aspects onto the same five levels::

    class DataHubMetadataProvider(MetadataProvider):
        def __init__(self, graph):          # a DataHubGraph instance
            self._graph = graph
        def search_with_summary(self, query, filters=None, limit=25):
            ...                             # one batched GraphQL call
            return [EnhancedSearchResult(...), ...]

Nothing downstream changes, because callers depend on the dataclasses rather
than on where the facts came from.

Request budget
--------------
:class:`EsdhMetadataProvider` answers a search in a **fixed** number of round
trips, independent of how many datasets match:

1. one SPARQL query for every dataset-level, schema-level and quality-level
   fact, with multi-valued fields folded via ``GROUP_CONCAT`` so a dataset with
   nine keywords stays one row instead of fanning out into nine;
2. one REST call for the repository node;
3. one REST call per *distinct branch present in the results* (typically one or
   two), memoised for the lifetime of the call.

That is the N+1 avoidance the batched-GraphQL requirement is really asking for.
These are blocking ``requests`` calls because the rest of the SDK is
synchronous; introducing asyncio here would force every caller into an event
loop for a saving of two or three requests.
"""

from __future__ import annotations

import abc
from typing import Any, Dict, Iterable, List, Optional

from .models import (
    NA,
    NOT_CONFIGURED,
    BranchLevel,
    DatasetLevel,
    EnhancedSearchResult,
    MetadataHierarchy,
    QualitySummary,
    RepositoryLevel,
    SchemaLevel,
    classify_environment,
)

# GROUP_CONCAT joiner. A pipe cannot appear in a CESDM entity-class name, a
# LakeFS branch name or a DCAT theme, so it is safe to split on.
_SEP = "|"

_CATALOG_GRAPH = "http://energy.ethz.ch/graph/catalog"

# Fields a free-text term is matched against.
#
# Two scoping rules are load-bearing here:
#  * `?ownerIri` - not `?owner` - because these FILTERs live inside the GRAPH
#    block while `?owner` is BIND-ed outside it. A filter can only see variables
#    bound in its own group, so matching `?owner` here would silently never fire.
#    The IRI embeds the owner name ("...#agent_researcher-a"), so substring
#    matching still behaves as the caller expects.
#  * `?keyword` is OPTIONAL, but SPARQL logical-or yields true when either side
#    is true even if the other errors on an unbound variable, so including it
#    cannot drop keyword-less datasets.
_TERM_FIELDS = ("?title", "?file_name", "?format", "?branch", "?ownerIri", "?keyword")

# Facet name -> triple pattern template. Values are escaped before formatting.
_FACET_PATTERNS: Dict[str, str] = {
    "branch": '?dataset energy:lakefsBranch "{}" .',
    "format": '?dataset dcat:format "{}" .',
    "entity_class": '?dataset energy:containsEntityClass "{}" .',
    "theme": '?dataset dcat:theme "{}" .',
    "energy_carrier": '?dataset energy:energyCarrier "{}" .',
}


def _sparql_literal(value: str) -> str:
    """Escape a caller-supplied string for interpolation into a SPARQL literal."""
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _split(value: Optional[str]) -> List[str]:
    """Split a GROUP_CONCAT result into a de-duplicated, ordered list."""
    if not value:
        return []
    seen: List[str] = []
    for part in str(value).split(_SEP):
        item = part.strip()
        if item and item not in seen:
            seen.append(item)
    return seen


def _text(row: Dict[str, Any], key: str, default: str = NA) -> str:
    raw = row.get(key)
    if raw is None:
        return default
    text = str(raw).strip()
    return text if text else default


def _number(row: Dict[str, Any], key: str) -> Optional[int]:
    raw = row.get(key)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(float(str(raw)))
    except (TypeError, ValueError):
        return None


class MetadataProvider(abc.ABC):
    """Contract every catalogue backend must satisfy."""

    @abc.abstractmethod
    def search_with_summary(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 25,
    ) -> List[EnhancedSearchResult]:
        """Return hits resolved across repository / branch / dataset / schema / quality."""
        raise NotImplementedError


class EsdhMetadataProvider(MetadataProvider):
    """Resolves enriched search against ESDH's Fuseki catalog and gateway REST API.

    Parameters
    ----------
    client:
        Anything exposing ``repository``, ``sparql()``, ``get_repository_metadata()``
        and ``get_branch_metadata()`` - in practice an ``EnergyDLMClient``. Taken
        as a collaborator rather than constructed here so the provider stays
        testable without a live gateway.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    # ── Public API ───────────────────────────────────────────────────────────
    def search_with_summary(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 25,
    ) -> List[EnhancedSearchResult]:
        filters = dict(filters or {})

        # Over-fetch when a post-hoc facet is active: `environment` and
        # `quality` are derived values with no RDF predicate to filter on, so
        # they are applied in Python and would otherwise silently shrink a page.
        post_facets = [f for f in ("environment", "quality") if filters.get(f)]
        fetch_limit = min(limit * 4, 400) if post_facets else limit

        rows = self._client.sparql(
            self._build_query(query, filters, fetch_limit)
        ).get("results", [])

        repository = self._repository_level()
        branch_cache: Dict[str, BranchLevel] = {}

        results: List[EnhancedSearchResult] = []
        for row in rows:
            branch_name = _text(row, "branch", "")
            if branch_name not in branch_cache:
                branch_cache[branch_name] = self._branch_level(branch_name)
            result = self._to_result(row, repository, branch_cache[branch_name])
            if self._passes_post_filters(result, filters):
                results.append(result)
            if len(results) >= limit:
                break
        return results

    # ── Query construction ───────────────────────────────────────────────────
    def _build_query(
        self, query: str, filters: Dict[str, Any], limit: int
    ) -> str:
        """Assemble the single batched SELECT.

        The dataset variable **must** be named ``?dataset``: the gateway injects
        ``?dataset energy:inRepository "<repo>"`` into every catalog GRAPH block
        (``knowledge_graph.indexer.inject_repo_filter``). For the same reason no
        ``energy:inRepository`` clause is written here - doing so would scope the
        query twice.
        """
        clauses: List[str] = []

        # Free-text terms: every term must match at least one field.
        for term in [t for t in str(query or "").lower().split() if t][:6]:
            esc = _sparql_literal(term)
            ors = " || ".join(
                'CONTAINS(LCASE(STR({})), "{}")'.format(f, esc) for f in _TERM_FIELDS
            )
            clauses.append("    FILTER({})".format(ors))

        # Structural facets that map directly onto a predicate.
        for key, pattern in _FACET_PATTERNS.items():
            value = filters.get(key)
            if value:
                clauses.append("    " + pattern.format(_sparql_literal(value)))

        if filters.get("owner"):
            # ?ownerIri, not ?owner - see the note on _TERM_FIELDS.
            clauses.append(
                '    FILTER(CONTAINS(LCASE(STR(?ownerIri)), "{}"))'.format(
                    _sparql_literal(str(filters["owner"]).lower())
                )
            )
        if filters.get("scenario"):
            clauses.append(
                '    ?dataset energy:belongsToScenario energy:scenario_{} .'.format(
                    _sparql_literal(filters["scenario"])
                )
            )

        # Lifecycle: superseded revisions are hidden unless explicitly asked for,
        # matching the governance rule the rest of the SDK follows.
        if not filters.get("include_superseded"):
            clauses.append(
                "    FILTER NOT EXISTS { ?dataset energy:supersededBy ?newer . }"
            )

        grouped = [
            "?dataset_id", "?file_name", "?title", "?description", "?format",
            "?version", "?issued", "?modified", "?branch", "?owner", "?checksum",
            "?fileSize", "?entityCount", "?validationStatus", "?warningCount",
            "?commitId", "?storedIn", "?definesScenario",
        ]

        return """PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX prov:    <http://www.w3.org/ns/prov#>
PREFIX energy:  <http://energy.ethz.ch/schema#>

SELECT {projection}
       (GROUP_CONCAT(DISTINCT ?keyword;     separator="{sep}") AS ?keywords)
       (GROUP_CONCAT(DISTINCT ?entityClass; separator="{sep}") AS ?entityClasses)
       (GROUP_CONCAT(DISTINCT ?scenarioId;  separator="{sep}") AS ?scenarioIds)
       (GROUP_CONCAT(DISTINCT ?theme;       separator="{sep}") AS ?themes)
       (GROUP_CONCAT(DISTINCT ?carrier;     separator="{sep}") AS ?carriers)
WHERE {{
  GRAPH <{graph}> {{
    ?dataset a dcat:Dataset ;
             dcterms:identifier ?dataset_id ;
             energy:fileName ?file_name ;
             dcat:title ?title ;
             dcat:format ?format ;
             dcat:issued ?issued ;
             energy:lakefsBranch ?branch ;
             prov:wasAttributedTo ?ownerIri .
    OPTIONAL {{ ?dataset dcat:description ?description . }}
    OPTIONAL {{ ?dataset dcat:version ?version . }}
    OPTIONAL {{ ?dataset dcat:modified ?modified . }}
    OPTIONAL {{ ?dataset energy:checksum ?checksum . }}
    OPTIONAL {{ ?dataset energy:fileSize ?fileSize . }}
    OPTIONAL {{ ?dataset energy:entityCount ?entityCount . }}
    OPTIONAL {{ ?dataset energy:validationStatus ?validationStatus . }}
    OPTIONAL {{ ?dataset energy:validationWarningCount ?warningCount . }}
    OPTIONAL {{ ?dataset energy:lakefsCommitId ?commitId . }}
    OPTIONAL {{ ?dataset energy:storedIn ?storedIn . }}
    OPTIONAL {{ ?dataset energy:definesScenario ?definesScenario . }}
    OPTIONAL {{ ?dataset dcat:keyword ?keyword . }}
    OPTIONAL {{ ?dataset energy:containsEntityClass ?entityClass . }}
    OPTIONAL {{ ?dataset dcat:theme ?theme . }}
    OPTIONAL {{ ?dataset energy:energyCarrier ?carrier . }}
    OPTIONAL {{ ?dataset energy:belongsToScenario ?scenarioIri . }}
{clauses}
  }}
  BIND(STRAFTER(STR(?ownerIri), "agent_") AS ?owner)
  BIND(STRAFTER(STR(?scenarioIri), "scenario_") AS ?scenarioId)
}}
GROUP BY {projection}
ORDER BY DESC(?issued)
LIMIT {limit}""".format(
            projection=" ".join(grouped),
            sep=_SEP,
            graph=_CATALOG_GRAPH,
            clauses="\n".join(clauses),
            limit=int(limit),
        )

    # ── Level assembly ───────────────────────────────────────────────────────
    def _repository_level(self) -> RepositoryLevel:
        """Read the repository node, degrading to 'Not Configured' if absent."""
        repo = getattr(self._client, "repository", NA)
        level = RepositoryLevel(repository=repo)
        try:
            meta = self._client.get_repository_metadata() or {}
        except Exception:
            # A repository with no metadata node is a normal, expected state -
            # it means nobody has called set_repository_metadata() yet.
            return level
        if not meta:
            return level
        level.configured = True
        level.title = meta.get("title") or NOT_CONFIGURED
        level.description = meta.get("description") or NOT_CONFIGURED
        level.owner = meta.get("owner") or NA
        level.publisher = meta.get("publisher") or NA
        level.funding_program = meta.get("funding_program") or NA
        level.themes = list(meta.get("themes") or [])
        level.energy_carriers = list(meta.get("energy_carriers") or [])
        level.spatial_coverage = _as_list(meta.get("spatial_coverage"))
        level.model_frameworks = list(meta.get("model_frameworks") or [])
        return level

    def _branch_level(self, branch: str) -> BranchLevel:
        """Read one branch node; the environment is inferred from its name."""
        level = BranchLevel(
            branch=branch or NA,
            environment=classify_environment(branch),
            environment_inferred=True,
        )
        if not branch:
            return level
        try:
            meta = self._client.get_branch_metadata(branch) or {}
        except Exception:
            return level
        if not meta:
            return level
        level.configured = True
        level.title = meta.get("title") or NOT_CONFIGURED
        level.description = meta.get("description") or NOT_CONFIGURED
        level.owner = meta.get("owner") or NA
        level.target_year = str(meta.get("target_year") or NA)
        level.hypothesis = meta.get("hypothesis") or NA
        level.themes = list(meta.get("themes") or [])
        level.energy_carriers = list(meta.get("energy_carriers") or [])
        return level

    def _to_result(
        self, row: Dict[str, Any], repository: RepositoryLevel, branch: BranchLevel
    ) -> EnhancedSearchResult:
        entity_classes = _split(row.get("entityClasses"))
        defines_scenario = _text(row, "definesScenario")

        dataset = DatasetLevel(
            dataset_id=_text(row, "dataset_id", ""),
            file_name=_text(row, "file_name"),
            title=_text(row, "title"),
            description=_text(row, "description"),
            file_format=_text(row, "format"),
            version=_text(row, "version"),
            owner=_text(row, "owner"),
            issued=_text(row, "issued"),
            modified=_text(row, "modified"),
            keywords=_split(row.get("keywords")),
            themes=_split(row.get("themes")),
            energy_carriers=_split(row.get("carriers")),
            scenarios=_split(row.get("scenarioIds")),
            defines_scenario=defines_scenario,
            storage_uri=_text(row, "storedIn"),
        )

        schema = SchemaLevel(
            entity_classes=entity_classes,
            entity_count=_number(row, "entityCount"),
            conforms_to="CESDM v4" if entity_classes else NA,
            tier=_infer_tier(defines_scenario, entity_classes),
        )

        quality = self._quality_summary(row, dataset)

        return EnhancedSearchResult(
            hierarchy=MetadataHierarchy(
                repository=repository,
                branch=branch,
                dataset=dataset,
                schema=schema,
            ),
            quality=quality,
        )

    @staticmethod
    def _quality_summary(
        row: Dict[str, Any], dataset: DatasetLevel
    ) -> QualitySummary:
        """Turn the ingestion pipeline's recorded facts into pass/fail checks."""
        validation_status = _text(row, "validationStatus")
        checksum = _text(row, "checksum")
        commit_id = _text(row, "commitId")

        # Superseded rows are filtered out of the query by default, so this is
        # only ever True when the caller passed include_superseded=True.
        superseded = bool(row.get("supersededBy"))

        checks = {
            "validated": validation_status not in (NA, "failed", "validation_failed"),
            "checksummed": checksum != NA,
            "committed": commit_id != NA,
            "current": not superseded,
        }

        return QualitySummary(
            validation_status=validation_status,
            validation_warning_count=_number(row, "warningCount"),
            checksum=checksum,
            file_size_bytes=_number(row, "fileSize"),
            commit_id=commit_id,
            owner=dataset.owner,
            last_modified=dataset.modified if dataset.modified != NA else dataset.issued,
            superseded=superseded,
            checks=checks,
        )

    # ── Derived (post-query) facets ──────────────────────────────────────────
    @staticmethod
    def _passes_post_filters(
        result: EnhancedSearchResult, filters: Dict[str, Any]
    ) -> bool:
        """Apply facets that have no RDF predicate to filter on.

        `environment` is inferred from the branch name and `quality` from a set
        of derived checks - neither exists as a triple, so both are applied here
        rather than pretending the store could answer them.
        """
        env = filters.get("environment")
        if env and result.hierarchy.branch.environment != env:
            return False

        quality = filters.get("quality")
        if quality == "passing_only" and result.quality.status_label != "Passing":
            return False
        if quality == "issues_only" and result.quality.status_label == "Passing":
            return False
        return True


def _infer_tier(defines_scenario: str, entity_classes: List[str]) -> str:
    """Name the ingestion tier from what the catalog recorded.

    The tier is not stored as a predicate, so it is reconstructed: a dataset
    that defines a scenario was Tier 3, one carrying CESDM entity classes was
    Tier 2, anything else fell through to Tier 1.
    """
    if defines_scenario and defines_scenario != NA:
        return "Tier 3 - Scenario Blueprint"
    if entity_classes:
        return "Tier 2 - CESDM-Aligned"
    return "Tier 1 - Generic Asset"


def _as_list(value: Any) -> List[str]:
    """Coerce a scalar-or-list metadata field into a list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Iterable):
        return [str(v) for v in value if str(v).strip()]
    return [str(value)]


__all__ = ["MetadataProvider", "EsdhMetadataProvider"]
