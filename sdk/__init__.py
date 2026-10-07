from __future__ import annotations

import os
import re
from typing import List, Optional, Union

from .client import CesDHQueryError  # re-exported for callers  # noqa: F401
from .client import EnergyDLMClient as _EnergyDLMClient

# Multi-level search models, re-exported so callers can type-annotate results
# without reaching into the submodule.
from .models import (  # noqa: F401
    BranchLevel,
    DatasetLevel,
    EnhancedSearchResult,
    MetadataHierarchy,
    QualitySummary,
    RepositoryLevel,
    SchemaLevel,
)


def _load_dotenv_if_needed() -> None:
    """Load .env from the repo root into os.environ without overriding existing vars.

    Only runs when LAKEFS_ACCESS_KEY_ID is absent from the environment (i.e. in
    local development sessions where the stack credentials live only in .env).
    Production and CI environments that export credentials explicitly are unaffected.
    """
    if os.getenv("LAKEFS_ACCESS_KEY_ID"):
        return
    # Walk up from src/sdk/ to find the repo root .env
    _sdk_dir = os.path.dirname(os.path.abspath(__file__))
    _candidates = [
        os.path.normpath(os.path.join(_sdk_dir, "..", "..", ".env")),  # repo root
        os.path.join(os.getcwd(), ".env"),
    ]
    for _dotenv_path in _candidates:
        if not os.path.isfile(_dotenv_path):
            continue
        with open(_dotenv_path) as _fh:
            for _raw in _fh:
                _line = _raw.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                if _key and _key not in os.environ:
                    os.environ[_key] = _val
        break


_load_dotenv_if_needed()

# LakeFS branch/tag ids: letters, digits, underscores and dashes only, and they
# cannot start with a dash. Slashes are NOT allowed - "scenario/CH_2040" is
# rejected by LakeFS itself. Validated here so the caller gets a precise error
# without a network round-trip.
_BRANCH_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")


def _validate_branch(branch: str, field: str = "branch") -> str:
    """Raise ValueError unless `branch` is a name LakeFS will accept."""
    if not branch or not branch.strip():
        raise ValueError(f"'{field}' must be a non-empty string")
    if not _BRANCH_RE.match(branch):
        raise ValueError(
            f"'{field}' must contain only letters, digits, underscores and dashes, "
            f"and cannot start with a dash - LakeFS rejects anything else "
            f"(e.g. 'scenario_CH_2040', not {branch!r})"
        )
    return branch


DEFAULT_ENDPOINT = os.getenv("CESDH_DATA_HUB_ENDPOINT", "http://localhost:8080")

# Advanced escape hatch - importable for multi-target scripts, hidden from default autocomplete
CESDHClient = _EnergyDLMClient

_clients: dict = {}

_active_user: tuple[str, str] = ("anonymous", "Anonymous")


def set_user(user_id: str, display_name: str) -> None:
    """Set the calling identity for subsequent SDK calls.

    Mirrors ``git config user.name`` — once set, every upload, create,
    and delete carries this identity in the X-ESDH-User-Id and
    X-ESDH-User-Name headers. The gateway stamps it into PROV-O triples
    and the audit log.
    """
    global _active_user
    if not user_id or not isinstance(user_id, str):
        raise ValueError("'user_id' must be a non-empty string")
    if not display_name or not isinstance(display_name, str):
        raise ValueError("'display_name' must be a non-empty string")
    _active_user = (user_id.strip(), display_name.strip())


def get_user() -> tuple:
    """Return (user_id, display_name) — the identity passed on subsequent calls."""
    return _active_user


def _client_for(repository: str) -> _EnergyDLMClient:
    """Return a cached EnergyDLMClient bound to repository, creating it on first use.

    The platform is multi-repository native - every call into the gateway must
    declare its target project. There is no implicit platform-wide default.
    """
    if not repository or not repository.strip():
        raise ValueError(
            "'repository' is required - the platform is multi-repository native. "
            "Pass the target project repository (e.g. 'project-alpha') to every call."
        )
    if repository not in _clients:
        _clients[repository] = _EnergyDLMClient(
            gateway_url=DEFAULT_ENDPOINT, repository=repository
        )
    return _clients[repository]


def download_to_dataframe(
    dataset_id: str,
    repository: str,
    *,
    branch: str | None = None,
    file_type: str = "auto",
):
    """Download any repository asset (YAML, JSON, CSV, HDF5) by dataset_id into a DataFrame.

    Not limited to scenario models - works for configuration assets, weather profiles,
    lookup matrices, or any other structured file type stored in the data lake.
    branch is reserved for future branch-aware resolution.
    """
    del branch  # reserved: future branch-aware dataset resolution
    return _client_for(repository).download_to_dataframe(
        dataset_id, file_type=file_type
    )


def upload_raw(
    file_path: str,
    owner: str,
    repository: str,
    branch: str = "main",
    version: str = "1.0",
    description: str = "",
    scenario_metadata: dict | None = None,
    force: bool = False,
    allow_direct_main: bool = False,
    derived_from: list | None = None,
    run_id: str | None = None,
    force_tier: str | None = None,
    scenario_ids: str | list[str] | None = None,
    commit_message: str | None = None,
    dcat_metadata: dict | None = None,
) -> dict:
    if owner != owner.strip() or " " in owner:
        raise ValueError(
            f"'owner' must not contain spaces, use hyphens or underscores "
            f"(e.g. 'FEN-team', not {owner!r})"
        )
    _validate_branch(branch)
    if run_id is not None and (" " in run_id or run_id != run_id.strip()):
        raise ValueError(
            f"'run_id' must not contain spaces - it is used to construct a provenance IRI "
            f"(e.g. 'run_20240101', not {run_id!r})"
        )
    return _client_for(repository).upload_model(
        file_path,
        owner=owner,
        branch=branch,
        version=version,
        description=description,
        scenario_metadata=scenario_metadata,
        force=force,
        allow_direct_main=allow_direct_main,
        derived_from=derived_from,
        run_id=run_id,
        force_tier=force_tier,
        scenario_ids=scenario_ids,
        commit_message=commit_message,
        dcat_metadata=dcat_metadata,
    )


# ------------------------------------------------------------------ #
# Repository / branch / tag lifecycle
# ------------------------------------------------------------------ #


def create_repository(repository: str) -> dict:
    """Explicitly provision repository; raises HTTP 409 if it already exists.

    Strict by design so a typo'd project name fails loudly instead of silently
    autoprovisioning a second repository. Uploads still autoprovision on first
    write - this is the opt-in pre-flight check.
    """
    return _client_for(repository).create_repository()


def delete_repository(repository: str, *, confirm: bool = False) -> dict:
    """Soft-delete a repository: archive immediately, queue physical teardown.

    Returns ``{"status": "archived", "repository": ...}`` within 1 second.
    The physical teardown runs in the background on the gateway.

    ``confirm=True`` is required to prevent accidental calls from notebooks.
    """
    return _client_for(repository).delete_repository(confirm=confirm)


def create_branch(branch: str, repository: str, source: str = "main") -> dict:
    """Create branch off source in repository; raises HTTP 409 if it exists."""
    _validate_branch(branch)
    return _client_for(repository).create_branch(branch, source=source)


def delete_branch(branch: str, repository: str) -> dict:
    """Delete a branch from the repository.

    The default branch cannot be deleted (raises HTTP 409). Commits remain
    in LakeFS history until garbage collection runs.
    """
    _validate_branch(branch)
    return _client_for(repository).delete_branch(branch)


def create_tag(tag: str, repository: str, ref: str = "main") -> dict:
    """Create an immutable tag at ref (a branch name or commit id).

    Tags are the citable reference for a finished run. Re-creating an existing
    tag raises HTTP 409 rather than repointing it, so a published reference
    cannot change meaning underneath the reader.
    """
    _validate_branch(tag, "tag")
    return _client_for(repository).create_tag(tag, ref=ref)


def list_tags(repository: str) -> list:
    """Return every tag in repository as a list of {tag, commit_id} dicts."""
    return _client_for(repository).list_tags()


# ------------------------------------------------------------------ #
# Repository / branch metadata
# ------------------------------------------------------------------ #


def set_repository_metadata(
    repository: str,
    title: str,
    *,
    description: Optional[str] = None,
    owner: Optional[str] = None,
    spatial_coverage: Optional[Union[str, List[str]]] = None,
    energy_carriers: Optional[List[str]] = None,
    themes: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
    model_frameworks: Optional[List[str]] = None,
    funding_program: Optional[str] = None,
    publisher: Optional[str] = None,
) -> dict:
    """Set or replace structured metadata for a repository (DCAT Catalog node).

    Only ``title`` is required. All other fields are optional and will be
    written as DCAT/DCTerms/energy predicates in the catalog graph.
    """
    return _client_for(repository).set_repository_metadata(
        title,
        description=description,
        owner=owner,
        spatial_coverage=spatial_coverage,
        energy_carriers=energy_carriers,
        themes=themes,
        keywords=keywords,
        model_frameworks=model_frameworks,
        funding_program=funding_program,
        publisher=publisher,
    )


def get_repository_metadata(repository: str) -> dict:
    """Read structured metadata for a repository. Raises HTTPError 404 if none set."""
    return _client_for(repository).get_repository_metadata()


def set_branch_metadata(
    branch: str,
    repository: str,
    title: str,
    *,
    description: Optional[str] = None,
    owner: Optional[str] = None,
    target_year: Optional[str] = None,
    spatial_coverage: Optional[Union[str, List[str]]] = None,
    energy_carriers: Optional[List[str]] = None,
    themes: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
    hypothesis: Optional[str] = None,
) -> dict:
    """Set or replace structured metadata for a branch (energy:Branch node)."""
    return _client_for(repository).set_branch_metadata(
        branch,
        title,
        description=description,
        owner=owner,
        target_year=target_year,
        spatial_coverage=spatial_coverage,
        energy_carriers=energy_carriers,
        themes=themes,
        keywords=keywords,
        hypothesis=hypothesis,
    )


def get_branch_metadata(branch: str, repository: str) -> dict:
    """Read structured metadata for a branch. Raises HTTPError 404 if none set."""
    return _client_for(repository).get_branch_metadata(branch)


def search(query: str, repository: str, limit: int = 10, timeout: int = 60) -> dict:
    return _client_for(repository).search(query, limit=limit, timeout=timeout)


def sparql(query: str, repository: str) -> dict:
    return _client_for(repository).sparql(query)


def search_datasets_with_summary(
    query: str,
    repository: str,
    filters: Optional[dict] = None,
    limit: int = 25,
) -> List[EnhancedSearchResult]:
    """Search the catalog, returning each hit with its full metadata hierarchy.

    Each result carries Repository / Branch / Dataset / Schema / Quality levels,
    so a caller can show what a dataset is, where it came from and whether it
    can be trusted without issuing a follow-up query per hit.

        >>> import cesdh
        >>> hits = cesdh.search_datasets_with_summary(
        ...     "solar irradiation",
        ...     repository="multi-researcher-demo",
        ...     filters={"environment": "PROD", "quality": "passing_only"},
        ... )
        >>> hits[0].breadcrumb
        'multi-researcher-demo / main (PROD) > CH_solar_irradiation_2024.csv'

    See :meth:`EnergyDLMClient.search_datasets_with_summary` for the filter
    vocabulary.
    """
    return _client_for(repository).search_datasets_with_summary(
        query, filters=filters, limit=limit
    )


def search_summary_frame(
    query: str,
    repository: str,
    filters: Optional[dict] = None,
    limit: int = 25,
) -> "pd.DataFrame":
    """Flattened DataFrame form of :func:`search_datasets_with_summary`."""
    return _client_for(repository).search_summary_frame(
        query, filters=filters, limit=limit
    )


# ------------------------------------------------------------------ #
# Staging & commit (Git-like update flow)
# ------------------------------------------------------------------ #


def stage_dataset_update(
    dataset_id: str, file_path: str, repository: str, *, branch: str = "main",
) -> dict:
    """Upload new bytes to LakeFS without committing."""
    return _client_for(repository).stage_dataset_update(
        dataset_id, file_path, branch=branch,
    )


def get_staged_changes(
    dataset_id: str, repository: str, *, branch: str = "main",
) -> dict:
    """Return the diff summary for uncommitted changes."""
    return _client_for(repository).get_staged_changes(dataset_id, branch=branch)


def commit_dataset_update(
    dataset_id: str,
    repository: str,
    commit_message: str,
    *,
    branch: str = "main",
    description: str = "",
    parent_commit_sha: Optional[str] = None,
) -> dict:
    """Atomically commit staged bytes as a new commit on branch."""
    return _client_for(repository).commit_dataset_update(
        dataset_id, commit_message, branch=branch,
        description=description, parent_commit_sha=parent_commit_sha,
    )


def discard_staged_changes(
    dataset_id: str, repository: str, *, branch: str = "main",
) -> None:
    """Drop uncommitted changes without committing."""
    _client_for(repository).discard_staged_changes(dataset_id, branch=branch)


def clone_branch(
    branch: str,
    destination: str,
    repository: str,
    *,
    tier: Optional[str] = None,
    include_superseded: bool = False,
) -> List[str]:
    """Download every active dataset on branch to disk under destination.

    Mirror of EnergyDLMClient.clone_branch. See that method for the full
    contract; the highlights are:

    - Preserves the storage hierarchy under destination.
    - Optional ``tier`` filter (raw|transformed|analytics).
    - The clone is a snapshot of the branch's HEAD commit; subsequent
      commits do not affect the local files.

    Examples:
        >>> import cesdh
        >>> cesdh.set_user("alice", "Alice from FEN-team")
        >>> cesdh.clone_branch(
        ...     "main", "/tmp/esdh-mirror", repository="energy-repository"
        ... )
        ['/tmp/esdh-mirror/raw/dataset_abc/foo.csv', ...]
    """
    _validate_branch(branch)
    return _client_for(repository).clone_branch(
        branch,
        destination,
        tier=tier,
        include_superseded=include_superseded,
    )


def delete_dataset(dataset_id: str, repository: str, *, branch: str = "main") -> dict:
    """Drop a dataset from a branch via a system commit.

    The physical file stays in LakeFS history. The catalog marks the
    dataset as superseded (self-supersession = explicit deletion).
    """
    return _client_for(repository).delete_dataset(dataset_id, branch=branch)


def delete_scenario(
    scenario_id: str, repository: str, force: bool = False, cascade: bool = False
) -> dict:
    if scenario_id != scenario_id.strip() or " " in scenario_id:
        raise ValueError(
            f"'scenario_id' must not contain spaces - it is used in URI and path construction "
            f"(got {scenario_id!r})"
        )
    return _client_for(repository).delete_scenario(
        scenario_id, force=force, cascade=cascade
    )


def download_to_file(dataset_id: str, destination: str, repository: str) -> str:
    return _client_for(repository).download_to_file(dataset_id, destination)


def list_branches(repository: str) -> list:
    return _client_for(repository).list_branches()


def list_scenarios(repository: str, branch: str | None = None) -> list:
    if branch is not None:
        _validate_branch(branch)
    return _client_for(repository).list_scenarios(branch=branch)


def list_datasets(
    repository: str,
    branch: str | None = None,
    format: str | None = None,
    entity_class: str | None = None,
    scenario: str | None = None,
    limit: int = 100,
) -> list:
    if branch is not None:
        _validate_branch(branch)
    return _client_for(repository).list_datasets(
        branch=branch,
        format=format,
        entity_class=entity_class,
        scenario=scenario,
        limit=limit,
    )


def list_entity_classes(repository: str) -> list:
    return _client_for(repository).list_entity_classes()


def get_entity(dataset_id: str, entity_id: str, repository: str) -> dict:
    return _client_for(repository).get_entity(dataset_id, entity_id)


def list_entities(
    dataset_id: str, repository: str, class_name: str | None = None
) -> list:
    return _client_for(repository).list_entities(dataset_id, class_name=class_name)


def get_schema(dataset_id: str, repository: str) -> dict:
    return _client_for(repository).get_schema(dataset_id)


# ------------------------------------------------------------------ #
# High-level query helpers - SPARQL abstraction layer
# ------------------------------------------------------------------ #


def scenario_summary(repository: str) -> pd.DataFrame:
    """Return all active scenarios as a DataFrame.

    Columns: scenario_id, target_year, geographic_region, temporal_resolution,
             owner, description, version, dataset_count.
    Raises CesDHQueryError on gateway or parse failure.
    """
    import pandas as pd  # noqa: F401 - type reference only at module load

    return _client_for(repository).scenario_summary()


def get_system_nodes(dataset_id: str, repository: str) -> pd.DataFrame:
    """Return all entity instances from a dataset's inflated Fuseki system graph.

    Columns: entity_id, entity_class, name.
    Works for both baselines and sub-scenarios; use this instead of list_entities()
    when working with sub-scenario datasets.
    Raises ValueError on empty/invalid dataset_id, CesDHQueryError on failure.
    """
    import pandas as pd  # noqa: F401 - type reference only at module load

    if not dataset_id or not dataset_id.strip():
        raise ValueError("'dataset_id' must be a non-empty string")
    return _client_for(repository).get_system_nodes(dataset_id.strip())


def get_entity_attributes(
    dataset_id: str, repository: str, entity_class: str | None = None
) -> pd.DataFrame:
    """Return all attribute values from a dataset's system graph in long format.

    Columns: entity_id, entity_class, attribute, value.
    Pass entity_class (e.g. ``"Demand.DispatchView"``) to restrict to one class.
    Raises ValueError on empty/invalid inputs, CesDHQueryError on failure.
    """
    import pandas as pd  # noqa: F401 - type reference only at module load

    if not dataset_id or not dataset_id.strip():
        raise ValueError("'dataset_id' must be a non-empty string")
    return _client_for(repository).get_entity_attributes(
        dataset_id.strip(),
        entity_class=entity_class.strip() if entity_class else None,
    )


def get_branch_changes(
    branch: str, repository: str, base: str = "main"
) -> pd.DataFrame:
    """Return active datasets uploaded to branch as a DataFrame.

    Queries the Fuseki semantic catalog for all non-superseded datasets
    tagged with the given branch label. The base parameter is accepted
    for API compatibility but is not used in the query.

    Columns: path, type, size_bytes, last_modified.
    'type' is always 'added' (active, non-superseded dataset on the branch).
    'path' is the LakeFS object path relative to the repository root.
    Returns an empty DataFrame with this exact schema when no active
    datasets are found on the branch.

    Raises ValueError on empty/blank branch or base.
    Raises CesDHQueryError on catalog query failure.
    """
    import pandas as pd  # noqa: F401 - type reference only at module load

    if not branch or not branch.strip():
        raise ValueError("'branch' must be a non-empty string")
    if not base or not base.strip():
        raise ValueError("'base' must be a non-empty string")
    return _client_for(repository).get_branch_changes(branch.strip(), base=base.strip())


def get_file_branches_history(
    file_path: str, repository: str, include_superseded: bool = False
) -> pd.DataFrame:
    """Trace all branch snapshots of a repository path as a DataFrame.

    Scans every active LakeFS branch for file_path, extracts ownership from
    object metadata (``owner``/``author`` keys) or the branch HEAD committer,
    and cross-references the Fuseki knowledge graph to drop superseded dataset
    versions (unless include_superseded=True).

    Columns: branch, path, owner, checksum, size_bytes, last_modified.
    Sorted newest-first. Returns an empty DataFrame with this exact schema
    when file_path is absent from all branches.

    Raises ValueError on empty/blank file_path.
    Raises CesDHQueryError if the branch list cannot be fetched.
    """
    import pandas as pd  # noqa: F401 - type reference only at module load

    if not file_path or not file_path.strip():
        raise ValueError("'file_path' must be a non-empty string")
    return _client_for(repository).get_file_branches_history(
        file_path.strip(), include_superseded=include_superseded
    )


def relate_data_to_scenarios(
    dataset_id: str,
    scenario_ids: str | list[str],
    repository: str,
) -> bool:
    """Append energy:belongsToScenario triples to an existing dataset catalog node.

    Binds dataset_id to one or more scenario URIs without overwriting any
    existing scenario links already in the graph.  Safe to call repeatedly -
    SPARQL INSERT DATA is idempotent.  Use this to bind a shared dataset to
    additional scenarios after initial upload, or to create many-to-many
    relationships between a physical file and multiple scenario contexts.

    Parameters
    ----------
    dataset_id:
        Identifier returned by ``upload_raw()`` (e.g. ``"dataset_abc123"``).
    scenario_ids:
        One scenario ID string or a list of scenario ID strings.
        Each entry produces one ``energy:belongsToScenario`` triple.
    repository:
        The project repository that owns dataset_id.

    Returns
    -------
    bool
        True when the gateway confirms the update was applied.

    Raises
    ------
    ValueError
        If ``dataset_id`` is empty or blank.
    requests.HTTPError
        If the gateway or Fuseki returns a non-2xx response.
    """
    if not dataset_id or not dataset_id.strip():
        raise ValueError("'dataset_id' must be a non-empty string")
    return _client_for(repository).relate_data_to_scenarios(
        dataset_id.strip(), scenario_ids
    )


def unlink_data_from_scenario(
    dataset_id: str, scenario_id: str, repository: str
) -> bool:
    """Surgically removes a single energy:belongsToScenario triple from the catalog graph.

    Targets the specific (dataset_id, scenario_id) pair only.  No other catalog
    triples, physical LakeFS objects, or scenario graph entries are touched.
    Safe to call even when the triple no longer exists - SPARQL DELETE DATA on
    an absent triple is a no-op.

    Parameters
    ----------
    dataset_id:
        Identifier returned by ``upload_raw()`` (e.g. ``"dataset_abc123"``).
    scenario_id:
        The scenario ID string to unlink (e.g. ``"baseline_SC_NT_SY_2030_WY_2009"``).
    repository:
        The project repository that owns dataset_id.

    Returns
    -------
    bool
        True when the gateway confirms the DELETE was executed.

    Raises
    ------
    ValueError
        If either argument is empty or blank.
    requests.HTTPError
        If the gateway or Fuseki returns a non-2xx response.
    """
    if not dataset_id or not dataset_id.strip():
        raise ValueError("'dataset_id' must be a non-empty string")
    if not scenario_id or not scenario_id.strip():
        raise ValueError("'scenario_id' must be a non-empty string")
    return _client_for(repository).unlink_data_from_scenario(
        dataset_id.strip(), scenario_id.strip()
    )


def get_scenario_lineage(scenario_id: str, repository: str) -> pd.DataFrame:
    """Return datasets for a scenario and all sub-scenarios derived from it.

    Columns: dataset_id, title, branch, scenario_id, parent_scenario_id,
             parent_dataset_ids, activity_id.
    parent_scenario_id is None for root scenario datasets, populated for sub-scenario datasets.
    parent_dataset_ids is None unless derived_from= was passed to upload_raw();
    multiple inputs are joined with "|" in a single string.
    Raises ValueError on empty/invalid scenario_id, CesDHQueryError on failure.
    """
    import pandas as pd  # noqa: F401 - type reference only at module load

    if not scenario_id or not scenario_id.strip():
        raise ValueError("'scenario_id' must be a non-empty string")
    return _client_for(repository).get_scenario_lineage(scenario_id.strip())
