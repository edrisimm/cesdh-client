from __future__ import annotations

import io
import os
import tempfile
import zipfile
from typing import List, Optional

import pandas as pd
import requests


class CesDHQueryError(RuntimeError):
    """Raised when a high-level SDK query method fails."""


class EnergyDLMClient:
    def __init__(
        self,
        repository: str,
        gateway_url: str = "http://localhost:8080",
        lakefs_url: str = "http://localhost:8000",
        lakefs_access_key: str | None = None,
        lakefs_secret_key: str | None = None,
        provider=None,
    ):
        if not repository or not repository.strip():
            raise ValueError(
                "'repository' is required - the platform is multi-repository native. "
                "Every client targets exactly one project repository (e.g. 'project-alpha')."
            )
        self.gateway_url = gateway_url.rstrip("/")
        self.lakefs_url = lakefs_url.rstrip("/")
        self.repository = repository
        self._lakefs_auth = (
            lakefs_access_key or os.environ.get("LAKEFS_ACCESS_KEY_ID", ""),
            lakefs_secret_key or os.environ.get("LAKEFS_SECRET_ACCESS_KEY", ""),
        )
        # Metadata backend for enriched search. Defaults to this platform's own
        # Fuseki catalog; pass a different MetadataProvider to resolve the same
        # search contract against another catalogue (see metadata_providers).
        # Imported lazily so `import cesdh` stays cheap and cycle-free.
        if provider is None:
            from .metadata_providers import EsdhMetadataProvider

            provider = EsdhMetadataProvider(self)
        self._metadata_provider = provider

    def _repo_headers(self) -> dict:
        """Return headers for every gateway call: repository scope + user identity."""
        from . import _active_user
        headers = {"X-CESDH-Repository": self.repository}
        uid, uname = _active_user
        headers["X-ESDH-User-Id"] = uid
        headers["X-ESDH-User-Name"] = uname
        return headers

    # ------------------------------------------------------------------ #
    # Ingestion
    # ------------------------------------------------------------------ #

    def upload_model(
        self,
        file_path: str,
        owner: str,
        version: str = "1.0",
        branch: str = "main",
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
        import json

        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            data = {
                "owner": owner,
                "version": version,
                "branch": branch,
                "description": description,
                "force": str(force).lower(),
                "allow_direct_main": str(allow_direct_main).lower(),
            }
            if scenario_metadata:
                data["scenario_metadata"] = json.dumps(scenario_metadata)
            if derived_from:
                data["derived_from"] = json.dumps(derived_from)
            if run_id:
                data["run_id"] = run_id
            if force_tier:
                data["force_tier"] = force_tier
            if commit_message:
                data["commit_message"] = commit_message
            if dcat_metadata:
                data["dcat_metadata"] = json.dumps(dcat_metadata)
            if scenario_ids is not None:
                ids_list = (
                    [scenario_ids]
                    if isinstance(scenario_ids, str)
                    else list(scenario_ids)
                )
                data["scenario_ids"] = json.dumps(ids_list)
            resp = requests.post(
                f"{self.gateway_url}/ingress/upload",
                data=data,
                files=files,
                headers=self._repo_headers(),
            )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # Repository / branch / tag lifecycle
    # ------------------------------------------------------------------ #

    def create_repository(self) -> dict:
        """Explicitly provision this client's repository; 409 if it exists.

        Strict on purpose. Uploads autoprovision a repository on first write, so
        a typo'd project name would otherwise create a second repository in
        silence. Call this first when you want that mistake to be loud.

        Raises requests.HTTPError (409) when the repository already exists.
        """
        resp = requests.post(
            f"{self.gateway_url}/versioning/repository", headers=self._repo_headers()
        )
        resp.raise_for_status()
        return resp.json()

    def delete_repository(self, confirm: bool = False) -> dict:
        """Soft-delete this repository: mark as archived, queue physical teardown.

        Returns 202 with ``{"status": "archived", "repository": ...}``
        immediately. The physical teardown (Fuseki, LakeFS, MinIO) runs in
        the background. The repository disappears from listings immediately.

        ``confirm=True`` is required to prevent accidental calls from notebooks.
        """
        if not confirm:
            raise ValueError(
                "Refusing to delete without confirm=True — this is irreversible. "
                "Soft-delete archives the namespace; physical teardown is queued."
            )
        resp = requests.delete(
            f"{self.gateway_url}/versioning/repository",
            headers=self._repo_headers(),
            params={"confirm": self.repository},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()

    def create_branch(self, branch: str, source: str = "main") -> dict:
        """Create branch off source; 409 if it already exists.

        Returns {"repository", "branch", "source", "head"}.
        """
        safe_branch = self._safe_id(branch, "branch")
        resp = requests.post(
            f"{self.gateway_url}/versioning/branches",
            headers=self._repo_headers(),
            json={"branch": safe_branch, "source": source},
        )
        resp.raise_for_status()
        return resp.json()

    def delete_branch(self, branch: str) -> dict:
        """Delete a branch from the repository.

        The default branch cannot be deleted (raises HTTP 409).
        Commits remain in LakeFS history until garbage collection.
        """
        resp = requests.delete(
            f"{self.gateway_url}/versioning/branches/{branch}",
            headers=self._repo_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def create_tag(self, tag: str, ref: str = "main") -> dict:
        """Create an immutable tag at ref (branch name or commit id).

        Tags never move: re-creating an existing name raises HTTP 409 rather
        than repointing it, so a tag cited in a paper keeps resolving to the
        same bytes. Returns {"repository", "tag", "commit_id"}.
        """
        safe_tag = self._safe_id(tag, "tag")
        resp = requests.post(
            f"{self.gateway_url}/versioning/tags",
            headers=self._repo_headers(),
            json={"tag": safe_tag, "ref": ref},
        )
        resp.raise_for_status()
        return resp.json()

    def list_tags(self) -> list:
        """Return every tag in this repository as a list of dicts."""
        resp = requests.get(
            f"{self.gateway_url}/versioning/tags", headers=self._repo_headers()
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # Repository / branch metadata
    # ------------------------------------------------------------------ #

    def set_repository_metadata(self, title: str, **kwargs) -> dict:
        """Set or replace structured metadata for this repository."""
        body = {"title": title, **{k: v for k, v in kwargs.items() if v is not None}}
        resp = requests.post(
            f"{self.gateway_url}/versioning/repository/metadata",
            json=body,
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def get_repository_metadata(self) -> dict:
        """Read structured metadata for this repository."""
        resp = requests.get(
            f"{self.gateway_url}/versioning/repository/metadata",
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def set_branch_metadata(self, branch: str, title: str, **kwargs) -> dict:
        """Set or replace structured metadata for a branch."""
        body = {"branch": branch, "title": title,
                **{k: v for k, v in kwargs.items() if v is not None}}
        resp = requests.post(
            f"{self.gateway_url}/versioning/branches/metadata",
            json=body,
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def get_branch_metadata(self, branch: str) -> dict:
        """Read structured metadata for a branch."""
        resp = requests.get(
            f"{self.gateway_url}/versioning/branches/metadata",
            params={"branch": branch},
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def relate_data_to_scenarios(
        self,
        dataset_id: str,
        scenario_ids: str | list[str],
    ) -> bool:
        """Append energy:belongsToScenario triples to an existing dataset catalog node.

        Safe to call multiple times - INSERT DATA is idempotent in SPARQL.
        Returns True when the gateway confirms the update was applied.
        Raises requests.HTTPError on gateway or Fuseki failure.
        """
        ids_list = (
            [scenario_ids] if isinstance(scenario_ids, str) else list(scenario_ids)
        )
        resp = requests.post(
            f"{self.gateway_url}/datasets/{dataset_id}/scenarios",
            json={"scenario_ids": ids_list},
            headers=self._repo_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("linked", False)

    def unlink_data_from_scenario(self, dataset_id: str, scenario_id: str) -> bool:
        """Surgically removes a single energy:belongsToScenario triple.

        Does not delete or alter the physical data payload.
        Raises requests.HTTPError on gateway or Fuseki failure.
        """
        resp = requests.delete(
            f"{self.gateway_url}/datasets/{dataset_id}/scenarios/{scenario_id}",
            headers=self._repo_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("unlinked", False)

    # ------------------------------------------------------------------ #
    # Download
    # ------------------------------------------------------------------ #

    def download_to_dataframe(
        self, dataset_id: str, file_type: str = "auto"
    ) -> pd.DataFrame:
        resolve = requests.get(
            f"{self.gateway_url}/datasets/{dataset_id}/resolve",
            headers=self._repo_headers(),
        )
        resolve.raise_for_status()
        meta = resolve.json()

        fmt = file_type if file_type != "auto" else meta.get("format", "json")
        content = self._download_from_gateway(dataset_id)

        if fmt == "csv":
            return pd.read_csv(io.BytesIO(content))
        if fmt in ("h5", "hdf5"):
            return pd.read_hdf(io.BytesIO(content))
        if fmt == "json":
            import json

            data = json.loads(content)
            rows = []
            for cls_name, entities in data.items():
                for eid, body in entities.items():
                    row = {"entity_id": eid, "class": cls_name}
                    for attr in body.get("attributes", []):
                        row[attr["id"]] = attr.get("value")
                    rows.append(row)
            return pd.DataFrame(rows)
        if fmt in ("yaml", "yml"):
            import yaml

            data = yaml.safe_load(content)
            rows = []
            for cls_name, entities in data.items():
                for eid, body in entities.items():
                    row = {"entity_id": eid, "class": cls_name}
                    for attr in body.get("attributes", []):
                        row[attr["id"]] = attr.get("value")
                    rows.append(row)
            return pd.DataFrame(rows)
        raise ValueError(f"Unsupported format for DataFrame conversion: {fmt}")

    def download_to_file(self, dataset_id: str, destination: str) -> str:
        """Stream a dataset file from the gateway directly to disk without loading it into RAM.

        If *destination* is a directory, the original filename is resolved from
        the catalog and the file is saved as ``destination/<original_name>``.
        If *destination* is a file path, it is used as-is.

        Use this instead of download_to_dataframe() for large HDF5 / CSV files that
        exceed available memory. Returns the resolved destination path.
        """
        # When destination is a directory, resolve the original filename
        if os.path.isdir(destination):
            original_name = self._resolve_filename(dataset_id)
            destination = os.path.join(destination, original_name)

        with requests.get(
            f"{self.gateway_url}/datasets/{dataset_id}/download",
            headers=self._repo_headers(),
            timeout=300,
            stream=True,
        ) as resp:
            resp.raise_for_status()
            with open(destination, "wb") as fh:
                fh.writelines(resp.iter_content(chunk_size=8 * 1024 * 1024))
        return destination

    def _resolve_filename(self, dataset_id: str) -> str:
        """Fetch the original filename from the catalog via the resolve endpoint.

        Falls back to dataset_id if the filename cannot be determined.
        """
        try:
            resp = requests.get(
                f"{self.gateway_url}/datasets/{dataset_id}/resolve",
                headers=self._repo_headers(),
                timeout=15,
            )
            resp.raise_for_status()
            meta = resp.json()
            # lakefs_uri has the form lakefs://repo/branch/raw/dataset_id/filename
            lakefs_uri = meta.get("lakefs_uri", "")
            if lakefs_uri:
                name = lakefs_uri.rstrip("/").rsplit("/", 1)[-1]
                if name:
                    return name
            # Try format field to at least give the fallback an extension
            fmt = meta.get("format", "")
            if fmt:
                return f"{dataset_id}.{fmt}"
        except Exception:
            pass
        return dataset_id

    def _download_from_gateway(self, dataset_id: str) -> bytes:
        resp = requests.get(
            f"{self.gateway_url}/datasets/{dataset_id}/download",
            headers=self._repo_headers(),
            timeout=120,
        )
        resp.raise_for_status()
        return resp.content

    def clone_branch(
        self,
        branch: str,
        destination: str,
        *,
        tier: Optional[str] = None,
        include_superseded: bool = False,
    ) -> List[str]:
        """Download every active dataset on branch to disk under destination.

        Preserves the storage hierarchy (tier/dataset_id/filename) under the
        destination directory. If ``tier`` is supplied, restricts the clone to
        that tier (raw|transformed|analytics). Returns the list of resolved
        local paths in tier/dataset_id/filename order.

        Git-like semantics:
          - The clone is a snapshot of the branch's HEAD commit.
          - Future commits to the branch do not change the local files.
          - Re-cloning at the same HEAD returns identical files.
          - The branch is not modified; no commit is created.

        Raises:
            requests.HTTPError: on non-2xx gateway responses.
            requests.exceptions.ConnectionError: if the gateway is unreachable.
            RuntimeError: if the ZIP stream is malformed or contains no entries.
            ValueError: if tier is not one of raw/transformed/analytics/None,
                or if branch or destination are invalid.
        """
        # Validate inputs
        branch = self._safe_id(branch, "branch")
        if tier is not None:
            if tier.lower() not in ("raw", "transformed", "analytics"):
                raise ValueError(
                    f"tier must be one of 'raw', 'transformed', 'analytics', or None "
                    f"(got {tier!r})"
                )
            tier = tier.lower()
        if not isinstance(include_superseded, bool):
            raise ValueError("include_superseded must be a bool")
        if not destination or not isinstance(destination, str):
            raise ValueError("destination must be a non-empty string path")

        # POST to the clone endpoint
        body: dict = {"include_superseded": include_superseded}
        if tier is not None:
            body["tier"] = tier

        with requests.post(
            f"{self.gateway_url}/repos/{self.repository}/branches/{branch}/clone",
            json=body,
            headers=self._repo_headers(),
            timeout=600,
            stream=True,
        ) as resp:
            resp.raise_for_status()

            # Stream to a temp file
            tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
            tmp_path = tmp.name
            try:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
                tmp.close()

                # Extract the ZIP
                os.makedirs(destination, exist_ok=True)
                extracted: List[str] = []

                with zipfile.ZipFile(tmp_path, "r") as zf:
                    for member in zf.infolist():
                        if member.is_dir():
                            continue
                        # Security: reject path-traversal entries
                        if ".." in member.filename or member.filename.startswith("/"):
                            continue
                        target = os.path.join(destination, member.filename)
                        os.makedirs(os.path.dirname(target), exist_ok=True)
                        with zf.open(member) as src, open(target, "wb") as dst:
                            while True:
                                chunk = src.read(8 * 1024 * 1024)
                                if not chunk:
                                    break
                                dst.write(chunk)
                        extracted.append(os.path.abspath(target))

                extracted.sort()
                return extracted

            except zipfile.BadZipFile as exc:
                raise RuntimeError(
                    f"Gateway returned a malformed ZIP for {self.repository}/{branch}: {exc}"
                ) from exc
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    # ------------------------------------------------------------------ #
    # Search
    # ------------------------------------------------------------------ #

    def search(self, query: str, limit: int = 10, timeout: int = 60) -> dict:
        resp = requests.get(
            f"{self.gateway_url}/search",
            params={"q": query, "limit": limit},
            headers=self._repo_headers(),
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def sparql(self, query: str) -> dict:
        resp = requests.post(
            f"{self.gateway_url}/search/sparql",
            json={"query": query},
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def search_datasets_with_summary(
        self,
        query: str,
        filters: dict | None = None,
        limit: int = 25,
    ) -> list:
        """Search the catalog and return each hit resolved across every metadata level.

        Unlike :meth:`search` (which runs the NL translator) and :meth:`sparql`
        (which returns raw bindings), this returns
        :class:`~cesdh.models.EnhancedSearchResult` objects carrying the full
        Repository / Branch / Dataset / Schema / Quality breakdown, so a caller
        can render a result card without a follow-up request per row.

        Parameters
        ----------
        query:
            Free-text terms. Every term must match at least one of title,
            filename, format, branch, owner or keyword. Pass ``""`` to browse.
        filters:
            Optional facets. Predicate-backed: ``branch``, ``format``,
            ``entity_class``, ``theme``, ``energy_carrier``, ``owner``,
            ``scenario``. Derived (applied after the query): ``environment``
            (``"PROD"`` / ``"STG"`` / ``"DEV"``) and ``quality``
            (``"passing_only"`` / ``"issues_only"``). ``include_superseded``
            (bool) lifts the default lifecycle filter.
        limit:
            Maximum results returned.

        Notes
        -----
        Resolved in a fixed number of round trips regardless of result count -
        one SPARQL query plus one REST call per distinct branch - rather than
        one lookup per hit. See ``metadata_providers`` for the backend contract.
        """
        return self._metadata_provider.search_with_summary(
            query, filters=filters, limit=limit
        )

    def search_summary_frame(
        self,
        query: str,
        filters: dict | None = None,
        limit: int = 25,
    ) -> pd.DataFrame:
        """Same search as :meth:`search_datasets_with_summary`, flattened to a DataFrame.

        Provided because the platform's semantic abstraction rule expects
        tabular SDK results to come back as pandas. Use the object form when
        the nested levels matter, this one for analysis or export.
        """
        results = self.search_datasets_with_summary(query, filters=filters, limit=limit)
        columns = [
            "dataset_id", "title", "file_name", "format", "repository", "branch",
            "environment", "owner", "entity_classes", "quality", "pass_rate",
            "size", "issued",
        ]
        if not results:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame([r.to_row() for r in results], columns=columns)

    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # Staging & commit (Git-like update flow)
    # ------------------------------------------------------------------ #

    def stage_dataset_update(self, dataset_id: str, file_path: str, *,
                             branch: str = "main") -> dict:
        """Upload new bytes to LakeFS without committing."""
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            resp = requests.post(
                f"{self.gateway_url}/datasets/{dataset_id}/stage",
                params={"branch": branch},
                files=files,
                headers=self._repo_headers(),
                timeout=300,
            )
        resp.raise_for_status()
        return resp.json()

    def get_staged_changes(self, dataset_id: str, *,
                           branch: str = "main") -> dict:
        """Return uncommitted changes for a dataset on a branch."""
        resp = requests.get(
            f"{self.gateway_url}/datasets/{dataset_id}/staged",
            params={"branch": branch},
            headers=self._repo_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def commit_dataset_update(self, dataset_id: str, commit_message: str, *,
                              branch: str = "main", description: str = "",
                              parent_commit_sha: str | None = None) -> dict:
        """Atomically commit staged bytes as a new commit on branch."""
        body: dict = {"commit_message": commit_message}
        if description:
            body["description"] = description
        if parent_commit_sha:
            body["parent_commit_sha"] = parent_commit_sha
        resp = requests.post(
            f"{self.gateway_url}/datasets/{dataset_id}/commit",
            params={"branch": branch},
            json=body,
            headers=self._repo_headers(),
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def discard_staged_changes(self, dataset_id: str, *,
                               branch: str = "main") -> dict:
        """Drop uncommitted changes without committing."""
        resp = requests.delete(
            f"{self.gateway_url}/datasets/{dataset_id}/staged",
            params={"branch": branch},
            headers=self._repo_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # Deletion
    # ------------------------------------------------------------------ #

    def delete_dataset(self, dataset_id: str, *, branch: str = "main") -> dict:
        """Drop a dataset from a branch via a system commit.

        The physical file stays in LakeFS history. Previous commits still
        reference it. The catalog marks the dataset as superseded.
        """
        resp = requests.delete(
            f"{self.gateway_url}/datasets/{dataset_id}",
            params={"branch": branch},
            headers=self._repo_headers(),
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def delete_scenario(
        self,
        scenario_id: str,
        force: bool = False,
        cascade: bool = False,
    ) -> dict:
        resp = requests.delete(
            f"{self.gateway_url}/scenarios/{scenario_id}",
            params={"force": str(force).lower(), "cascade": str(cascade).lower()},
            headers=self._repo_headers(),
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # Entity access
    # ------------------------------------------------------------------ #

    def get_entity(self, dataset_id: str, entity_id: str) -> dict:
        resp = requests.get(
            f"{self.gateway_url}/models/{dataset_id}/entities/{entity_id}",
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def list_entities(self, dataset_id: str, class_name: str | None = None) -> list:
        params = {}
        if class_name:
            params["class_name"] = class_name
        resp = requests.get(
            f"{self.gateway_url}/models/{dataset_id}/entities",
            params=params,
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return resp.json()

    def get_schema(self, dataset_id: str) -> dict:
        resp = requests.get(
            f"{self.gateway_url}/models/{dataset_id}/schema/classes",
            headers=self._repo_headers(),
        )
        resp.raise_for_status()
        return {"classes": resp.json()}

    # ------------------------------------------------------------------ #
    # Catalog browsing
    # ------------------------------------------------------------------ #

    def list_branches(self) -> list:
        """Return [{branch, dataset_count, last_uploaded}] from GET /catalog/branches."""
        resp = requests.get(
            f"{self.gateway_url}/catalog/branches",
            headers=self._repo_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def list_scenarios(self, branch: str | None = None) -> list:
        """Return scenario summaries from GET /catalog/scenarios."""
        params = {}
        if branch:
            params["branch"] = branch
        resp = requests.get(
            f"{self.gateway_url}/catalog/scenarios",
            params=params,
            headers=self._repo_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def list_datasets(
        self,
        branch: str | None = None,
        format: str | None = None,
        entity_class: str | None = None,
        scenario: str | None = None,
        limit: int = 100,
    ) -> list:
        """Return DatasetSummary list from GET /catalog/datasets with optional facet params."""
        params: dict = {"limit": limit}
        if branch:
            params["branch"] = branch
        if format:
            params["format"] = format
        if entity_class:
            params["entity_class"] = entity_class
        if scenario:
            params["scenario"] = scenario
        resp = requests.get(
            f"{self.gateway_url}/catalog/datasets",
            params=params,
            headers=self._repo_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def list_entity_classes(self) -> list:
        """Return [{class_name, dataset_count}] from GET /catalog/entity-classes."""
        resp = requests.get(
            f"{self.gateway_url}/catalog/entity-classes",
            headers=self._repo_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # High-level query helpers - SPARQL abstraction layer
    # ------------------------------------------------------------------ #

    @staticmethod
    def _safe_id(value: str, name: str) -> str:
        """Validate an identifier before interpolating it into a SPARQL string."""
        if not value or not value.strip():
            raise ValueError(f"'{name}' must be a non-empty string")
        cleaned = value.strip()
        if any(c in cleaned for c in "<>\"' \t\n\r{}"):
            raise ValueError(
                f"'{name}' contains characters not allowed in SPARQL identifiers: {cleaned!r}"
            )
        return cleaned

    def scenario_summary(self) -> pd.DataFrame:
        """Return all active scenarios as a DataFrame.

        Columns: scenario_id, target_year, geographic_region, temporal_resolution,
                 owner, description, version, dataset_count.

        Queries both the catalog graph (dataset-level metadata) and each scenario
        named graph (target year, region) in a single federated SPARQL call.
        Returns an empty DataFrame when no scenarios are catalogued yet.
        """
        # owner, description, version are per-dataset in the catalog graph;
        # using SAMPLE() aggregates them to one representative value per scenario
        # so that multiple companion files on the same branch don't fan out into
        # separate rows.  GROUP BY retains only scenario-graph variables which
        # are constant across all datasets belonging to the same scenario.
        _QUERY = """\
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX energy:  <http://energy.ethz.ch/schema#>
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX prov:    <http://www.w3.org/ns/prov#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?scenarioIri ?targetYear ?geographicRegion ?temporalResolution
       (SAMPLE(?ownerStr) AS ?owner)
       (SAMPLE(?descRaw) AS ?description)
       (SAMPLE(?versionRaw) AS ?version)
       (COUNT(?dataset) AS ?datasetCount)
WHERE {
  GRAPH <http://energy.ethz.ch/graph/catalog> {
    ?dataset a dcat:Dataset ;
        energy:belongsToScenario ?scenarioIri ;
        prov:wasAttributedTo ?ownerIri .
    OPTIONAL { ?dataset dcat:description ?descRaw . }
    OPTIONAL { ?dataset dcat:version ?versionRaw . }
    FILTER NOT EXISTS { ?dataset energy:supersededBy ?any . }
  }
  BIND(IRI(CONCAT("http://energy.ethz.ch/graph/scenario/",
                  STRAFTER(STR(?scenarioIri), "schema#scenario_"))) AS ?sg)
  OPTIONAL {
    GRAPH ?sg {
      ?scenarioIri energy:targetYear ?targetYear .
      OPTIONAL { ?scenarioIri energy:geographicRegion ?geographicRegion . }
      OPTIONAL { ?scenarioIri energy:temporalResolution ?temporalResolution . }
    }
  }
  BIND(STRAFTER(STR(?ownerIri), "agent_") AS ?ownerStr)
}
GROUP BY ?scenarioIri ?targetYear ?geographicRegion ?temporalResolution
ORDER BY ?scenarioIri"""
        try:
            raw = self.sparql(_QUERY)
            rows = raw.get("results", [])
            if not rows:
                return pd.DataFrame(
                    columns=[
                        "scenario_id",
                        "target_year",
                        "geographic_region",
                        "temporal_resolution",
                        "owner",
                        "description",
                        "version",
                        "dataset_count",
                    ]
                )
            records = []
            for r in rows:
                scenario_iri = r.get("scenarioIri", "")
                scenario_id = (
                    scenario_iri.split("scenario_", 1)[-1]
                    if "scenario_" in scenario_iri
                    else scenario_iri
                )
                records.append(
                    {
                        "scenario_id": scenario_id,
                        "target_year": r.get("targetYear"),
                        "geographic_region": r.get("geographicRegion"),
                        "temporal_resolution": r.get("temporalResolution"),
                        "owner": r.get("owner"),
                        "description": r.get("description"),
                        "version": r.get("version"),
                        "dataset_count": int(r["datasetCount"])
                        if r.get("datasetCount")
                        else 0,
                    }
                )
            return pd.DataFrame(records)
        except (requests.HTTPError, KeyError) as exc:
            raise CesDHQueryError(f"scenario_summary failed: {exc}") from exc
        except CesDHQueryError:
            raise
        except Exception as exc:
            raise CesDHQueryError(f"scenario_summary failed: {exc}") from exc

    def get_system_nodes(self, dataset_id: str) -> pd.DataFrame:
        """Return all entity instances in a dataset's inflated system graph as a DataFrame.

        Columns: entity_id, entity_class, name.

        Unlike list_entities(), this queries the Fuseki system graph directly and
        therefore works correctly for sub-scenarios (whose in-memory model is a delta
        and yields no results from list_entities()).
        """
        safe_id = self._safe_id(dataset_id, "dataset_id")
        _QUERY = f"""\
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX cesdm: <http://www.fen-ethz.ch/ontology/2026/cesdm#>

SELECT ?entityId ?entityClass ?name WHERE {{
  GRAPH <http://energy.ethz.ch/graph/dataset/{safe_id}> {{
    ?entity rdf:type ?entityClass .
    BIND(STRAFTER(STR(?entity), "#") AS ?entityId)
    OPTIONAL {{ ?entity cesdm:name ?name . }}
    FILTER(STRSTARTS(STR(?entityClass),
           "http://www.fen-ethz.ch/ontology/2026/cesdm#"))
  }}
}}
ORDER BY ?entityClass ?entityId"""
        try:
            raw = self.sparql(_QUERY)
            rows = raw.get("results", [])
            if not rows:
                return pd.DataFrame(columns=["entity_id", "entity_class", "name"])
            records = []
            cesdm_prefix = "http://www.fen-ethz.ch/ontology/2026/cesdm#"
            for r in rows:
                cls_iri = r.get("entityClass", "")
                records.append(
                    {
                        "entity_id": r.get("entityId"),
                        "entity_class": cls_iri.replace(cesdm_prefix, "")
                        if cls_iri.startswith(cesdm_prefix)
                        else cls_iri,
                        "name": r.get("name"),
                    }
                )
            return pd.DataFrame(records)
        except (requests.HTTPError, KeyError) as exc:
            raise CesDHQueryError(
                f"get_system_nodes failed for '{dataset_id}': {exc}"
            ) from exc
        except CesDHQueryError:
            raise
        except Exception as exc:
            raise CesDHQueryError(
                f"get_system_nodes failed for '{dataset_id}': {exc}"
            ) from exc

    def get_entity_attributes(
        self, dataset_id: str, entity_class: str | None = None
    ) -> pd.DataFrame:
        """Return all attribute values from a dataset's system graph in long format.

        Columns: entity_id, entity_class, attribute, value.

        One row per entity × attribute combination. Unit triples (predicates ending
        in ``_unit``) are excluded - use ``get_system_nodes()`` if you only need the
        structural inventory without attribute values.

        Parameters
        ----------
        dataset_id:
            Dataset identifier returned by ``upload_raw()``.
        entity_class:
            Optional CESDM class name (e.g. ``"Demand.DispatchView"``) to restrict results.
            Pass ``None`` to retrieve all classes.
        """
        safe_id = self._safe_id(dataset_id, "dataset_id")
        class_filter = ""
        if entity_class is not None:
            safe_cls = self._safe_id(entity_class, "entity_class")
            class_filter = (
                f"    FILTER(?entityClass = "
                f"<http://www.fen-ethz.ch/ontology/2026/cesdm#{safe_cls}>)\n"
            )
        _QUERY = f"""\
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX cesdm: <http://www.fen-ethz.ch/ontology/2026/cesdm#>

SELECT ?entityId ?entityClass ?attribute ?value WHERE {{
  GRAPH <http://energy.ethz.ch/graph/dataset/{safe_id}> {{
    ?entity rdf:type ?entityClass ;
            ?attrPredicate ?value .
    BIND(STRAFTER(STR(?entity), "#") AS ?entityId)
    BIND(STRAFTER(STR(?attrPredicate), "cesdm#") AS ?attribute)
    FILTER(STRSTARTS(STR(?entityClass), "http://www.fen-ethz.ch/ontology/2026/cesdm#"))
    FILTER(STRSTARTS(STR(?attrPredicate), "http://www.fen-ethz.ch/ontology/2026/cesdm#"))
    FILTER(!STRENDS(STR(?attrPredicate), "_unit"))
{class_filter}  }}
}}
ORDER BY ?entityClass ?entityId ?attribute"""
        try:
            raw = self.sparql(_QUERY)
            rows = raw.get("results", [])
            if not rows:
                return pd.DataFrame(
                    columns=["entity_id", "entity_class", "attribute", "value"]
                )
            cesdm_prefix = "http://www.fen-ethz.ch/ontology/2026/cesdm#"
            records = [
                {
                    "entity_id": r.get("entityId"),
                    "entity_class": r["entityClass"].replace(cesdm_prefix, "")
                    if r.get("entityClass", "").startswith(cesdm_prefix)
                    else r.get("entityClass"),
                    "attribute": r.get("attribute"),
                    "value": r.get("value"),
                }
                for r in rows
            ]
            return pd.DataFrame(records)
        except (requests.HTTPError, KeyError) as exc:
            raise CesDHQueryError(
                f"get_entity_attributes failed for '{dataset_id}': {exc}"
            ) from exc
        except CesDHQueryError:
            raise
        except Exception as exc:
            raise CesDHQueryError(
                f"get_entity_attributes failed for '{dataset_id}': {exc}"
            ) from exc

    def get_scenario_lineage(self, scenario_id: str) -> pd.DataFrame:
        """Return datasets for a scenario and all sub-scenarios derived from it.

        Columns: dataset_id, title, branch, scenario_id, parent_scenario_id,
                 parent_dataset_ids, activity_id.

        scenario_id        - which scenario the dataset belongs to (root or sub).
        parent_scenario_id - baseline scenario for sub-scenario datasets; None for root.
        parent_dataset_ids - pipe-separated list of input dataset IDs (PROV-O
                             wasDerivedFrom); None unless derived_from= was used.
                             Multiple inputs produce a single "|"-joined string.
        activity_id        - PROV-O activity; only populated when derived_from= was used.
        """
        safe_id = self._safe_id(scenario_id, "scenario_id")
        _QUERY = f"""\
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX energy:  <http://energy.ethz.ch/schema#>
PREFIX prov:    <http://www.w3.org/ns/prov#>
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?datasetId ?title ?branch ?scenarioId ?parentScenarioId
       (GROUP_CONCAT(DISTINCT ?parentDatasetId; separator="|") AS ?parentDatasetIds)
       (SAMPLE(?activityIdRaw) AS ?activityId)
WHERE {{
  {{
    GRAPH <http://energy.ethz.ch/graph/catalog> {{
      ?dataset energy:belongsToScenario energy:scenario_{safe_id} ;
          dcterms:identifier ?datasetId ;
          dcat:title ?title ;
          energy:lakefsBranch ?branch .
      FILTER NOT EXISTS {{ ?dataset energy:supersededBy ?any . }}
    }}
    BIND("{safe_id}" AS ?scenarioId)
  }}
  UNION
  {{
    GRAPH ?subGraph {{
      ?subScenIri energy:baselineScenario energy:scenario_{safe_id} .
    }}
    FILTER(STRSTARTS(STR(?subGraph), "http://energy.ethz.ch/graph/scenario/"))
    GRAPH <http://energy.ethz.ch/graph/catalog> {{
      ?dataset energy:belongsToScenario ?subScenIri ;
          dcterms:identifier ?datasetId ;
          dcat:title ?title ;
          energy:lakefsBranch ?branch .
      FILTER NOT EXISTS {{ ?dataset energy:supersededBy ?any . }}
    }}
    BIND(STRAFTER(STR(?subScenIri), "schema#scenario_") AS ?scenarioId)
    BIND("{safe_id}" AS ?parentScenarioId)
  }}
  OPTIONAL {{
    GRAPH <http://energy.ethz.ch/graph/catalog> {{
      ?dataset prov:wasDerivedFrom ?parentNode .
      ?parentNode dcterms:identifier ?parentDatasetId .
    }}
  }}
  OPTIONAL {{
    GRAPH <http://energy.ethz.ch/graph/catalog> {{
      ?dataset prov:wasGeneratedBy ?activity .
      BIND(STRAFTER(STR(?activity), "schema#") AS ?activityIdRaw)
    }}
  }}
}}
GROUP BY ?datasetId ?title ?branch ?scenarioId ?parentScenarioId
ORDER BY ?scenarioId ?datasetId"""
        try:
            raw = self.sparql(_QUERY)
            rows = raw.get("results", [])
            if not rows:
                return pd.DataFrame(
                    columns=[
                        "dataset_id",
                        "title",
                        "branch",
                        "scenario_id",
                        "parent_scenario_id",
                        "parent_dataset_ids",
                        "activity_id",
                    ]
                )
            records = [
                {
                    "dataset_id": r.get("datasetId"),
                    "title": r.get("title"),
                    "branch": r.get("branch"),
                    "scenario_id": r.get("scenarioId"),
                    "parent_scenario_id": r.get("parentScenarioId"),
                    # GROUP_CONCAT returns "" when no parents exist; normalise to None
                    "parent_dataset_ids": r.get("parentDatasetIds") or None,
                    "activity_id": r.get("activityId") or None,
                }
                for r in rows
            ]
            return pd.DataFrame(records)
        except (requests.HTTPError, KeyError) as exc:
            raise CesDHQueryError(
                f"get_scenario_lineage failed for '{scenario_id}': {exc}"
            ) from exc
        except CesDHQueryError:
            raise
        except Exception as exc:
            raise CesDHQueryError(
                f"get_scenario_lineage failed for '{scenario_id}': {exc}"
            ) from exc

    # ------------------------------------------------------------------ #
    # Audit utilities
    # ------------------------------------------------------------------ #

    def get_file_branches_history(
        self, file_path: str, include_superseded: bool = False
    ) -> pd.DataFrame:
        """Return all branch snapshots of file_path as a DataFrame.

        Columns: branch, path, owner, checksum, size_bytes, last_modified.
        Sorted newest-first by last_modified. Returns an empty DataFrame with
        this exact schema when file_path is absent from every branch.

        When include_superseded=False (default), rows whose parent dataset
        carries energy:supersededBy in the Fuseki catalog are silently dropped.
        Pass include_superseded=True to bypass that filter.

        Raises CesDHQueryError if the gateway branch list cannot be fetched.
        Per-branch LakeFS stat failures are silently skipped.
        """
        _COLUMNS = [
            "branch",
            "path",
            "owner",
            "checksum",
            "size_bytes",
            "last_modified",
        ]

        # Step A: Branch scanning via gateway catalog
        try:
            resp = requests.get(
                f"{self.gateway_url}/catalog/branches",
                headers=self._repo_headers(),
                timeout=30,
            )
            resp.raise_for_status()
            branch_names = [
                b.get("branch") or b.get("name", "")
                for b in resp.json()
                if b.get("branch") or b.get("name")
            ]
        except (requests.HTTPError, ValueError) as exc:
            raise CesDHQueryError(
                f"get_file_branches_history: branch scan failed: {exc}"
            ) from exc

        # Steps B & C: Object inspection + governance extraction per branch
        records = []
        for branch in branch_names:
            stat = self._lakefs_stat(branch, file_path)
            if stat is None:
                continue

            metadata = stat.get("metadata") or {}
            owner = (
                metadata.get("owner")
                or metadata.get("author")
                or self._lakefs_head_committer(branch)
            )
            mtime = stat.get("mtime")
            last_modified = (
                pd.Timestamp(mtime, unit="s", tz="UTC").isoformat() if mtime else None
            )
            records.append(
                {
                    "branch": branch,
                    "path": stat.get("path", file_path),
                    "owner": owner,
                    "checksum": stat.get("checksum"),
                    "size_bytes": stat.get("size_bytes"),
                    "last_modified": last_modified,
                }
            )

        if not records:
            return pd.DataFrame(columns=_COLUMNS)

        df = pd.DataFrame(records)[_COLUMNS]

        # Step D: Semantic lifecycle filtering
        if not include_superseded:
            df = self._drop_superseded_rows(df)

        if df.empty:
            return pd.DataFrame(columns=_COLUMNS)

        return df.sort_values(
            "last_modified", ascending=False, na_position="last"
        ).reset_index(drop=True)

    def get_branch_changes(self, branch: str, base: str = "main") -> pd.DataFrame:
        """Return active datasets uploaded to branch as a DataFrame.

        Queries the Fuseki semantic catalog for all non-superseded datasets
        tagged with energy:lakefsBranch = branch. The base parameter is
        accepted for API compatibility but not used - the catalog is the
        authoritative source for branch membership on this platform.

        Columns: path, type, size_bytes, last_modified.
        'type' is always 'added' (active dataset on the branch).
        'path' is the LakeFS object path relative to the repository root
        (e.g. 'raw/dataset_abc123/filename.csv').
        Returns an empty DataFrame with this exact schema when no active
        datasets are found on the branch.

        Raises CesDHQueryError on catalog query failure.
        """
        _COLUMNS = ["path", "type", "size_bytes", "last_modified"]
        _branch_esc = branch.replace("\\", "\\\\").replace('"', '\\"')
        _QUERY = f"""\
PREFIX energy:  <http://energy.ethz.ch/schema#>
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
SELECT ?storedIn ?fileSize ?issued WHERE {{
  GRAPH <http://energy.ethz.ch/graph/catalog> {{
    ?dataset a dcat:Dataset ;
        energy:lakefsBranch "{_branch_esc}" ;
        energy:storedIn ?storedIn ;
        dcat:issued ?issued .
    OPTIONAL {{ ?dataset energy:fileSize ?fileSize . }}
    FILTER NOT EXISTS {{ ?dataset energy:supersededBy ?successor . }}
  }}
}}
ORDER BY DESC(?issued)"""
        try:
            raw = self.sparql(_QUERY)
        except (CesDHQueryError, requests.HTTPError) as exc:
            raise CesDHQueryError(
                f"get_branch_changes({branch!r}) failed: {exc}"
            ) from exc

        results = raw.get("results", [])
        if not results:
            return pd.DataFrame(columns=_COLUMNS)

        _prefix = f"lakefs://{self.repository}/{branch}/"
        records = []
        for row in results:
            stored_in = row.get("storedIn", "")
            path = stored_in.removeprefix(_prefix)
            size_raw = row.get("fileSize")
            records.append(
                {
                    "path": path,
                    "type": "added",
                    "size_bytes": int(size_raw) if size_raw else None,
                    "last_modified": row.get("issued"),
                }
            )

        if not records:
            return pd.DataFrame(columns=_COLUMNS)
        return pd.DataFrame(records)[_COLUMNS]

    def _lakefs_stat(self, branch: str, file_path: str) -> dict | None:
        """Return LakeFS object stat for file_path on branch, or None if absent."""
        try:
            resp = requests.get(
                f"{self.lakefs_url}/api/v1/repositories/{self.repository}"
                f"/refs/{branch}/objects/stat",
                params={"path": file_path},
                auth=self._lakefs_auth,
                timeout=30,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError:
            return None

    def _lakefs_head_committer(self, branch: str) -> str | None:
        """Return the committer name from the most recent commit on branch."""
        try:
            resp = requests.get(
                f"{self.lakefs_url}/api/v1/repositories/{self.repository}"
                f"/refs/{branch}/commits",
                params={"amount": 1},
                auth=self._lakefs_auth,
                timeout=30,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            return results[0].get("committer") if results else None
        except (requests.HTTPError, IndexError, KeyError):
            return None

    def _drop_superseded_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows whose associated dataset carries energy:supersededBy in Fuseki."""
        import re

        _DATASET_RE = re.compile(r"(?:^|/)dataset_([A-Za-z0-9_-]+)")

        def _extract_id(path: str) -> str | None:
            m = _DATASET_RE.search(path)
            return f"dataset_{m.group(1)}" if m else None

        path_to_id: dict = {p: _extract_id(p) for p in df["path"].unique()}
        dataset_ids = [did for did in path_to_id.values() if did]
        if not dataset_ids:
            return df

        id_list = ", ".join(f'"{did}"' for did in dataset_ids)
        _QUERY = f"""\
PREFIX energy:  <http://energy.ethz.ch/schema#>
PREFIX dcat:    <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?datasetId WHERE {{
  GRAPH <http://energy.ethz.ch/graph/catalog> {{
    ?dataset a dcat:Dataset ;
        dcterms:identifier ?datasetId ;
        energy:supersededBy ?replacement .
    FILTER(?datasetId IN ({id_list}))
  }}
}}"""
        try:
            raw = self.sparql(_QUERY)
            superseded_ids = set(
                r.get("datasetId") for r in raw.get("results", []) if r.get("datasetId")
            )
        except Exception:
            return df

        if not superseded_ids:
            return df

        keep_mask = df["path"].map(lambda p: path_to_id.get(p) not in superseded_ids)
        return df[keep_mask].reset_index(drop=True)
