"""LocalRepo — Git-like working-copy wrapper over a cloned ESDH branch.

Provides the clone → edit → status → diff → stage → commit → restore loop
as a single class, backed by the existing EnergyDLMClient primitives.
No new gateway endpoints are needed.

Usage::

    from cesdh import LocalRepo

    repo = LocalRepo(
        repository="energy-repository",
        branch="experiment-2030",
        path="./my-working-copy",
        owner="alice",
    )
    # repo.path now contains the cloned files

    # Edit a file on disk, then:
    print(repo.status())        # DataFrame of local vs cloned state
    print(repo.diff("raw/dataset_abc/foo.csv"))  # unified diff

    repo.add("raw/dataset_abc/foo.csv")           # stage one file
    repo.commit("Update demand profile for Q4")    # commit staged changes

    repo.restore("raw/dataset_abc/foo.csv")        # discard staged + local edits
"""
from __future__ import annotations

import difflib
import hashlib
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .client import EnergyDLMClient


class BranchAdvancedError(RuntimeError):
    """Raised when a commit fails because the branch HEAD has moved.

    Attributes:
        current_head: The HEAD commit SHA the server reports now.
    """

    def __init__(self, message: str, current_head: str = ""):
        super().__init__(message)
        self.current_head = current_head


class LocalRepo:
    """A local working copy of one (repository, branch) pair.

    Mirrors the Git mental model: clone gives you files on disk, status
    shows what changed, add stages a file, commit pushes it, restore
    throws away local edits.

    The constructor clones the branch immediately. All subsequent
    operations are against this local snapshot.
    """

    def __init__(
        self,
        repository: str,
        branch: str,
        path: str,
        owner: str = "anonymous",
        *,
        gateway_url: str = "",
        lakefs_url: str = "",
    ) -> None:
        if not repository or not repository.strip():
            raise ValueError("repository is required")
        if not branch or not branch.strip():
            raise ValueError("branch is required")
        if not path or not path.strip():
            raise ValueError("path is required")

        self.repository = repository
        self.branch = branch
        self.path = Path(path).resolve()
        self.owner = owner

        # Build the underlying client
        kwargs: dict = {"repository": repository}
        if gateway_url:
            kwargs["gateway_url"] = gateway_url
        if lakefs_url:
            kwargs["lakefs_url"] = lakefs_url
        self._client = EnergyDLMClient(**kwargs)

        # Clone the branch to populate the working directory
        self._cloned_files = self._client.clone_branch(
            branch, str(self.path),
        )

        # Record the SHA-256 of every cloned file as the "clean" baseline.
        # status() compares against this to detect local edits.
        self._baseline: Dict[str, str] = {}
        for abs_path in self._cloned_files:
            rel = str(Path(abs_path).relative_to(self.path))
            self._baseline[rel] = _sha256(Path(abs_path))

    # ── from_existing (alternate constructor) ───────────────────────────

    @classmethod
    def from_existing(
        cls,
        repository: str,
        branch: str,
        path: str,
        owner: str = "anonymous",
        *,
        download_missing: bool = True,
        verify_against_server: bool = False,
        gateway_url: str = "",
        lakefs_url: str = "",
    ) -> "LocalRepo":
        """Construct a LocalRepo over an existing on-disk working copy.

        Unlike ``__init__``, this method does **not** re-clone the branch
        from the gateway.  It reuses every file already on disk under
        ``path`` and uses their SHA-256 hashes as the baseline.  This
        makes "open an existing working copy and continue editing" instant
        and offline-friendly.

        Use ``LocalRepo(...)`` for the initial clone.  Use
        ``LocalRepo.from_existing(...)`` to reopen the same working copy
        in a later session without re-downloading.

        Parameters
        ----------
        repository : str
            The repository name (``X-CESDH-Repository`` header).
        branch : str
            The branch name.
        path : str
            The local working-copy directory.  Must already exist and
            contain at least one file.
        owner : str
            The user identity for subsequent commits.  Defaults to
            ``"anonymous"``.
        download_missing : bool
            If True (default), files listed by the server but absent
            locally are downloaded via the per-file endpoint.  If False,
            missing files are skipped silently.
        verify_against_server : bool
            If True (default False), every local file's SHA-256 is
            compared against the server HEAD.  Mismatches are printed as
            warnings to stderr.  The local SHA is still used as the
            baseline, so ``status()`` reports "unchanged" — drift is
            informational only.
        gateway_url : str
            Optional gateway URL override.
        lakefs_url : str
            Optional LakeFS URL override.

        Returns
        -------
        LocalRepo
            A fully initialised instance ready for ``status()`` /
            ``diff()`` / ``add()`` / ``commit()`` / ``restore()`` /
            ``reset()``.

        Raises
        ------
        FileNotFoundError
            If ``path`` does not exist or is not a directory.
        ValueError
            If ``path`` exists but contains no files, or if required
            arguments are blank.
        requests.exceptions.ConnectionError
            If ``download_missing=True`` and the gateway is unreachable.

        Examples
        --------
        Resume editing a previously cloned branch::

            repo = LocalRepo.from_existing(
                repository="project-alpha",
                branch="experiment-2030",
                path="./my-working-copy",
                owner="alice",
            )
            print(repo.status())
        """
        # ── validate ────────────────────────────────────────────────
        if not repository or not repository.strip():
            raise ValueError("repository is required")
        if not branch or not branch.strip():
            raise ValueError("branch is required")
        if not path or not path.strip():
            raise ValueError("path is required")

        resolved = Path(path).resolve()
        if not resolved.exists() or not resolved.is_dir():
            raise FileNotFoundError(
                f"Working copy path does not exist: {resolved}"
            )

        # ── walk local files ────────────────────────────────────────
        local_files: Dict[str, str] = {}
        for p in resolved.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(resolved))
                local_files[rel] = _sha256(p)

        if not local_files:
            raise ValueError(
                f"Working copy at {resolved} contains no files. "
                "Use LocalRepo(...) for the initial clone."
            )

        # ── build client ────────────────────────────────────────────
        kwargs: dict = {"repository": repository}
        if gateway_url:
            kwargs["gateway_url"] = gateway_url
        if lakefs_url:
            kwargs["lakefs_url"] = lakefs_url
        client = EnergyDLMClient(**kwargs)

        # ── download missing files ──────────────────────────────────
        if download_missing:
            import requests as _requests

            server_datasets = client.list_datasets(
                branch=branch, limit=1000,
            )
            local_ds_ids: set = set()
            for rel in local_files:
                try:
                    local_ds_ids.add(_dataset_id_from_path(rel))
                except ValueError:
                    pass

            for ds in server_datasets:
                ds_id = ds.get("dataset_id", "")
                if not ds_id or ds_id in local_ds_ids:
                    continue
                # Resolve the LakeFS URI to derive the local path
                resp = _requests.get(
                    f"{client.gateway_url}/datasets/{ds_id}/resolve",
                    headers=client._repo_headers(),
                    timeout=15,
                )
                resp.raise_for_status()
                lakefs_uri = resp.json().get("lakefs_uri", "")
                if not lakefs_uri:
                    continue
                # lakefs://repo/branch/tier/ds_id/file → tier/ds_id/file
                uri_body = lakefs_uri.replace("lakefs://", "")
                uri_parts = uri_body.split("/", 2)
                rel_path = uri_parts[2] if len(uri_parts) >= 3 else ""
                if (
                    not rel_path
                    or ".." in rel_path
                    or rel_path.startswith("/")
                ):
                    continue
                dest_path = str(resolved / rel_path)
                client._download_file_by_id(ds_id, dest_path)
                local_files[rel_path] = _sha256(resolved / rel_path)

        # ── verify against server ───────────────────────────────────
        if verify_against_server:
            import sys

            for rel_path, local_sha in list(local_files.items()):
                try:
                    ds_id = _dataset_id_from_path(rel_path)
                except ValueError:
                    continue
                try:
                    server_bytes = client._download_from_gateway(ds_id)
                except Exception:
                    continue
                server_sha = hashlib.sha256(server_bytes).hexdigest()
                if local_sha != server_sha:
                    print(
                        f"WARNING: {rel_path} differs from server HEAD "
                        f"(local={local_sha[:12]}... "
                        f"server={server_sha[:12]}...)",
                        file=sys.stderr,
                    )

        # ── construct instance ──────────────────────────────────────
        inst = object.__new__(cls)
        inst.repository = repository
        inst.branch = branch
        inst.path = resolved
        inst.owner = owner
        inst._client = client
        inst._cloned_files = [
            str((resolved / rel).resolve())
            for rel in sorted(local_files)
        ]
        inst._baseline = dict(local_files)
        return inst

    # ── status ──────────────────────────────────────────────────────────

    def status(self) -> pd.DataFrame:
        """Compare local files against the cloned baseline.

        Returns a DataFrame with columns: path, local_sha256, baseline_sha256,
        status. Status is one of: unchanged, modified, added, deleted.

        This is a purely local operation — no HTTP calls.
        """
        rows: List[dict] = []

        # Walk every file currently on disk under self.path
        local_files: Dict[str, str] = {}
        for p in self.path.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(self.path))
                local_files[rel] = _sha256(p)

        # Check every file in the baseline
        for rel, baseline_sha in self._baseline.items():
            local_sha = local_files.pop(rel, None)
            if local_sha is None:
                rows.append({
                    "path": rel,
                    "local_sha256": None,
                    "baseline_sha256": baseline_sha,
                    "status": "deleted",
                })
            elif local_sha != baseline_sha:
                rows.append({
                    "path": rel,
                    "local_sha256": local_sha,
                    "baseline_sha256": baseline_sha,
                    "status": "modified",
                })
            else:
                rows.append({
                    "path": rel,
                    "local_sha256": local_sha,
                    "baseline_sha256": baseline_sha,
                    "status": "unchanged",
                })

        # Files on disk that weren't in the baseline — locally added
        for rel, local_sha in local_files.items():
            rows.append({
                "path": rel,
                "local_sha256": local_sha,
                "baseline_sha256": None,
                "status": "added",
            })

        return pd.DataFrame(
            rows, columns=["path", "local_sha256", "baseline_sha256", "status"]
        )

    # ── diff ────────────────────────────────────────────────────────────

    def diff(self, path: str) -> str:
        """Return a unified diff of a local file against its baseline.

        For text files: returns a unified diff string.
        For binary files: returns a one-line summary.
        If the file doesn't exist locally: returns a deletion summary.
        If the file doesn't exist in the baseline: returns an addition summary.
        """
        local = self.path / path
        baseline_path = self.path / path

        # Read the baseline version from the stored clone
        if path not in self._baseline:
            if local.exists():
                size = local.stat().st_size
                return f"<new file: {size} bytes>"
            return "<file not found>"

        if not local.exists():
            return f"<deleted: was {path}>"

        # Try text diff
        try:
            local_text = local.read_text(encoding="utf-8")
        except (UnicodeDecodeError, ValueError):
            # Binary file
            local_size = local.stat().st_size
            return f"<binary file: {local_size} bytes local>"

        # We need the original bytes. Re-read from the clone baseline.
        # The baseline was the clone content — the file on disk may have been
        # modified. We stored only the SHA, not the content. For diff, we need
        # to download the server version.
        try:
            dataset_id = _dataset_id_from_path(path)
            server_bytes = self._client._download_from_gateway(dataset_id)
            server_text = server_bytes.decode("utf-8")
        except Exception:
            return f"<could not fetch server version for diff>"

        local_lines = local_text.splitlines(keepends=True)
        server_lines = server_text.splitlines(keepends=True)

        diff_lines = difflib.unified_diff(
            server_lines, local_lines,
            fromfile=f"a/{path} (server)",
            tofile=f"b/{path} (local)",
        )
        result = "".join(diff_lines)
        return result if result else "<no differences>"

    # ── add (stage) ─────────────────────────────────────────────────────

    def add(self, path: str) -> dict:
        """Stage a local file for commit (upload without committing).

        ``path`` is relative to self.path, e.g. ``raw/dataset_abc/foo.csv``.
        """
        dataset_id = _dataset_id_from_path(path)
        abs_path = str(self.path / path)
        return self._client.stage_dataset_update(
            dataset_id, abs_path, branch=self.branch,
        )

    def add_all(self) -> List[dict]:
        """Stage every modified or added file."""
        st = self.status()
        changed = st[st["status"].isin(["modified", "added"])]
        results = []
        for _, row in changed.iterrows():
            results.append(self.add(row["path"]))
        return results

    # ── commit ──────────────────────────────────────────────────────────

    def commit(self, message: str) -> List[dict]:
        """Commit all staged changes.

        Calls commit_dataset_update for each staged dataset. Uses the
        parent_commit_sha from get_staged_changes for OCC.

        Raises BranchAdvancedError if any commit gets a 409.
        """
        if not message or not message.strip():
            raise ValueError("commit message is required")

        # Get all staged changes on the branch
        import requests as _requests

        # We need to find which dataset_ids have staged changes.
        # Use the branch-level uncommitted endpoint if available,
        # otherwise fall back to checking known files.
        staged_datasets: List[str] = []
        st = self.status()
        for _, row in st[st["status"].isin(["modified", "added"])].iterrows():
            try:
                ds_id = _dataset_id_from_path(row["path"])
                if ds_id not in staged_datasets:
                    staged_datasets.append(ds_id)
            except ValueError:
                continue

        results = []
        for ds_id in staged_datasets:
            # Get the parent commit SHA for OCC
            try:
                staged = self._client.get_staged_changes(
                    ds_id, branch=self.branch,
                )
                parent = staged.get("parent_commit_sha")
            except Exception:
                parent = None

            try:
                result = self._client.commit_dataset_update(
                    ds_id, message, branch=self.branch,
                    parent_commit_sha=parent,
                )
                results.append(result)
                # After a successful commit, refresh the baseline SHA for
                # every local path that maps to this dataset_id so that
                # status() reports clean (mirrors git commit → git status).
                for rel_path in list(self._baseline.keys()):
                    try:
                        if _dataset_id_from_path(rel_path) != ds_id:
                            continue
                    except ValueError:
                        continue
                    local = self.path / rel_path
                    if local.exists():
                        self._baseline[rel_path] = _sha256(local)
            except _requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 409:
                    raise BranchAdvancedError(
                        f"Branch '{self.branch}' HEAD advanced while committing "
                        f"{ds_id}. Re-clone or re-stage.",
                        current_head=exc.response.text[:200],
                    ) from exc
                raise

        return results

    # ── restore ─────────────────────────────────────────────────────────

    def restore(self, path: str) -> None:
        """Discard staged changes and restore the file to its baseline state.

        Calls discard_staged_changes on the server, then overwrites the
        local file with the server version.
        """
        dataset_id = _dataset_id_from_path(path)
        self._client.discard_staged_changes(dataset_id, branch=self.branch)

        # Re-download the server version to restore the local file
        local = self.path / path
        try:
            server_bytes = self._client._download_from_gateway(dataset_id)
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(server_bytes)
            # Update baseline
            self._baseline[path] = _sha256(local)
        except Exception:
            pass  # discard succeeded on server; local restore is best-effort

    def restore_all(self) -> None:
        """Discard all staged changes and restore all files."""
        st = self.status()
        for _, row in st[st["status"].isin(["modified", "deleted"])].iterrows():
            try:
                self.restore(row["path"])
            except Exception:
                continue

    # ── reset ───────────────────────────────────────────────────────────

    def reset(self, *, force: bool = False) -> List[str]:
        """Re-clone the branch, replacing all local files.

        This is destructive — all local edits are lost. Requires
        ``force=True`` to prevent accidental data loss.

        Returns the list of cloned file paths (same as __init__).
        """
        if not force:
            raise ValueError(
                "reset() destroys all local edits. "
                "Pass force=True to confirm."
            )
        # Remove the local directory and re-clone
        if self.path.exists():
            shutil.rmtree(self.path)

        self._cloned_files = self._client.clone_branch(
            self.branch, str(self.path),
        )
        self._baseline = {}
        for abs_path in self._cloned_files:
            rel = str(Path(abs_path).relative_to(self.path))
            self._baseline[rel] = _sha256(Path(abs_path))

        return self._cloned_files


# ── Module-level helpers ──────────────────────────────────────────────────


def _sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _dataset_id_from_path(rel_path: str) -> str:
    """Extract dataset_id from the tier/[subpath/]dataset_id/filename layout.

    With subpaths the layout becomes ``tier/seg1/.../segN/dataset_id/filename``.
    The dataset_id is always the segment that starts with ``dataset_``.
    Raises ValueError if no such segment is found.
    """
    parts = Path(rel_path).parts
    for part in parts:
        if part.startswith("dataset_"):
            return part
    raise ValueError(
        f"Cannot derive dataset_id from path {rel_path!r}: "
        f"no segment starting with 'dataset_'"
    )
