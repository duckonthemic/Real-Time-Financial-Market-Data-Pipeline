"""Atomic artifact I/O, identity validation, and the exclusive demo lock."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from contextlib import AbstractContextManager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .errors import ConfigFailure

SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def artifact(artifact_type: str, run_id: str, **values: Any) -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "updated_at": utc_now(),
        **values,
    }


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def append_jsonl(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def read_artifact(path: Path, *, run_id: str, artifact_type: str | None = None) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigFailure(f"Cannot read artifact {path}: {exc}", phase="artifact-read") from exc
    if not isinstance(value, dict):
        raise ConfigFailure(f"Artifact {path} must contain a JSON object", phase="artifact-read")
    if value.get("schema_version") != SCHEMA_VERSION or value.get("run_id") != run_id:
        raise ConfigFailure(f"Artifact identity/version mismatch: {path}", phase="artifact-read")
    if artifact_type is not None and value.get("artifact_type") != artifact_type:
        raise ConfigFailure(f"Artifact type mismatch: {path}", phase="artifact-read")
    return value


def safe_run_directory(root: Path, run_id: str, *, must_exist: bool = False) -> Path:
    if not run_id or any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in run_id
    ):
        raise ConfigFailure("Unsafe run_id for artifact path", phase="cleanup")
    resolved_root = root.resolve()
    candidate = (resolved_root / run_id).resolve()
    if candidate.parent != resolved_root:
        raise ConfigFailure("Run artifact path escapes configured root", phase="cleanup")
    if must_exist and (not candidate.is_dir() or candidate.is_symlink()):
        raise ConfigFailure("Run artifact directory is missing or is a link", phase="cleanup")
    return candidate


class DemoLock(AbstractContextManager["DemoLock"]):
    def __init__(self, path: Path, run_id: str):
        self.path = path
        self.run_id = run_id
        self._fd: int | None = None

    def __enter__(self) -> DemoLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            owner = self.path.read_text(encoding="utf-8", errors="replace").strip()
            raise ConfigFailure(
                f"Another demo lock exists ({owner or 'unknown owner'}). Confirm no run is active, then remove {self.path}.",
                phase="lock",
            ) from exc
        os.write(self._fd, f"run_id={self.run_id}\npid={os.getpid()}\n".encode())
        os.fsync(self._fd)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        with suppress(FileNotFoundError):
            self.path.unlink()


def write_primary_failure(
    path: Path, failure: Mapping[str, Any], cleanup_error: str | None = None
) -> None:
    if path.exists():
        current = json.loads(path.read_text(encoding="utf-8"))
        if cleanup_error:
            current.setdefault("cleanup_errors", []).append(cleanup_error[:1000])
            atomic_write_json(path, current)
        return
    value = dict(failure)
    value.setdefault("cleanup_errors", [])
    atomic_write_json(path, value)
