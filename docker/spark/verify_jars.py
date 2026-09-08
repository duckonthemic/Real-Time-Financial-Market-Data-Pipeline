"""Verify the Maven-resolved connector closure against the committed lock file."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

DEFAULT_LOCK = Path("/opt/market-pipeline/docker/spark/jars.lock.json")
DEFAULT_JAR_ROOT = Path("/opt/spark/jars")


def sha256(path: Path) -> str:
    """Return a streaming SHA-256 digest without loading a large JAR into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_expected(lock_path: Path) -> list[dict[str, Any]]:
    """Load and minimally validate the stable lock-file contract."""
    payload = json.loads(lock_path.read_text(encoding="utf-8"))
    if payload.get("lock_version") != 1 or not isinstance(payload.get("jars"), list):
        raise ValueError("unsupported or malformed JAR lock")
    return payload["jars"]


def verify(lock_path: Path, jar_root: Path) -> list[str]:
    """Return every missing, size-mismatched, or digest-mismatched locked JAR."""
    errors: list[str] = []
    for entry in load_expected(lock_path):
        name = entry.get("file")
        expected_size = entry.get("bytes")
        expected_digest = entry.get("sha256")
        if (
            not isinstance(name, str)
            or not isinstance(expected_size, int)
            or not isinstance(expected_digest, str)
        ):
            errors.append(f"invalid lock entry: {entry!r}")
            continue
        path = jar_root / name
        if not path.is_file():
            errors.append(f"missing: {name}")
            continue
        actual_size = path.stat().st_size
        if actual_size != expected_size:
            errors.append(f"size mismatch: {name} (expected {expected_size}, got {actual_size})")
            continue
        actual_digest = sha256(path)
        if actual_digest != expected_digest:
            errors.append(
                f"SHA-256 mismatch: {name} (expected {expected_digest}, got {actual_digest})"
            )
    return errors


def main() -> int:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    command.add_argument("--jar-root", type=Path, default=DEFAULT_JAR_ROOT)
    args = command.parse_args()
    errors = verify(args.lock, args.jar_root)
    if errors:
        print(json.dumps({"status": "FAIL", "errors": errors}, indent=2))
        return 1
    print(json.dumps({"status": "PASS", "verified_jars": len(load_expected(args.lock))}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
