from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]
LOCK = ROOT / "docker" / "spark" / "jars.lock.json"
VERIFIER = ROOT / "docker" / "spark" / "verify_jars.py"
DOCKERFILE = ROOT / "docker" / "spark" / "Dockerfile"


def test_spark_lock_has_unique_sha256_pinned_closure() -> None:
    payload = json.loads(LOCK.read_text(encoding="utf-8"))
    jars = payload["jars"]
    names = [entry["file"] for entry in jars]
    assert payload["lock_version"] == 1
    assert len(jars) == 30
    assert len(names) == len(set(names))
    assert all(entry["bytes"] > 0 for entry in jars)
    assert all(len(entry["sha256"]) == 64 for entry in jars)


def test_spark_lock_verifier_detects_tampering(tmp_path: Path) -> None:
    jar_root = tmp_path / "jars"
    jar_root.mkdir()
    jar = jar_root / "example.jar"
    jar.write_bytes(b"locked bytes")
    lock = tmp_path / "lock.json"
    lock.write_text(
        json.dumps(
            {
                "lock_version": 1,
                "jars": [
                    {
                        "file": jar.name,
                        "bytes": jar.stat().st_size,
                        "sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    passing = subprocess.run(
        [sys.executable, str(VERIFIER), "--lock", str(lock), "--jar-root", str(jar_root)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert passing.returncode == 0
    assert '"status": "PASS"' in passing.stdout

    jar.write_bytes(b"tampered")
    failing = subprocess.run(
        [sys.executable, str(VERIFIER), "--lock", str(lock), "--jar-root", str(jar_root)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert failing.returncode == 1
    assert "size mismatch" in failing.stdout


def test_spark_image_pins_python_and_smoke_imports_streaming_entrypoint() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert "PYTHON_BASE_IMAGE=python:3.11.10-slim-bullseye" in dockerfile
    assert "PYSPARK_PYTHON=/usr/local/bin/python3.11" in dockerfile
    assert "python3 -m compileall -q /opt/market-pipeline/src" in dockerfile
    assert "import market_pipeline.streaming.main" in dockerfile
