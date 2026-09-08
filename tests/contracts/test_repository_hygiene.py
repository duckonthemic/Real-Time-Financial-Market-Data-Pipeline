from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_new_package_initializers_do_not_eager_import_runtime_dependencies() -> None:
    for init_file in (ROOT / "src" / "market_pipeline").rglob("__init__.py"):
        text = init_file.read_text(encoding="utf-8")
        assert "pyspark" not in text
        assert "confluent_kafka" not in text
        assert "cassandra" not in text


def test_runtime_files_do_not_use_ambient_dotenv() -> None:
    offenders = []
    for path in (ROOT / "src" / "market_pipeline").rglob("*.py"):
        if "load_dotenv" in path.read_text(encoding="utf-8"):
            offenders.append(path)
    assert offenders == []


def test_env_example_does_not_advertise_ignored_assignments() -> None:
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "does not load an" in example
    assert not any(
        line and not line.startswith("#") and "=" in line for line in example.splitlines()
    )


def test_docker_context_is_an_explicit_allowlist() -> None:
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert dockerignore[0] == "**"
    assert "!docker/**" in dockerignore
    assert "!src/**" in dockerignore
    assert not any("artifacts" in line for line in dockerignore[1:])


def test_generated_evidence_ignore_does_not_hide_artifact_schemas() -> None:
    ignore_lines = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "/artifacts/" in ignore_lines
    assert "artifacts/" not in ignore_lines
    assert (ROOT / "schemas" / "artifacts" / "run.schema.json").is_file()


def test_repository_has_no_unsafe_cassandra_queries_in_new_implementation() -> None:
    roots = [ROOT / "src" / "market_pipeline", ROOT / "schemas" / "cassandra"]
    offenders = []
    for root in roots:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in {".py", ".cql", ".json"}:
                text = path.read_text(encoding="utf-8").upper()
                if "ALLOW FILTERING" in text or "CREATE INDEX" in text:
                    offenders.append(path)
    assert offenders == []


def test_authoritative_config_owns_runtime_versions() -> None:
    demo = (ROOT / "config" / "demo.toml").read_text(encoding="utf-8")
    assert "confluentinc/cp-kafka:7.7.2" in demo
    assert "apache/spark:3.5.5" in demo
    assert "cassandra:4.1.7" in demo


def test_silver_timestamp_preserves_manifest_milliseconds() -> None:
    source = (ROOT / "src" / "market_pipeline" / "streaming" / "spark_pipeline.py").read_text(
        encoding="utf-8"
    )
    assert "timestamp_millis" in source
    assert "from_unixtime" not in source
