"""Emit a deterministic digest inventory for every non-base connector JAR."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


jar_root = Path("/opt/spark/jars")
inventory = []
for path in sorted(jar_root.glob("*.jar")):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    inventory.append({"file": path.name, "sha256": digest, "bytes": path.stat().st_size})
print(json.dumps({"lock_version": 1, "jars": inventory}, indent=2, sort_keys=True))
