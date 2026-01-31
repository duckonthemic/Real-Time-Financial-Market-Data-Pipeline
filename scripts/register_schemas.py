#!/usr/bin/env python3
"""
Schema Registration Script
Registers Avro schemas with Confluent Schema Registry.
"""
import json
import os
import sys
from pathlib import Path

import requests

# Default Schema Registry URL
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Schema files to register
SCHEMAS = {
    "trades_raw-value": "schemas/avro/trade.avsc",
    "quotes_raw-value": "schemas/avro/quote.avsc",
}


def read_schema(schema_path: str) -> dict:
    """Read Avro schema from file."""
    project_root = Path(__file__).parent.parent
    full_path = project_root / schema_path
    
    with open(full_path, "r") as f:
        return json.load(f)


def register_schema(subject: str, schema: dict) -> bool:
    """
    Register a schema with Schema Registry.
    
    Args:
        subject: Subject name (e.g., "trades_raw-value")
        schema: Avro schema dictionary
        
    Returns:
        True if registration successful
    """
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    
    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json"
    }
    
    payload = {
        "schema": json.dumps(schema)
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code in (200, 201):
            result = response.json()
            print(f"✅ Registered {subject} (id: {result.get('id')})")
            return True
        elif response.status_code == 409:
            print(f"⚠️  Schema {subject} already exists (compatible)")
            return True
        else:
            print(f"❌ Failed to register {subject}: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.RequestException as e:
        print(f"❌ Connection error for {subject}: {e}")
        return False


def set_compatibility(subject: str, level: str = "BACKWARD") -> bool:
    """Set compatibility level for a subject."""
    url = f"{SCHEMA_REGISTRY_URL}/config/{subject}"
    
    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json"
    }
    
    payload = {"compatibility": level}
    
    try:
        response = requests.put(url, headers=headers, json=payload)
        return response.status_code == 200
    except requests.RequestException:
        return False


def check_registry_health() -> bool:
    """Check if Schema Registry is accessible."""
    try:
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        return response.status_code == 200
    except requests.RequestException:
        return False


def main() -> int:
    """Main entry point."""
    print("=" * 50)
    print("Schema Registry Registration")
    print("=" * 50)
    print(f"Registry URL: {SCHEMA_REGISTRY_URL}")
    print()
    
    # Check connectivity
    if not check_registry_health():
        print("❌ Cannot connect to Schema Registry")
        print("   Make sure it's running: docker-compose up -d schema-registry")
        return 1
    
    print("✅ Schema Registry is accessible")
    print()
    
    # Register schemas
    success_count = 0
    for subject, schema_path in SCHEMAS.items():
        try:
            schema = read_schema(schema_path)
            if register_schema(subject, schema):
                set_compatibility(subject, "BACKWARD")
                success_count += 1
        except FileNotFoundError:
            print(f"❌ Schema file not found: {schema_path}")
        except json.JSONDecodeError:
            print(f"❌ Invalid JSON in: {schema_path}")
    
    print()
    print(f"Registered {success_count}/{len(SCHEMAS)} schemas")
    
    return 0 if success_count == len(SCHEMAS) else 1


if __name__ == "__main__":
    sys.exit(main())
