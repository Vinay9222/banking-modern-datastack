"""
Unit tests to validate connector configurations and project schemas.
"""

from pathlib import Path
import yaml


def test_postgres_schema_exists():
    schema_file = Path(__file__).parent.parent / "postgres" / "schema.sql"
    assert schema_file.exists()
    content = schema_file.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS customers" in content
    assert "CREATE TABLE IF NOT EXISTS accounts" in content
    assert "CREATE TABLE IF NOT EXISTS transactions" in content


def test_dbt_project_file_validity():
    dbt_proj_file = Path(__file__).parent.parent / "banking_dbt" / "dbt_project.yml"
    assert dbt_proj_file.exists()
    with open(dbt_proj_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["name"] == "banking_dbt"
    assert data["profile"] == "banking_dbt"


def test_dbt_sources_validity():
    sources_file = Path(__file__).parent.parent / "banking_dbt" / "models" / "source.yml"
    assert sources_file.exists()
    with open(sources_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    tables = [t["name"] for t in data["sources"][0]["tables"]]
    assert "customers" in tables
    assert "accounts" in tables
    assert "transactions" in tables
