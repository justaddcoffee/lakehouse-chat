#!/usr/bin/env python3
"""Generate LinkML schemas from KBase/BERDL database metadata."""

import json
import os
import subprocess
import yaml
from pathlib import Path

KBASE_TOKEN = os.environ.get("KBASE_TOKEN")
KBASE_MCP_URL = os.environ.get("KBASE_MCP_URL", "https://hub.berdl.kbase.us/apis/mcp")

# Databases to generate schemas for (excluding test/demo databases)
TARGET_DATABASES = [
    "kbase_ke_pangenome",
    "kbase_genomes",
    "kbase_msd_biochemistry",
    "kbase_ontology_source",
    "kbase_phenotype",
    "kbase_refseq_taxon_api",
    "kbase_uniprot_archaea",
    "kbase_uniprot_bacteria",
    "kbase_uniref50",
    "kbase_uniref90",
    "kbase_uniref100",
    "nmdc_core",
]


def run_script(script_name: str, *args) -> dict:
    """Run a kbase script and return JSON result."""
    script_path = Path(__file__).parent.parent / "kbase-query" / "scripts" / script_name
    cmd = [str(script_path)] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, env=os.environ)
    if result.returncode != 0:
        raise RuntimeError(f"Script failed: {result.stderr}")
    return json.loads(result.stdout)


def get_tables(database: str) -> list[str]:
    """Get list of tables in a database."""
    result = run_script("kbase_list_tables.sh", database)
    return result.get("tables", [])


def get_schema(database: str, table: str) -> list[str]:
    """Get column names for a table."""
    result = run_script("kbase_table_schema.sh", database, table)
    return result.get("columns", [])


def to_linkml_schema(database: str, tables_columns: dict[str, list[str]]) -> dict:
    """Convert database structure to LinkML schema."""
    schema = {
        "id": f"https://w3id.org/kbase/{database}",
        "name": database,
        "prefixes": {
            "linkml": "https://w3id.org/linkml/",
            "kbase": f"https://w3id.org/kbase/{database}/",
        },
        "default_range": "string",
        "imports": ["linkml:types"],
        "classes": {},
    }

    for table, columns in tables_columns.items():
        # Convert table name to PascalCase class name
        class_name = "".join(word.capitalize() for word in table.split("_"))

        attrs = {}
        for col in columns:
            attr = {"range": "string"}
            # Guess if it's an identifier
            if col.endswith("_id") or col == "id":
                attr["identifier"] = True if col == f"{table}_id" or col == "id" else False
            attrs[col] = attr

        schema["classes"][class_name] = {
            "attributes": attrs,
            "annotations": {"source_table": table},
        }

    return schema


def main():
    output_dir = Path(__file__).parent

    for database in TARGET_DATABASES:
        print(f"Processing {database}...")

        try:
            tables = get_tables(database)
            print(f"  Found {len(tables)} tables")

            tables_columns = {}
            for table in tables:
                try:
                    columns = get_schema(database, table)
                    tables_columns[table] = columns
                    print(f"    {table}: {len(columns)} columns")
                except Exception as e:
                    print(f"    {table}: ERROR - {e}")

            if tables_columns:
                schema = to_linkml_schema(database, tables_columns)
                output_file = output_dir / f"{database}.linkml.yaml"
                with open(output_file, "w") as f:
                    yaml.dump(schema, f, default_flow_style=False, sort_keys=False)
                print(f"  Wrote {output_file}")

        except Exception as e:
            print(f"  ERROR: {e}")


if __name__ == "__main__":
    main()
