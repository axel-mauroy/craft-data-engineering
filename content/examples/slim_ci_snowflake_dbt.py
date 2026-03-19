# slim_ci_snowflake_dbt.py
import os
import snowflake.connector # Or your provider
from pathlib import Path

def create_ephemeral_schema(pr_number: str):
    """
    Creates a dedicated schema for the PR to ensure isolation.
    """
    schema_name = f"CI_PR_{pr_number}"
    conn = snowflake.connector.connect(...)
    
    # We use 'OR REPLACE' to ensure a clean state
    conn.cursor().execute(f"CREATE OR REPLACE SCHEMA {schema_name}")
    print(f"✅ Isolated environment created: {schema_name}")
    return schema_name

def run_slim_tests(schema_name: str):
    """
    Runs transformations ONLY on modified models.
    """
    # Example using dbt state comparison
    # It only runs models that changed vs the 'prod' manifest
    os.system(f"dbt run --select state:modified+ --target ci --vars '{{schema: {schema_name}}}'")
    os.system(f"dbt test --select state:modified+")

if __name__ == "__main__":
    pr_id = os.getenv("GITHUB_PR_NUMBER", "local_test")
    schema = create_ephemeral_schema(pr_id)
    run_slim_tests(schema)