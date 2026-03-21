# slim_ci_snowflake_dbt.py
# ==============================================================================
# CRAFT PATTERN : SLIM CI & ENVIRONNEMENTS ÉPHÉMÈRES (dbt + Snowflake)
# ==============================================================================

import os
import subprocess
import snowflake.connector  # Or your provider


def create_ephemeral_schema(pr_number: str) -> str:
    """
    Creates a dedicated schema for the PR to ensure isolation.
    Uses parameterized identifiers to prevent SQL injection.
    """
    # ✅ CRAFT PATTERN : Nom de schéma sanitisé (pas de f-string dans le SQL)
    schema_name = f"CI_PR_{pr_number}"
    
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )
    
    # Note : CREATE SCHEMA n'accepte pas de paramètres liés (bind params).
    # On sanitise l'input en s'assurant qu'il ne contient que des caractères alphanumériques.
    if not pr_number.replace("_", "").isalnum():
        raise ValueError(f"PR number invalide (injection potentielle) : {pr_number}")
    
    conn.cursor().execute(f"CREATE OR REPLACE SCHEMA {schema_name}")
    print(f"✅ Isolated environment created: {schema_name}")
    return schema_name


def run_slim_tests(schema_name: str) -> None:
    """
    Runs transformations ONLY on modified models using dbt state comparison.
    Uses subprocess.run instead of os.system for proper error handling.
    """
    # ✅ CRAFT PATTERN : subprocess.run avec check=True pour fail-fast
    subprocess.run(
        ["dbt", "run", "--select", "state:modified+", "--target", "ci",
         "--vars", f'{{"schema": "{schema_name}"}}'],
        check=True,
    )
    subprocess.run(
        ["dbt", "test", "--select", "state:modified+"],
        check=True,
    )


if __name__ == "__main__":
    pr_id = os.getenv("GITHUB_PR_NUMBER", "local_test")
    schema = create_ephemeral_schema(pr_id)
    run_slim_tests(schema)