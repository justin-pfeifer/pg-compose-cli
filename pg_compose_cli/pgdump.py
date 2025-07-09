import subprocess
from pg_compose_cli.extract import extract_build_queries
from typing import Optional, List

def extract_from_postgres(conn_str: str, schemas: Optional[List[str]] = None) -> list[dict]:
    """Extracts schema DDL from a PostgreSQL connection using pg_dump, with optional schema filter."""
    cmd = ["pg_dump", "--schema-only"]

    if schemas:
        for schema in schemas:
            cmd += ["--schema", schema]

    cmd.append(conn_str)

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"pg_dump failed:\n{e.stderr.decode()}")

    return extract_build_queries(result.stdout.decode())
