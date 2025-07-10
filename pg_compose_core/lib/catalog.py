import psycopg
from typing import Generator, Optional

def extract_from_catalog(
    conn_str: str,
    schemas: Optional[list[str]] = None
) -> Generator[dict, None, None]:
    """
    Yields schema objects from PostgreSQL catalogs one at a time for progress tracking.
    """
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            # Apply schema filter if provided
            schema_filter = " AND n.nspname = ANY(%s)" if schemas else ""

            # === Example: Base Tables ===
            cur.execute(f"""
                SELECT c.oid, c.relname, n.nspname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r' {schema_filter}
            """, (schemas,) if schemas else None)

            for oid, relname, nspname in cur.fetchall():
                # Fetch columns
                cur.execute("""
                    SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull
                    FROM pg_attribute a
                    WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                """, (oid,))
                columns = [
                    {"name": name, "type": typ, "nullable": not notnull}
                    for name, typ, notnull in cur.fetchall()
                ]

                yield {
                    "query_type": "base_table",
                    "object_name": relname,
                    "schema_name": nspname,
                    "columns": columns,
                    "dependencies": [],
                }