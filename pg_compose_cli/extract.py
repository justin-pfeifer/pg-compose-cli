import hashlib
import re
from pglast import parse_sql
from typing import Literal

BuildStage = Literal[
    "extension",
    "schema",
    "enum",
    "domain",
    "composite_type",
    "base_table",
    "sequence",
    "dependent_table",
    "index",
    "constraint",
    "view",
    "materialized_view",
    "function",
    "trigger",
    "policy",
    "grant",
    "unknown"
]

POSTGRES_BUILTINS = {
    "text", "varchar", "char", "int", "int4", "int8", "integer", "bigint", "smallint",
    "serial", "bigserial", "boolean", "bool", "uuid", "numeric",
    "real", "double precision", "date", "timestamp", "timestamptz",
    "json", "jsonb", "bytea", "inet", "cidr", "time", "interval",
    "regclass", "name", "oid"
}

def normalize_sql(sql: str) -> str:
    """Normalize SQL by removing whitespace differences while preserving structure."""
    # Remove comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Normalize whitespace
    sql = re.sub(r'\s+', ' ', sql)  # Replace multiple whitespace with single space
    sql = re.sub(r'\s*([,()])\s*', r'\1', sql)  # Remove spaces around commas and parentheses
    sql = sql.strip()
    
    # Normalize case for keywords (optional, but helps with consistency)
    sql = sql.upper()
    
    return sql

def extract_build_queries(sql: str) -> list[dict]:
    results = []
    stmts = parse_sql(sql)

    for raw_stmt in stmts:
        node = raw_stmt.stmt
        typename = type(node).__name__

        query_type: BuildStage = "unknown"
        dependencies = []
        object_name = None

        if typename == "CreateExtensionStmt":
            query_type = "extension"

        elif typename == "CreateSchemaStmt":
            query_type = "schema"

        elif typename == "CreateEnumStmt":
            query_type = "enum"
            obj = getattr(node, "typeName", []) or []
            if obj:
                object_name = str(obj[-1].sval).lower()

        elif typename == "CreateDomainStmt":
            query_type = "domain"
            obj = getattr(node, "domainname", []) or []
            if obj:
                object_name = str(obj[-1].sval).lower()

        elif typename == "CompositeTypeStmt":
            query_type = "composite_type"
            obj = getattr(node, "typevar", None)
            if obj:
                object_name = obj.relname.lower()

        elif typename == "CreateStmt":
            constraints = getattr(node, "constraints", []) or []
            fk_found = any(getattr(c, "contype", None) == 5 for c in constraints)
            query_type = "dependent_table" if fk_found else "base_table"

            rel = getattr(node, "relation", None)
            if rel:
                object_name = rel.relname.lower()

            for col in getattr(node, "tableElts", []) or []:
                typename_node = getattr(col, "typeName", None)
                if typename_node:
                    names = getattr(typename_node, "names", []) or []
                    dependencies.extend(str(n.sval).lower() for n in names)

            for constraint in constraints:
                pktable = getattr(constraint, "pktable", None)
                if pktable:
                    dependencies.append(pktable.relname.lower())

        elif typename == "CreateSeqStmt":
            query_type = "sequence"
            rel = getattr(node, "sequence", None)
            if rel:
                object_name = rel.relname.lower()

        elif typename == "IndexStmt":
            query_type = "index"
            rel = getattr(node, "relation", None)
            if rel:
                dependencies.append(rel.relname.lower())
                object_name = f"idx_{rel.relname.lower()}"

        elif typename == "AlterTableStmt":
            query_type = "constraint"
            rel = getattr(node, "relation", None)
            if rel:
                dependencies.append(rel.relname.lower())
                object_name = f"alter_{rel.relname.lower()}"

        elif typename == "ViewStmt":
            query_type = "view"
            view = getattr(node, "view", None)
            if view:
                object_name = view.relname.lower()
            query = getattr(node, "query", None)
            if query and hasattr(query, "fromClause") and query.fromClause:
                for clause in query.fromClause:
                    relname = getattr(clause, "relname", None)
                    if relname:
                        dependencies.append(relname.lower())

        elif typename == "CreateTableAsStmt":
            objtype = getattr(node, "objtype", None)
            query_type = "materialized_view" if objtype == 23 else "base_table"
            into = getattr(node, "into", None)
            if into:
                # Handle different types of into clauses
                if hasattr(into, 'relname'):
                    object_name = into.relname.lower()
                elif hasattr(into, 'rel'):
                    object_name = into.rel.relname.lower()
            query = getattr(node, "query", None)
            if query and hasattr(query, "fromClause") and query.fromClause:
                for clause in query.fromClause:
                    relname = getattr(clause, "relname", None)
                    if relname:
                        dependencies.append(relname.lower())

        elif typename in ("CreateFunctionStmt", "CreateProcedureStmt"):
            query_type = "function"
            for arg in (getattr(node, "parameters", []) or []):
                type_name = getattr(arg, "argType", None)
                if type_name:
                    names = getattr(type_name, "names", []) or []
                    dependencies.extend(str(n.sval).lower() for n in names)
            funcname = getattr(node, "funcname", []) or []
            if funcname:
                object_name = str(funcname[-1].sval).lower()

        elif typename == "CreateTrigStmt":
            query_type = "trigger"
            rel = getattr(node, "relation", None)
            if rel:
                dependencies.append(rel.relname.lower())
                object_name = f"trigger_on_{rel.relname.lower()}"

        elif typename == "CreatePolicyStmt":
            query_type = "policy"
            rel = getattr(node, "table", None)
            if rel:
                dependencies.append(rel.relname.lower())
                object_name = f"policy_on_{rel.relname.lower()}"

        elif typename == "GrantStmt":
            query_type = "grant"
            objs = getattr(node, "objects", []) or []
            if objs and hasattr(objs[0], "relname"):
                object_name = f"grant_on_{objs[0].relname.lower()}"
                dependencies.append(objs[0].relname.lower())

        # Extract SQL slice and create normalized hash
        start = raw_stmt.stmt_location
        end = start + raw_stmt.stmt_len
        query_text = sql[start:end]
        normalized_sql = normalize_sql(query_text)
        query_hash = hashlib.sha256(normalized_sql.encode()).hexdigest()

        # Clean up dependency list
        filtered_deps = [
            d for d in dependencies
            if d not in POSTGRES_BUILTINS and not d.startswith("pg_")
        ]

        results.append({
            "query_type": query_type,
            "object_name": object_name,
            "query_start_pos": start,
            "query_end_pos": end,
            "query_hash": query_hash,
            "query_text": query_text,
            "dependencies": sorted(set(filtered_deps))
        })

    return results
