from pg_compose_cli.extract import extract_build_queries
from pg_compose_cli.diff import diff_schemas
from pg_compose_cli.pgdump import extract_from_postgres
import json
import os
from typing import Optional, List


def load_source(source: str, schemas: Optional[List[str]] = None, grants: bool = True) -> list[dict]:
    """Load schema objects from a file, connection string, directory, or raw SQL string."""
    if source.startswith("postgres://"):
        return extract_from_postgres(source, schemas=schemas)

    # Handle git URLs
    if source.startswith("git://") or source.startswith("git@") or source.startswith("https://github.com/"):
        from pg_compose_cli.git import extract_from_git_repo
        
        # Parse target path and branch from URL if present
        target_path = None
        branch = None
        # If #branch is present, split it off
        if "#" in source:
            source, branch = source.rsplit("#", 1)
        if "/" in source and not source.endswith(".git"):
            # Handle URLs with .git/ in them
            if ".git/" in source:
                base_url, target_path = source.split(".git/", 1)
                source = base_url + ".git"
            # Handle URLs with /tree/branch/path format
            elif "/tree/" in source:
                base_url, tree_part = source.split("/tree/", 1)
                if "/" in tree_part:
                    branch, target_path = tree_part.split("/", 1)
                    source = f"{base_url}#{branch}"
                else:
                    # Just a branch, no path
                    source = f"{base_url}#{tree_part}"
                    target_path = None
            else:
                # Handle case where .git is in the URL but not at the end
                if ".git" in source:
                    # Find the .git part and split there
                    git_index = source.find(".git")
                    if git_index != -1:
                        base_url = source[:git_index + 4]  # Include .git
                        target_path = source[git_index + 5:]  # Skip the / after .git
                        source = base_url
                else:
                    # Handle case where .git is not in URL at all
                    parts = source.split("/", 3)  # Split into max 4 parts
                    if len(parts) >= 4:
                        source = "/".join(parts[:3])  # First 3 parts are the repo URL
                        target_path = "/".join(parts[3:])  # Rest is the target directory
        # If we parsed a branch, append it to the repo URL
        if branch:
            source = f"{source}#{branch}"
        # Get the working directory from git module using context manager
        git_context = extract_from_git_repo(source, target_path)
        with git_context as working_dir:
            # Check if working_dir is a file or directory
            if os.path.isfile(working_dir):
                # It's a single file, read it directly
                with open(working_dir, "r", encoding="utf-8") as f:
                    objs = extract_build_queries(f.read())
            else:
                # It's a directory, merge all SQL files
                from pg_compose_cli.merge import merge_sql
                import tempfile
                
                # Create a temporary directory for the merged file
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Merge all SQL files in the working directory (no subdirectory filter)
                    merge_sql(working_dir, temp_dir)
                    
                    # Read the merged file
                    sorted_path = os.path.join(temp_dir, "sorted.sql")
                    if os.path.exists(sorted_path):
                        with open(sorted_path, "r", encoding="utf-8") as f:
                            objs = extract_build_queries(f.read())
                    else:
                        # If no SQL files found, return empty list
                        objs = []
            if not grants:
                objs = [o for o in objs if o.get("query_type") != "grant"]
            return objs

    if os.path.isfile(source):
        if source.endswith(".sql"):
            with open(source, "r", encoding="utf-8") as f:
                objs = extract_build_queries(f.read())
        elif source.endswith(".json"):
            with open(source, "r", encoding="utf-8") as f:
                objs = json.load(f)
        if not grants:
            objs = [o for o in objs if o.get("query_type") != "grant"]
        return objs
    elif os.path.isdir(source):
        # Handle directory by merging all SQL files
        from pg_compose_cli.merge import merge_sql
        import tempfile
        
        # Create a temporary directory for the merged file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Merge all SQL files in the directory (no subdirectory filter)
            merge_sql(source, temp_dir)
            
            # Read the merged file
            sorted_path = os.path.join(temp_dir, "sorted.sql")
            if os.path.exists(sorted_path):
                with open(sorted_path, "r", encoding="utf-8") as f:
                    objs = extract_build_queries(f.read())
            else:
                # If no SQL files found, return empty list
                objs = []
        if not grants:
            objs = [o for o in objs if o.get("query_type") != "grant"]
        return objs

    # Assume raw SQL string
    objs = extract_build_queries(source)
    if not grants:
        objs = [o for o in objs if o.get("query_type") != "grant"]
    return objs


def compare_sources(
    source_a: str,
    source_b: str,
    *,
    schemas: Optional[List[str]] = None,
    verbose: bool = True,
    grants: bool = True
) -> dict:
    """Compare two schema sources: .sql, .json, raw SQL, or postgres:// URIs."""
    
    schema_a = load_source(source_a, schemas=schemas, grants=grants)
    schema_b = load_source(source_b, schemas=schemas, grants=grants)

    result = diff_schemas(schema_a, schema_b)

    if verbose:
        print("\nSchema Diff Results\n" + "=" * 40)
        for obj in result["created"]:
            print(f"[CREATE] {obj['query_type']} {obj['object_name']}")
        for obj in result["dropped"]:
            print(f"[DROP]   {obj['query_type']} {obj['object_name']}")
        for obj in result["changed"]:
            print(f"[CHANGE] {obj['query_type']} {obj['object_name']}")
            if obj.get("ast_diff"):
                print("         - AST diff:")
                ast_diff = obj["ast_diff"]
                
                # Handle table changes
                for col in ast_diff.get("add_columns", []):
                    print(f"            + column: {col}")
                for col in ast_diff.get("drop_columns", []):
                    print(f"            - column: {col}")
                for col in ast_diff.get("change_columns", []):
                    print(f"            ~ column: {col['column']}")
                    if col.get("type"):
                        print(f"              type: {col['type']['from']} → {col['type']['to']}")
                    if col.get("nullable"):
                        print(f"              nullable: {col['nullable']['from']} → {col['nullable']['to']}")
                    if col.get("default"):
                        print(f"              default: {col['default']['from']} → {col['default']['to']}")
                
                # Handle other changes
                if ast_diff.get("query_changed"):
                    print("            ~ query changed")
                if ast_diff.get("function_changed"):
                    print("            ~ function changed")
                if ast_diff.get("ast_changed"):
                    print("            ~ AST structure changed")
        print("=" * 40)

    return result
