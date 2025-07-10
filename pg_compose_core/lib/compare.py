from pg_compose_core.lib.extract import extract_build_queries
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.pgdump import extract_from_postgres
from pg_compose_core.lib.ast_objects import ASTList, BuildStage
import json
import os
from typing import Optional, List, Union




def load_source(source: str, schemas: Optional[List[str]] = None, grants: bool = True, use_ast_objects: bool = True) -> Union[List[dict], ASTList]:
    """Load schema objects from a file, connection string, directory, or raw SQL string."""
    if source.startswith("postgres://"):
        objs = extract_from_postgres(source, schemas=schemas)
        if use_ast_objects:
            return ASTList.from_dict_list(objs)
        return objs

    # Handle git URLs
    if source.startswith("git://") or source.startswith("git@") or source.startswith("https://github.com/"):
        from pg_compose_core.lib.git import extract_from_git_repo
        
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
                    objs = extract_build_queries(f.read(), use_ast_objects=use_ast_objects)
            else:
                # It's a directory, merge all SQL files
                from pg_compose_core.lib.merge import merge_sql
                import tempfile
                
                # Create a temporary directory for the merged file
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Merge all SQL files in the working directory (no subdirectory filter)
                    ast_list = merge_sql(working_dir, temp_dir)
                    if use_ast_objects:
                        objs = ast_list
                    else:
                        objs = ast_list.to_dict_list()
            
            if not grants:
                if use_ast_objects:
                    objs = ASTList([o for o in objs if o.query_type.value != "grant"])
                else:
                    objs = [o for o in objs if o.get("query_type") != "grant"]
            # Wrap in ASTList if using ASTObjects and not already an ASTList
            if use_ast_objects and not isinstance(objs, ASTList):
                objs = ASTList(objs)
            return objs

    if os.path.isfile(source):
        if source.endswith(".sql"):
            with open(source, "r", encoding="utf-8") as f:
                objs = extract_build_queries(f.read(), use_ast_objects=use_ast_objects)
        elif source.endswith(".json"):
            with open(source, "r", encoding="utf-8") as f:
                objs = json.load(f)
                if use_ast_objects:
                    objs = ASTList.from_dict_list(objs)
        if not grants:
            if use_ast_objects:
                objs = ASTList([o for o in objs if o.query_type.value != "grant"])
            else:
                objs = [o for o in objs if o.get("query_type") != "grant"]
        # Wrap in ASTList if using ASTObjects and not already an ASTList
        if use_ast_objects and not isinstance(objs, ASTList):
            objs = ASTList(objs)
        return objs
    elif os.path.isdir(source):
        # Handle directory by merging all SQL files
        from pg_compose_core.lib.merge import merge_sql
        import tempfile
        
        # Create a temporary directory for the merged file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Merge all SQL files in the directory (no subdirectory filter)
            ast_list = merge_sql(source, temp_dir)
            if use_ast_objects:
                objs = ast_list
            else:
                objs = ast_list.to_dict_list()
        if not grants:
            if use_ast_objects:
                objs = ASTList([o for o in objs if o.query_type.value != "grant"])
            else:
                objs = [o for o in objs if o.get("query_type") != "grant"]
        # Wrap in ASTList if using ASTObjects and not already an ASTList
        if use_ast_objects and not isinstance(objs, ASTList):
            objs = ASTList(objs)
        return objs

    # Assume raw SQL string
    objs = extract_build_queries(source, use_ast_objects=use_ast_objects)
    if not grants:
        if use_ast_objects:
            objs = ASTList([o for o in objs if o.query_type.value != "grant"])
        else:
            objs = [o for o in objs if o.get("query_type") != "grant"]
    # Wrap in ASTList if using ASTObjects and not already an ASTList
    if use_ast_objects and not isinstance(objs, ASTList):
        objs = ASTList(objs)
    return objs


def compare_sources(
    source_a: str,
    source_b: str,
    *,
    schemas: Optional[List[str]] = None,
    grants: bool = True,
    use_ast_objects: bool = True
) -> Union[dict, 'ASTList']:
    """Compare two schema sources and return diff result or ASTList of alter commands."""
    
    schema_a = load_source(source_a, schemas=schemas, grants=grants, use_ast_objects=use_ast_objects)
    schema_b = load_source(source_b, schemas=schemas, grants=grants, use_ast_objects=use_ast_objects)

    # Convert to dict format for diff_schemas if needed
    if use_ast_objects:
        schema_a_dict = schema_a.to_dict_list()
        schema_b_dict = schema_b.to_dict_list()
    else:
        schema_a_dict = schema_a
        schema_b_dict = schema_b

    result = diff_schemas(schema_a_dict, schema_b_dict)

    # Access global verbosity from CLI module
    import pg_compose_core.cli.cli as cli_module
    if cli_module.VERBOSE:
        print("\nSchema Diff Results\n" + "=" * 40)
        # Filter commands by type for display
        create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE")]
        drop_cmds = [obj for obj in result if obj.command.strip().upper().startswith("DROP")]
        alter_cmds = [obj for obj in result if obj.command.strip().upper().startswith("ALTER")]
        
        for obj in create_cmds:
            print(f"[CREATE] {obj.query_type.value} {obj.object_name}")
        for obj in drop_cmds:
            print(f"[DROP]   {obj.query_type.value} {obj.object_name}")
        for obj in alter_cmds:
            print(f"[ALTER]  {obj.query_type.value} {obj.object_name}")
        print("=" * 40)

    return result



