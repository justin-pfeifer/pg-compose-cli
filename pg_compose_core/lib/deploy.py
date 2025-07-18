from typing import List, Optional
from pg_compose_core.lib.parser import load_source
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.ast.list import ASTList




def diff_sort(
    source_a: str,
    source_b: str,
    *,
    schemas: Optional[List[str]] = None,
    grants: bool = True
):
    """
    Generate sorted diff between two schema sources.
    
    Args:
        source_a: Path to source A (file, directory, or connection string)
        source_b: Path to source B (file, directory, or connection string)
        schemas: Optional list of schemas to include
        grants: Whether to include grant statements
    
    Returns:
        Sorted ASTList containing the diff commands
    """
    # Load both sources
    schema_a = load_source(source_a, schemas=schemas, grants=grants)
    schema_b = load_source(source_b, schemas=schemas, grants=grants)
    
    import logging
    logging.info(f"Loaded {len(schema_a)} objects from source A")
    logging.info(f"Loaded {len(schema_b)} objects from source B")
    
    # Generate diff as ASTList (diff_schemas now works with ASTObjects directly)
    diff_result = diff_schemas(schema_a, schema_b)
    
    logging.info(f"Generated {len(diff_result)} alter commands")
    
    # Sort by dependencies and return
    return diff_result.sort()


def deploy(
    source: str | ASTList,
    target: str,
    *,
    dry_run: bool = True,
    verbose: bool = False
) -> dict:
    """
    Deploy a source to a target.
    
    Args:
        source: Source SQL string or ASTList containing the commands to deploy
        target: Target destination - filename, git URL, or PostgreSQL connection string
        dry_run: Whether to preview changes without applying them (default: True for safety)
        verbose: Whether to print verbose output
    
    Returns:
        Dictionary containing deployment results
    """
    # TODO: Implement actual deployment logic
    # This should handle:
    # - File output: Write SQL to file
    # - Git: Commit changes to repository
    # - PostgreSQL: Execute SQL against database
    
    # Convert source to SQL if it's an ASTList
    if isinstance(source, ASTList):
        sql = source.to_sql()
        ast_list = source
    else:
        sql = source
        # TODO: Parse SQL string to ASTList for better handling
        ast_list = ASTList([])  # Placeholder
    
    if verbose:
        import logging
        logging.info(f"Deploying to {target}")
        if isinstance(source, ASTList):
            logging.info(f"Deploying {len(ast_list)} commands")
        else:
            logging.info(f"Deploying SQL string ({len(sql)} characters)")
        if dry_run:
            logging.info("DRY RUN - No changes will be applied")
    
    if dry_run:
        return {
            "status": "preview",
            "target": target,
            "changes_count": len(ast_list) if isinstance(source, ASTList) else 1,
            "sql": sql,
            "message": "Dry run completed - no changes applied"
        }
    else:
        # TODO: Implement actual deployment based on target type
        # - Detect target type (file, git, postgres)
        # - Execute appropriate deployment logic
        # - Return actual results
        
        return {
            "status": "success",
            "target": target,
            "changes_applied": len(ast_list) if isinstance(source, ASTList) else 1,
            "sql": sql,
            "message": f"Generated changes to apply to {target}"
        } 