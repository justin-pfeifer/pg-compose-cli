from typing import List, Optional
from pg_compose_cli.compare import load_source
from pg_compose_cli.diff import diff_schemas


def generate_deploy_sql(
    source_a: str,
    source_b: str,
    *,
    schemas: Optional[List[str]] = None,
    grants: bool = True,
    verbose: bool = False
) -> str:
    """
    Generate deploy SQL to migrate from source_a to source_b.
    
    Args:
        source_a: Path to source A (file, directory, or connection string)
        source_b: Path to source B (file, directory, or connection string)
        schemas: Optional list of schemas to include
        grants: Whether to include grant statements
        verbose: Whether to print verbose output
    
    Returns:
        SQL string containing all necessary commands to deploy changes
    """
    # Load both sources
    schema_a = load_source(source_a, schemas=schemas, grants=grants, use_ast_objects=False)
    schema_b = load_source(source_b, schemas=schemas, grants=grants, use_ast_objects=False)
    
    if verbose:
        print(f"Loaded {len(schema_a)} objects from source A")
        print(f"Loaded {len(schema_b)} objects from source B")
    
    # Generate diff as ASTList
    diff_result = diff_schemas(schema_a, schema_b)
    
    if verbose:
        print(f"Generated {len(diff_result)} alter commands")
    
    # Sort by dependencies and convert to SQL
    sorted_result = diff_result.sort()
    sql = sorted_result.to_sql()
    
    return sql


def generate_deploy_sql_list(
    source_a: str,
    source_b: str,
    *,
    schemas: Optional[List[str]] = None,
    grants: bool = True,
    verbose: bool = False
) -> List[str]:
    """
    Generate deploy SQL as a list of commands.
    
    Args:
        source_a: Path to source A (file, directory, or connection string)
        source_b: Path to source B (file, directory, or connection string)
        schemas: Optional list of schemas to include
        grants: Whether to include grant statements
        verbose: Whether to print verbose output
    
    Returns:
        List of SQL commands to execute
    """
    # Load both sources
    schema_a = load_source(source_a, schemas=schemas, grants=grants, use_ast_objects=False)
    schema_b = load_source(source_b, schemas=schemas, grants=grants, use_ast_objects=False)
    
    if verbose:
        print(f"Loaded {len(schema_a)} objects from source A")
        print(f"Loaded {len(schema_b)} objects from source B")
    
    # Generate diff as ASTList
    diff_result = diff_schemas(schema_a, schema_b)
    
    if verbose:
        print(f"Generated {len(diff_result)} alter commands")
    
    # Sort by dependencies and return as list
    sorted_result = diff_result.sort()
    return [obj.command for obj in sorted_result.objects] 