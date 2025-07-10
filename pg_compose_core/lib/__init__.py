"""
Core library functionality for PostgreSQL schema comparison and migration.
"""

from pg_compose_core.lib.compare import compare_sources, load_source
from pg_compose_core.lib.ast_objects import ASTList, ASTObject, BuildStage
from pg_compose_core.lib.extract import extract_build_queries
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.deploy import diff_sort, deploy
from pg_compose_core.lib.sorter import sort_queries, sort_alter_commands
from pg_compose_core.lib.merge import merge_sql, reorder_sql_file
from pg_compose_core.lib.git import extract_from_git_repo, GitRepoContext
from pg_compose_core.lib.pgdump import extract_from_postgres
from pg_compose_core.lib.catalog import extract_from_catalog
from pg_compose_core.lib.alter_generator import generate_alter_commands

__all__ = [
    # Core comparison functionality
    "compare_sources",
    "load_source",
    
    # AST objects
    "ASTList",
    "ASTObject", 
    "BuildStage",
    
    # Schema extraction
    "extract_build_queries",
    "extract_from_postgres",
    "extract_from_git_repo",
    
    # Diff and deployment
    "diff_schemas",
    "diff_sort",
    "deploy",
    
    # Sorting and merging
    "sort_queries",
    "sort_alter_commands",
    "merge_sql",
    "reorder_sql_file",
    
    # Utilities
    "GitRepoContext",
    "extract_from_catalog",
    "generate_alter_commands"
] 