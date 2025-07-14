"""
Core library functionality for PostgreSQL schema comparison and migration.
"""

from pg_compose_core.lib.ast import ASTObject, BuildStage
from pg_compose_core.lib.ast import ASTList
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.deploy import diff_sort, deploy
from pg_compose_core.lib.sorter import sort_queries, sort_alter_commands
from pg_compose_core.lib.git import extract_from_git_repo, GitRepoContext
from pg_compose_core.lib.pgdump import extract_from_postgres

__all__ = [
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
    
    # Sorting
    "sort_queries",
    "sort_alter_commands",
    
    # Utilities
    "GitRepoContext"
] 