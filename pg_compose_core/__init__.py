"""
pg-compose-core: Core library for PostgreSQL schema comparison and migration

This package provides the core functionality for comparing PostgreSQL schemas
from SQL files or live database connections, and generating migration scripts.
"""

# Import core library functionality
from pg_compose_core.lib import (
    compare_sources,
    ASTList,
    ASTObject
)

# Import CLI and API interfaces
from pg_compose_core.cli import main
from pg_compose_core.api import app

__version__ = "0.2.0"
__all__ = [
    # Core library exports
    "compare_sources",
    "ASTList",
    "ASTObject",
    
    # Interface exports
    "main",
    "app"
]
