"""
pg-compose-core: Core library for PostgreSQL schema comparison and migration

This package provides the core functionality for comparing PostgreSQL schemas
from SQL files or live database connections, and generating migration scripts.
"""

# Import core library functionality
from pg_compose_core.lib import (
    ASTList,
    ASTObject
)

__version__ = "0.2.0"
__all__ = [
    # Core library exports
    "ASTList",
    "ASTObject"
]
