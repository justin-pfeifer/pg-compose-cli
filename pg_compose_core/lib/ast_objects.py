from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import re

class BuildStage(Enum):
    """Enumeration of possible build stages for database objects."""
    EXTENSION = "extension"
    SCHEMA = "schema"
    ENUM = "enum"
    DOMAIN = "domain"
    COMPOSITE_TYPE = "composite_type"
    BASE_TABLE = "base_table"
    SEQUENCE = "sequence"
    DEPENDENT_TABLE = "dependent_table"
    INDEX = "index"
    CONSTRAINT = "constraint"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    FUNCTION = "function"
    TRIGGER = "trigger"
    POLICY = "policy"
    GRANT = "grant"
    UNKNOWN = "unknown"

@dataclass
class ASTObject:
    """
    Represents a parsed SQL AST object with all necessary metadata.
    
    Attributes:
        command: The SQL command text
        object_name: Name of the database object (e.g., table name, view name)
        query_type: Type of SQL statement (CREATE TABLE, GRANT, etc.)
        dependencies: List of object names this object depends on
        query_hash: Hash of the normalized SQL for deduplication
        query_start_pos: Start position of the query in the original SQL
        query_end_pos: End position of the query in the original SQL
        schema: Schema name if specified
        ast_node: The original pglast AST node (optional, for debugging)
    """
    command: str
    object_name: Optional[str] = None
    query_type: BuildStage = BuildStage.UNKNOWN
    dependencies: List[str] = field(default_factory=list)
    query_hash: Optional[str] = None
    query_start_pos: int = 0
    query_end_pos: int = 0
    schema: Optional[str] = None
    ast_node: Optional[Any] = None
    
    def __post_init__(self):
        """Generate query_hash if not provided."""
        if self.query_hash is None:
            self.query_hash = self._generate_hash()
    
    def _generate_hash(self) -> str:
        """Generate a hash of the normalized command."""
        normalized = self._normalize_sql(self.command)
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    def _normalize_sql(self, sql: str) -> str:
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
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "query_text": self.command,
            "object_name": self.object_name,
            "query_type": self.query_type.value,
            "dependencies": self.dependencies,
            "query_hash": self.query_hash,
            "query_start_pos": self.query_start_pos,
            "query_end_pos": self.query_end_pos,
            "schema": self.schema
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ASTObject':
        """Create ASTObject from dictionary format."""
        return cls(
            command=data.get("query_text", ""),
            object_name=data.get("object_name"),
            query_type=BuildStage(data.get("query_type", "unknown")),
            dependencies=data.get("dependencies", []),
            query_hash=data.get("query_hash"),
            query_start_pos=data.get("query_start_pos", 0),
            query_end_pos=data.get("query_end_pos", 0),
            schema=data.get("schema")
        )
    
    def __str__(self) -> str:
        """String representation for debugging."""
        return f"ASTObject({self.query_type.value}: {self.object_name or 'unnamed'})"
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (f"ASTObject(command='{self.command[:50]}...', "
                f"object_name='{self.object_name}', "
                f"query_type={self.query_type.value}, "
                f"dependencies={self.dependencies})")



class ASTList:
    """
    Container for a list of ASTObject instances, with utilities for merging, sorting, diffing, and exporting to SQL.
    """
    def __init__(self, objects: Optional[List[ASTObject]] = None):
        self.objects: List[ASTObject] = objects or []

    def __iter__(self):
        return iter(self.objects)

    def __len__(self):
        return len(self.objects)

    def __getitem__(self, idx):
        return self.objects[idx]

    def merge(self, other: 'ASTList') -> 'ASTList':
        # Combine and deduplicate by (object_name, query_type, query_hash)
        combined = self.objects + other.objects
        seen = set()
        deduped = []
        for obj in combined:
            key = (obj.object_name, obj.query_type, obj.query_hash)
            if key not in seen:
                seen.add(key)
                deduped.append(obj)
        return ASTList(deduped)

    def sort(self) -> 'ASTList':
        from .sorter import sort_queries
        sorted_objs = sort_queries(self.objects, use_object_names=True, grant_handling=True)
        return ASTList(sorted_objs)

    def to_sql(self) -> str:
        # Output SQL for all objects in order
        return "\n\n".join(obj.command for obj in self.objects)

    def to_dict_list(self) -> List[dict]:
        return [obj.to_dict() for obj in self.objects]

    @classmethod
    def from_dict_list(cls, dicts: List[dict]) -> 'ASTList':
        return cls([ASTObject.from_dict(d) for d in dicts])

    def __str__(self):
        return f"ASTList({len(self.objects)} objects)"

    def __repr__(self):
        return f"ASTList(objects={self.objects!r})" 