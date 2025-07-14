from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from pg_compose_core.lib.ast.objects import ASTObject, BuildStage

@dataclass
class TableColumn:
    name: str
    data_type: str
    is_nullable: bool = True
    default: Optional[str] = None
    # You can add more fields as needed (e.g., collation, comment)

@dataclass
class TableConstraint:
    name: Optional[str]
    constraint_type: str  # e.g., 'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK'
    columns: List[str] = field(default_factory=list)
    details: Optional[Dict[str, Any]] = None  # For FK target, check expr, etc.

@dataclass
class TablePartition:
    partition_type: str  # 'RANGE', 'LIST', 'HASH'
    columns: List[str]
    bounds: Optional[str] = None  # e.g., FOR VALUES FROM (...) TO (...)

@dataclass
class TableASTObject(ASTObject):
    columns: List[TableColumn] = field(default_factory=list)
    constraints: List[TableConstraint] = field(default_factory=list)
    partition: Optional[TablePartition] = None
    parent_table: Optional[str] = None  # If this is a partition

    def __post_init__(self):
        self.query_type = BuildStage.BASE_TABLE
        super().__post_init__()

    def diff(self, source: 'TableASTObject') -> Tuple[List[TableColumn], List[TableColumn], List[Tuple[TableColumn, TableColumn]]]:
        """
        Compare this table (new version) with source table (prior version).
        
        Returns:
            Tuple of (added_columns, removed_columns, changed_columns)
            where changed_columns is a list of (old_column, new_column) pairs
        """
        # Treat self as the new version, source as prior
        old_columns = {col.name: col for col in source.columns}
        new_columns = {col.name: col for col in self.columns}
        
        added = []
        removed = []
        changed = []
        
        # Find added columns
        for col_name, new_col in new_columns.items():
            if col_name not in old_columns:
                added.append(new_col)
        
        # Find removed columns
        for col_name, old_col in old_columns.items():
            if col_name not in new_columns:
                removed.append(old_col)
        
        # Find changed columns
        for col_name in old_columns:
            if col_name in new_columns:
                old_col = old_columns[col_name]
                new_col = new_columns[col_name]
                
                # Check if column has changed
                if (old_col.data_type != new_col.data_type or
                    old_col.is_nullable != new_col.is_nullable or
                    old_col.default != new_col.default):
                    changed.append((old_col, new_col))
        
        return added, removed, changed

__all__ = [
    "TableColumn",
    "TableConstraint",
    "TablePartition",
    "TableASTObject",
] 