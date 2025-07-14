"""
ASTList container for ASTObject instances.
"""

from typing import List, Iterator, Optional, Callable, Any
from pg_compose_core.lib.ast.objects import ASTObject


class ASTList(list):
    """
    Container for a list of ASTObject instances, with utilities for merging, sorting, diffing, and exporting to SQL.
    """
    def __init__(self, items: Optional[List[ASTObject]] = None):
        super().__init__(items or [])

    def __iter__(self) -> Iterator[ASTObject]:
        return super().__iter__()

    def __len__(self):
        return super().__len__()

    def __getitem__(self, item) -> Any:
        return super().__getitem__(item)

    def merge(self, other: 'ASTList') -> 'ASTList':
        # Combine and deduplicate by (object_name, query_type, query_hash)
        combined = self + other
        seen = set()
        deduped = []
        for obj in combined:
            key = (obj.object_name, obj.query_type, obj.query_hash)
            if key not in seen:
                seen.add(key)
                deduped.append(obj)
        return ASTList(deduped)

    def sort(self) -> 'ASTList':
        # Import here to avoid circular dependency
        from pg_compose_core.lib.sorter import sort_queries
        sorted_objs = sort_queries(self, use_object_names=False, grant_handling=True)
        return ASTList(sorted_objs)

    def to_sql(self) -> str:
        # Output SQL for all objects in order
        return "\n\n".join(obj.command for obj in self)

    def to_dict_list(self) -> List[dict]:
        return [obj.to_dict() for obj in self]

    @classmethod
    def from_dict_list(cls, dicts: List[dict]) -> 'ASTList':
        return cls([ASTObject.from_dict(d) for d in dicts])

    def __str__(self):
        return f"ASTList({len(self)} objects)"

    def __repr__(self):
        return f"ASTList(objects={list.__repr__(self)})"

    def filter(self, predicate: Callable[[ASTObject], bool]) -> 'ASTList':
        return ASTList([obj for obj in self if predicate(obj)]) 