"""
ASTList container for ASTObject instances.
"""

from typing import List, Optional
from .ast_objects import ASTObject


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
        # Import here to avoid circular dependency
        from .sorter import sort_queries
        sorted_objs = sort_queries(self.objects, use_object_names=False, grant_handling=True)
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