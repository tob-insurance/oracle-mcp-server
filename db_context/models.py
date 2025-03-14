from dataclasses import dataclass
from typing import Dict, List, Set, Protocol, Optional, Any
from pathlib import Path

@dataclass
class TableInfo:
    columns: List[Dict[str, str]]
    relationships: Dict[str, Dict[str, str]]
    fully_loaded: bool = False

@dataclass
class SchemaCache:
    tables: Dict[str, TableInfo]
    last_updated: float
    all_table_names: Set[str]  # Set of all table names in the database

class SchemaManager(Protocol):
    """Protocol defining the interface for schema management"""
    def is_cache_valid(self, cache_type: str, key: str) -> bool: ...
    def update_cache(self, cache_type: str, key: str, data: Any) -> None: ...
    async def save_cache(self, cache: Optional[SchemaCache] = None) -> None: ...