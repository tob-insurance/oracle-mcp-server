import json
import time
from pathlib import Path
import sys
from typing import Dict, List, Set, Optional, Any

from ..models import TableInfo, SchemaCache, SchemaManager as SchemaManagerProtocol

class SchemaManager(SchemaManagerProtocol):
    def __init__(self, db_connector: Any, cache_path: Path):
        self.db_connector = db_connector
        self.cache_path = cache_path
        self.cache: Optional[SchemaCache] = None
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'last_full_refresh': time.time()
        }
        self.object_cache = {
            'plsql': {},
            'constraints': {},
            'indexes': {},
            'types': {}
        }
        self.ttl = {
            'plsql': 1800,     # 30 minutes
            'constraints': 3600,  # 1 hour
            'indexes': 3600,     # 1 hour
            'types': 3600       # 1 hour
        }

    async def build_schema_index(self) -> Dict[str, TableInfo]:
        """
        Build a basic schema index with just table names.
        Detailed information will be loaded lazily when needed.
        """
        all_table_names = await self.db_connector.get_all_table_names()
        print(f"Found {len(all_table_names)} tables in the database", file=sys.stderr)
        
        # Initialize empty table info for each table (lazy loading)
        schema_index = {
            table_name: TableInfo(columns=[], relationships={}, fully_loaded=False)
            for table_name in all_table_names
        }
        
        return schema_index

    async def load_or_build_cache(self, force_rebuild: bool = False) -> SchemaCache:
        """Load the schema cache from disk or rebuild it if needed"""
        if not force_rebuild and self.cache_path.exists():
            try:
                print("Opening existing index file...", file=sys.stderr)
                with open(self.cache_path, 'r') as f:
                    data = json.load(f)
                    print("Loading index in memory...", file=sys.stderr)
                    # Load main schema cache
                    cache = SchemaCache(
                        tables={k: TableInfo(**v) for k, v in data['tables'].items()},
                        last_updated=data['last_updated'],
                        all_table_names=set(data.get('all_table_names', []))
                    )
                    
                    # Load additional object caches if they exist
                    if 'object_cache' in data:
                        self.object_cache = data['object_cache']
                    if 'cache_stats' in data:
                        self.cache_stats = data['cache_stats']
                    
                    return cache
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error loading cache: {e}", file=sys.stderr)
                # Fall through to rebuild
        
        # Build new cache
        tables = await self.build_schema_index()
        all_table_names = set(tables.keys())
        print("Loading index in memory...", file=sys.stderr)
        cache = SchemaCache(
            tables=tables, 
            last_updated=time.time(),
            all_table_names=all_table_names
        )
        
        # Save to disk
        await self.save_cache(cache)
        return cache

    async def save_cache(self, cache: Optional[SchemaCache] = None) -> None:
        """Save the current cache to disk"""
        cache_to_save = cache or self.cache
        if not cache_to_save:
            return
            
        print("Saving updated index to disk...", file=sys.stderr)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, 'w') as f:
            json.dump({
                'tables': {k: v.__dict__ for k, v in cache_to_save.tables.items()},
                'last_updated': cache_to_save.last_updated,
                'all_table_names': list(cache_to_save.all_table_names),
                'object_cache': self.object_cache,
                'cache_stats': self.cache_stats
            }, f, indent=2)
        print("Index saved!", file=sys.stderr)

    async def get_schema_info(self, table_name: str) -> Optional[TableInfo]:
        """Get schema information for a specific table, loading it if necessary"""
        if not self.cache:
            self.cache = await self.load_or_build_cache()
            
        table_name = table_name.upper()
        
        # Check if we know about this table
        if table_name not in self.cache.all_table_names:
            return None
            
        # Check if we have the table in our cache
        if table_name not in self.cache.tables:
            self.cache.tables[table_name] = TableInfo(columns=[], relationships={}, fully_loaded=False)
            
        # If the table isn't fully loaded, load it now
        if not self.cache.tables[table_name].fully_loaded:
            print(f"Lazily loading details for table {table_name}...", file=sys.stderr)
            table_details = await self.db_connector.load_table_details(table_name)
            if table_details:
                table_info = TableInfo(
                    columns=table_details["columns"],
                    relationships=table_details["relationships"],
                    fully_loaded=True
                )
                self.cache.tables[table_name] = table_info
                # Save the updated cache to disk
                await self.save_cache()
            else:
                # Table doesn't actually exist, remove it from our cache
                self.cache.tables.pop(table_name, None)
                self.cache.all_table_names.discard(table_name)
                await self.save_cache()
                return None
                
        return self.cache.tables.get(table_name)

    async def search_tables(self, search_term: str, limit: int = 20) -> List[str]:
        """Search for table names matching the search term"""
        if not self.cache:
            self.cache = await self.load_or_build_cache()
            
        search_term = search_term.upper()
        matching_tables = [
            table_name for table_name in self.cache.all_table_names
            if search_term in table_name
        ]
        
        # Return the first 'limit' matching tables
        return matching_tables[:limit]

    async def search_columns(self, search_term: str, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """Search for columns matching the given pattern across all tables"""
        if not self.cache:
            await self.initialize()
            
        search_term = search_term.upper()
        result = {}
        
        # First check in cached tables to avoid database queries for already loaded tables
        for table_name, table_info in self.cache.tables.items():
            for column in table_info.columns:
                if search_term in column["name"].upper():
                    if table_name not in result:
                        result[table_name] = []
                    result[table_name].append(column)
        
        # If we have enough results already, return them
        if len(result) >= limit:
            # Just return the first 'limit' tables
            return dict(list(result.items())[:limit])
        
        # If we don't have enough results, search in all tables
        # We'll sample some uncached tables to see if we find columns matching the search term
        uncached_tables = [
            t for t in self.cache.all_table_names 
            if t not in self.cache.tables
        ]
        
        # Shuffle to get a random sample of tables
        import random
        random.shuffle(uncached_tables)
        
        # Sample a reasonable number of tables to search
        sample_size = min(100, len(uncached_tables))
        sampled_tables = uncached_tables[:sample_size]
        
        for table_name in sampled_tables:
            # Skip if we already have enough tables
            if len(result) >= limit:
                break
                
            # Load table details
            table_info = await self.get_schema_info(table_name)
            if not table_info:
                continue
                
            # Check if any columns match
            matching_columns = [
                col for col in table_info.columns
                if search_term in col["name"].upper()
            ]
            
            if matching_columns:
                result[table_name] = matching_columns
        
        return result

    async def initialize(self) -> None:
        """Initialize the database context and build initial cache"""
        self.cache = await self.load_or_build_cache()
        if not self.cache:
            raise RuntimeError("Failed to initialize schema cache")

    def is_cache_valid(self, cache_type: str, key: str) -> bool:
        """Check if a cached item is still valid based on TTL"""
        if (cache_type not in self.object_cache or 
            key not in self.object_cache[cache_type] or 
            'timestamp' not in self.object_cache[cache_type][key]):
            return False
        return (time.time() - self.object_cache[cache_type][key]['timestamp']) < self.ttl[cache_type]

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            **self.cache_stats,
            'size': {
                'tables': len(self.cache.tables) if self.cache else 0,
                'plsql': len(self.object_cache['plsql']),
                'constraints': len(self.object_cache['constraints']),
                'indexes': len(self.object_cache['indexes']),
                'types': len(self.object_cache['types'])
            }
        }

    def update_cache(self, cache_type: str, key: str, data: Any) -> None:
        """Update cache with new data"""
        self.object_cache[cache_type][key] = {
            'data': data,
            'timestamp': time.time()
        }