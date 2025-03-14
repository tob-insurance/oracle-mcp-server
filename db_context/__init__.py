from pathlib import Path
from typing import Optional, List, Dict, Any

from .database import DatabaseConnector
from .schema.manager import SchemaManager
from .models import TableInfo


class DatabaseContext:
    def __init__(self, connection_string: str, cache_path: Path):
        self.db_connector = DatabaseConnector(connection_string)
        self.schema_manager = SchemaManager(self.db_connector, cache_path)
        # Set the schema manager reference in the connector
        self.db_connector.set_schema_manager(self.schema_manager)
        
    async def initialize(self) -> None:
        """Initialize the database context and build initial cache"""
        await self.schema_manager.initialize()
        
    async def get_database_info(self):
        """Get information about the database vendor and version"""
        return await self.db_connector.get_database_info()
        
    async def get_schema_info(self, table_name: str) -> Optional[TableInfo]:
        """Get schema information for a specific table"""
        return await self.schema_manager.get_schema_info(table_name)
    
    async def search_tables(self, search_term: str, limit: int = 20) -> List[str]:
        """Search for table names matching the search term"""
        return await self.schema_manager.search_tables(search_term, limit)
        
    async def rebuild_cache(self) -> None:
        """Force a rebuild of the schema cache"""
        self.schema_manager.cache = await self.schema_manager.load_or_build_cache(force_rebuild=True)
        
    async def search_columns(self, search_term: str, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """Search for columns matching the given pattern across all tables"""
        return await self.schema_manager.search_columns(search_term, limit)
        
    async def get_pl_sql_objects(self, object_type: str, name_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get information about PL/SQL objects of the specified type"""
        return await self.db_connector.get_pl_sql_objects(object_type, name_pattern)
        
    async def get_object_source(self, object_type: str, object_name: str) -> str:
        """Get the source code for a PL/SQL object"""
        return await self.db_connector.get_object_source(object_type, object_name)
        
    async def get_table_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """Get constraints for a specific table"""
        return await self.db_connector.get_table_constraints(table_name)
        
    async def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get indexes for a specific table"""
        return await self.db_connector.get_table_indexes(table_name)
        
    async def get_dependent_objects(self, object_name: str) -> List[Dict[str, Any]]:
        """Get objects that depend on the specified object"""
        return await self.db_connector.get_dependent_objects(object_name)
        
    async def get_user_defined_types(self, type_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get information about user-defined types"""
        return await self.db_connector.get_user_defined_types(type_pattern)