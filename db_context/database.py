import sys
import oracledb
import time
from typing import Dict, List, Set, Optional, Any
from pathlib import Path
from .models import SchemaManager

class DatabaseConnector:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.schema_manager: Optional[SchemaManager] = None  # Will be set by DatabaseContext
        
    def set_schema_manager(self, schema_manager: SchemaManager) -> None:
        """Set the schema manager reference"""
        self.schema_manager = schema_manager

    async def get_connection(self):
        """Create and return a database connection"""
        try:
            print(f"Connecting to database with connection string: {self.connection_string}", file=sys.stderr)
            return await oracledb.connect_async(self.connection_string)
        except oracledb.Error as e:
            print(f"Database connection error: {str(e)}", file=sys.stderr)
            raise  # Re-raise the exception after logging
        except Exception as e:
            print(f"Unexpected error while connecting to database: {str(e)}", file=sys.stderr)
            raise

    async def get_database_info(self) -> Dict[str, Any]:
        """Get information about the database vendor and version"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            # Query for database version information
            await cursor.execute("SELECT * FROM v$version")
            version_info = await cursor.fetchall()
            
            # Extract vendor type and full version string
            vendor_info = {}
            
            if version_info:
                # First row typically contains the main Oracle version info
                full_version = version_info[0][0]
                vendor_info["vendor"] = "Oracle"
                vendor_info["version"] = full_version
                
                # Additional version info rows
                additional_info = [row[0] for row in version_info[1:] if row[0]]
                if additional_info:
                    vendor_info["additional_info"] = additional_info
                    
            return vendor_info
        except oracledb.Error as e:
            print(f"Error getting database info: {str(e)}", file=sys.stderr)
            return {"vendor": "Oracle", "version": "Unknown", "error": str(e)}
        finally:
            await conn.close()

    async def get_all_table_names(self) -> Set[str]:
        """Get a list of all table names in the database"""
        conn = await self.get_connection()
        try:
            print("Getting list of all tables...", file=sys.stderr)
            cursor = conn.cursor()
            await cursor.execute("""
                SELECT table_name 
                FROM all_tables 
                WHERE owner = :owner
                ORDER BY table_name
            """, owner=conn.username.upper())
            
            all_tables = await cursor.fetchall()
            return {t[0] for t in all_tables}
        finally:
            await conn.close()
    
    async def load_table_details(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Load detailed schema information for a specific table"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            # Check if the table exists
            await cursor.execute("""
                SELECT COUNT(*) 
                FROM all_tables 
                WHERE owner = :owner AND table_name = :table_name
            """, owner=conn.username.upper(), table_name=table_name.upper())
            
            count = await cursor.fetchone()
            if count[0] == 0:
                return None
                
            # Get column information
            await cursor.execute("""
                SELECT column_name, data_type, nullable
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
            """, owner=conn.username.upper(), table_name=table_name.upper())
            
            columns = await cursor.fetchall()
            column_info = []
            for column, data_type, nullable in columns:
                column_info.append({
                    "name": column,
                    "type": data_type,
                    "nullable": nullable == 'Y'
                })
            
            # Get relationship information
            await cursor.execute("""
                SELECT acc.column_name,
                       rcc.table_name AS referenced_table,
                       rcc.column_name AS referenced_column
                FROM all_cons_columns acc
                JOIN all_constraints ac ON acc.constraint_name = ac.constraint_name
                JOIN all_cons_columns rcc ON rcc.constraint_name = ac.r_constraint_name
                WHERE ac.constraint_type = 'R'
                AND acc.owner = :owner
                AND acc.table_name = :table_name
                AND rcc.owner = ac.r_owner
            """, owner=conn.username.upper(), table_name=table_name.upper())
            
            relationships = await cursor.fetchall()
            relationship_info = {}
            for column, ref_table, ref_column in relationships:
                relationship_info[ref_table] = {
                    "local_column": column,
                    "foreign_column": ref_column
                }
            
            return {
                "columns": column_info,
                "relationships": relationship_info
            }
            
        except oracledb.Error as e:
            print(f"Error loading table details for {table_name}: {str(e)}", file=sys.stderr)
            raise
        finally:
            await conn.close()
    
    async def get_pl_sql_objects(self, object_type: str, name_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get PL/SQL objects with caching"""
        if not self.schema_manager:
            raise RuntimeError("Schema manager not initialized")
            
        cache_key = f"{object_type}_{name_pattern or 'all'}"
        
        if self.schema_manager.is_cache_valid('plsql', cache_key):
            self.schema_manager.cache_stats['hits'] += 1
            return self.schema_manager.object_cache['plsql'][cache_key]['data']
        
        self.schema_manager.cache_stats['misses'] += 1
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            where_clause = "WHERE owner = :owner AND object_type = :object_type"
            params = {"owner": conn.username.upper(), "object_type": object_type}
            
            if name_pattern:
                where_clause += " AND object_name LIKE :name_pattern"
                params["name_pattern"] = name_pattern.upper()
            
            await cursor.execute(f"""
                SELECT object_name, object_type, status, created, last_ddl_time
                FROM all_objects
                {where_clause}
                ORDER BY object_name
            """, **params)
            
            objects = await cursor.fetchall()
            result = []
            
            for name, obj_type, status, created, last_modified in objects:
                obj_info = {
                    "name": name,
                    "type": obj_type,
                    "status": status,
                    "owner": conn.username.upper()
                }
                
                if created:
                    obj_info["created"] = created.strftime("%Y-%m-%d %H:%M:%S")
                if last_modified:
                    obj_info["last_modified"] = last_modified.strftime("%Y-%m-%d %H:%M:%S")
                
                result.append(obj_info)
            
            # Update cache through schema manager
            self.schema_manager.update_cache('plsql', cache_key, result)
            await self.schema_manager.save_cache()
            return result
        finally:
            await conn.close()
    
    async def get_object_source(self, object_type: str, object_name: str) -> str:
        """Get the source code for a PL/SQL object"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Handle different object types accordingly
            if object_type in ('PACKAGE', 'PACKAGE BODY', 'TYPE', 'TYPE BODY'):
                # For packages and types, we need to get the full source
                await cursor.execute("""
                    SELECT text
                    FROM all_source
                    WHERE owner = :owner 
                    AND name = :name 
                    AND type = :type
                    ORDER BY line
                """, owner=conn.username.upper(), name=object_name, type=object_type)
                
                source_lines = await cursor.fetchall()
                if not source_lines:
                    return ""
                
                return "\n".join(line[0] for line in source_lines)
            else:
                # For procedures, functions, triggers, etc.
                await cursor.execute("""
                    SELECT dbms_metadata.get_ddl(
                        :object_type, 
                        :object_name, 
                        :owner
                    ) FROM dual
                """, 
                object_type=object_type, 
                object_name=object_name,
                owner=conn.username.upper())
                
                result = await cursor.fetchone()
                if not result:
                    return ""
                    
                return result[0].read()
                
        except oracledb.Error as e:
            print(f"Error getting object source: {str(e)}", file=sys.stderr)
            return f"Error retrieving source: {str(e)}"
        finally:
            await conn.close()
    
    async def get_table_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table constraints with caching"""
        if not self.schema_manager:
            raise RuntimeError("Schema manager not initialized")
            
        if self.schema_manager.is_cache_valid('constraints', table_name):
            self.schema_manager.cache_stats['hits'] += 1
            return self.schema_manager.object_cache['constraints'][table_name]['data']
        
        self.schema_manager.cache_stats['misses'] += 1
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Get all constraints for the table
            await cursor.execute("""
                SELECT ac.constraint_name,
                       ac.constraint_type,
                       ac.search_condition
                FROM all_constraints ac
                WHERE ac.owner = :owner
                AND ac.table_name = :table_name
            """, owner=conn.username.upper(), table_name=table_name.upper())
            
            constraints = await cursor.fetchall()
            result = []
            
            for constraint_name, constraint_type, condition in constraints:
                # Map constraint type codes to descriptions
                type_map = {
                    'P': 'PRIMARY KEY',
                    'R': 'FOREIGN KEY',
                    'U': 'UNIQUE',
                    'C': 'CHECK'
                }
                
                constraint_info = {
                    "name": constraint_name,
                    "type": type_map.get(constraint_type, constraint_type)
                }
                
                # Get columns involved in this constraint
                await cursor.execute("""
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE owner = :owner
                    AND constraint_name = :constraint_name
                    ORDER BY position
                """, owner=conn.username.upper(), constraint_name=constraint_name)
                
                columns = await cursor.fetchall()
                constraint_info["columns"] = [col[0] for col in columns]
                
                # If it's a foreign key, get the referenced table/columns
                if constraint_type == 'R':
                    await cursor.execute("""
                        SELECT ac.table_name,
                               acc.column_name
                        FROM all_constraints ac
                        JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name
                        WHERE ac.constraint_name = (
                            SELECT r_constraint_name
                            FROM all_constraints
                            WHERE owner = :owner
                            AND constraint_name = :constraint_name
                        )
                        AND acc.owner = ac.owner
                        ORDER BY acc.position
                    """, owner=conn.username.upper(), constraint_name=constraint_name)
                    
                    ref_info = await cursor.fetchall()
                    if ref_info:
                        constraint_info["references"] = {
                            "table": ref_info[0][0],
                            "columns": [col[1] for col in ref_info]
                        }
                
                # For check constraints, include the condition
                if constraint_type == 'C' and condition:
                    constraint_info["condition"] = condition
                
                result.append(constraint_info)
            
            # Cache the results
            self.schema_manager.update_cache('constraints', table_name, result)
            await self.schema_manager.save_cache()
            return result
        finally:
            await conn.close()
    
    async def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table indexes with caching"""
        if not self.schema_manager:
            raise RuntimeError("Schema manager not initialized")
            
        if self.schema_manager.is_cache_valid('indexes', table_name):
            self.schema_manager.cache_stats['hits'] += 1
            return self.schema_manager.object_cache['indexes'][table_name]['data']
        
        self.schema_manager.cache_stats['misses'] += 1
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Get all indexes for the table
            await cursor.execute("""
                SELECT ai.index_name,
                       ai.uniqueness,
                       ai.tablespace_name,
                       ai.status
                FROM all_indexes ai
                WHERE ai.owner = :owner
                AND ai.table_name = :table_name
            """, owner=conn.username.upper(), table_name=table_name.upper())
            
            indexes = await cursor.fetchall()
            result = []
            
            for index_name, uniqueness, tablespace, status in indexes:
                index_info = {
                    "name": index_name,
                    "unique": uniqueness == 'UNIQUE'
                }
                
                if tablespace:
                    index_info["tablespace"] = tablespace
                
                if status:
                    index_info["status"] = status
                
                # Get columns in this index
                await cursor.execute("""
                    SELECT column_name
                    FROM all_ind_columns
                    WHERE index_owner = :owner
                    AND index_name = :index_name
                    ORDER BY column_position
                """, owner=conn.username.upper(), index_name=index_name)
                
                columns = await cursor.fetchall()
                index_info["columns"] = [col[0] for col in columns]
                
                result.append(index_info)
            
            # Cache the results
            self.schema_manager.update_cache('indexes', table_name, result)
            await self.schema_manager.save_cache()
            return result
        finally:
            await conn.close()
    
    async def get_dependent_objects(self, object_name: str) -> List[Dict[str, Any]]:
        """Get objects that depend on the specified object"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            await cursor.execute("""
                SELECT ao.object_name, ao.object_type, ao.owner
                FROM all_dependencies ad
                JOIN all_objects ao ON ad.name = ao.object_name 
                                   AND ad.type = ao.object_type
                                   AND ad.owner = ao.owner
                WHERE ad.referenced_name = :object_name
                AND ad.referenced_owner = :owner
            """, object_name=object_name, owner=conn.username.upper())
            
            dependencies = await cursor.fetchall()
            result = []
            
            for name, obj_type, owner in dependencies:
                result.append({
                    "name": name,
                    "type": obj_type,
                    "owner": owner
                })
            
            return result
        except oracledb.Error as e:
            print(f"Error getting dependent objects: {str(e)}", file=sys.stderr)
            raise
        finally:
            await conn.close()
    
    async def get_user_defined_types(self, type_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get user-defined types with caching"""
        if not self.schema_manager:
            raise RuntimeError("Schema manager not initialized")
            
        cache_key = type_pattern or 'all'
        if self.schema_manager.is_cache_valid('types', cache_key):
            self.schema_manager.cache_stats['hits'] += 1
            return self.schema_manager.object_cache['types'][cache_key]['data']
        
        self.schema_manager.cache_stats['misses'] += 1
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            where_clause = "WHERE owner = :owner"
            params = {"owner": conn.username.upper()}
            
            if type_pattern:
                where_clause += " AND type_name LIKE :type_pattern"
                params["type_pattern"] = type_pattern.upper()
            
            await cursor.execute(f"""
                SELECT type_name, typecode
                FROM all_types
                {where_clause}
                ORDER BY type_name
            """, **params)
            
            types = await cursor.fetchall()
            result = []
            
            for type_name, typecode in types:
                type_info = {
                    "name": type_name,
                    "type_category": typecode,
                    "owner": conn.username.upper()
                }
                
                # For object types, get attributes
                if (typecode == 'OBJECT'):
                    await cursor.execute("""
                        SELECT attr_name, attr_type_name
                        FROM all_type_attrs
                        WHERE owner = :owner
                        AND type_name = :type_name
                        ORDER BY attr_no
                    """, owner=conn.username.upper(), type_name=type_name)
                    
                    attrs = await cursor.fetchall()
                    if attrs:
                        type_info["attributes"] = [
                            {"name": attr[0], "type": attr[1]} for attr in attrs
                        ]
                
                result.append(type_info)
            
            # Cache the results
            self.schema_manager.update_cache('types', cache_key, result)
            await self.schema_manager.save_cache()
            return result
        finally:
            await conn.close()