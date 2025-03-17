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
        """Get a list of all table names in the database using optimized query"""
        conn = await self.get_connection()
        try:
            print("Getting list of all tables...", file=sys.stderr)
            cursor = conn.cursor()
            # Using RESULT_CACHE hint for frequently accessed data
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE */ table_name 
                FROM all_tables 
                WHERE owner = :owner
                ORDER BY table_name
            """, owner=conn.username.upper())
            
            all_tables = await cursor.fetchall()
            return {t[0] for t in all_tables}
        finally:
            await conn.close()
    
    async def load_table_details(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Load detailed schema information for a specific table with optimized queries"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            # Check if the table exists using result cache
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE */ COUNT(*) 
                FROM all_tables 
                WHERE owner = :owner AND table_name = :table_name
            """, owner=conn.username.upper(), table_name=table_name.upper())
            
            count = await cursor.fetchone()
            if count[0] == 0:
                return None
                
            # Get column information using result cache and index hints
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE INDEX(atc) */ 
                    column_name, data_type, nullable
                FROM all_tab_columns atc
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
            
            # Get relationship information using optimized join order and result cache
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE LEADING(ac acc rcc) USE_NL(acc) USE_NL(rcc) */
                    acc.column_name,
                    rcc.table_name AS referenced_table,
                    rcc.column_name AS referenced_column
                FROM all_constraints ac
                JOIN all_cons_columns acc ON acc.constraint_name = ac.constraint_name
                                        AND acc.owner = ac.owner
                JOIN all_cons_columns rcc ON rcc.constraint_name = ac.r_constraint_name
                                        AND rcc.owner = ac.r_owner
                WHERE ac.constraint_type = 'R'
                AND ac.owner = :owner
                AND ac.table_name = :table_name
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
        """Get PL/SQL objects"""
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
        """Get table constraints"""
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
            
            return result
        finally:
            await conn.close()
    
    async def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table indexes"""
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
            
            return result
        finally:
            await conn.close()
    
    async def get_dependent_objects(self, object_name: str) -> List[Dict[str, Any]]:
        """Get objects that depend on the specified object"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            await cursor.execute("""
                WITH deps AS (
                    SELECT /*+ MATERIALIZE */
                           name, type, owner
                    FROM all_dependencies
                    WHERE referenced_name = :object_name
                    AND referenced_owner = :owner
                )
                SELECT /*+ LEADING(deps) USE_NL(ao) INDEX(ao) */
                    ao.object_name, ao.object_type, ao.owner
                FROM deps
                JOIN all_objects ao ON deps.name = ao.object_name 
                                   AND deps.type = ao.object_type
                                   AND deps.owner = ao.owner
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
        """Get user-defined types"""
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
            
            return result
        finally:
            await conn.close()
    
    async def get_related_tables(self, table_name: str) -> Dict[str, List[str]]:
        """Get all tables that are related to the specified table through foreign keys."""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            table_owner = conn.username.upper()
            
            # Get tables referenced by this table
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE LEADING(ac acc) USE_NL(acc) */
                    DISTINCT acc.table_name AS referenced_table
                FROM all_constraints ac
                JOIN all_cons_columns acc ON acc.constraint_name = ac.r_constraint_name
                    AND acc.owner = ac.owner
                WHERE ac.constraint_type = 'R'
                AND ac.table_name = :table_name
                AND ac.owner = :owner
            """, table_name=table_name.upper(), owner=table_owner)
            
            referenced_tables = [row[0] for row in await cursor.fetchall()]
            
            # Get tables that reference this table
            await cursor.execute("""
                WITH pk_constraints AS (
                    SELECT /*+ MATERIALIZE */ constraint_name
                    FROM all_constraints
                    WHERE table_name = :table_name
                    AND constraint_type IN ('P', 'U')
                    AND owner = :owner
                )
                SELECT /*+ RESULT_CACHE LEADING(ac pk) USE_NL(pk) */
                    DISTINCT ac.table_name AS referencing_table
                FROM all_constraints ac
                JOIN pk_constraints pk ON ac.r_constraint_name = pk.constraint_name
                WHERE ac.constraint_type = 'R'
                AND ac.owner = :owner
            """, table_name=table_name.upper(), owner=table_owner)
            
            referencing_tables = [row[0] for row in await cursor.fetchall()]
            
            return {
                'referenced_tables': referenced_tables,
                'referencing_tables': referencing_tables
            }
            
        finally:
            await conn.close()
    
    async def search_in_database(self, search_term: str, limit: int = 20) -> List[str]:
        """Search for table names in the database using similarity matching"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            # Use Oracle's built-in similarity features
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE */ DISTINCT table_name 
                FROM all_tables 
                WHERE owner = :owner
                AND (
                    -- Direct matches first
                    UPPER(table_name) LIKE '%' || :search_term || '%'
                    -- Then similar names using built-in similarity calculation
                    OR UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                        UPPER(table_name),
                        :search_term
                    ) > 65  -- Minimum similarity threshold (65%)
                )
                ORDER BY 
                    CASE 
                        WHEN UPPER(table_name) LIKE '%' || :search_term || '%' THEN 0
                        ELSE 1
                    END,
                    UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                        UPPER(table_name),
                        :search_term
                    ) DESC
            """, owner=conn.username.upper(), search_term=search_term.upper())
            
            results = await cursor.fetchall()
            return [row[0] for row in results][:limit]
            
        finally:
            await conn.close()
            
    async def search_columns_in_database(self, table_names: List[str], search_term: str) -> Dict[str, List[Dict[str, Any]]]:
        """Search for columns in specified tables"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            result = {}
            
            # Get columns for the specified tables that match the search term
            await cursor.execute("""
                SELECT /*+ RESULT_CACHE */ 
                    table_name,
                    column_name,
                    data_type,
                    nullable
                FROM all_tab_columns 
                WHERE owner = :owner
                AND table_name IN (SELECT column_value FROM TABLE(CAST(:table_names AS SYS.ODCIVARCHAR2LIST)))
                AND UPPER(column_name) LIKE '%' || :search_term || '%'
                ORDER BY table_name, column_id
            """, owner=conn.username.upper(), 
                table_names=table_names,
                search_term=search_term.upper())
            
            rows = await cursor.fetchall()
            
            for table_name, column_name, data_type, nullable in rows:
                if table_name not in result:
                    result[table_name] = []
                result[table_name].append({
                    "name": column_name,
                    "type": data_type,
                    "nullable": nullable == 'Y'
                })
            
            return result
            
        finally:
            await conn.close()
    
    async def explain_query_plan(self, query: str) -> Dict[str, Any]:
        """Get execution plan for a SQL query"""
        conn = await self.get_connection()
        try:
            cursor = conn.cursor()
            
            # First create an explain plan
            plan_statement = f"EXPLAIN PLAN FOR {query}"
            await cursor.execute(plan_statement)
            
            # Then retrieve the execution plan with cost and cardinality information
            await cursor.execute("""
                SELECT 
                    LPAD(' ', 2*LEVEL-2) || operation || ' ' || 
                    options || ' ' || object_name || 
                    CASE 
                        WHEN cost IS NOT NULL THEN ' (Cost: ' || cost || ')'
                        ELSE ''
                    END || 
                    CASE 
                        WHEN cardinality IS NOT NULL THEN ' (Rows: ' || cardinality || ')'
                        ELSE ''
                    END as execution_plan_step
                FROM plan_table
                START WITH id = 0
                CONNECT BY PRIOR id = parent_id
                ORDER SIBLINGS BY position
            """)
            
            plan_rows = await cursor.fetchall()
            
            # Clear the plan table for next time
            await cursor.execute("DELETE FROM plan_table")
            await conn.commit()
            
            # Also get some basic optimization hints based on query content
            basic_analysis = self._analyze_query_for_optimization(query)
            
            return {
                "execution_plan": [row[0] for row in plan_rows],
                "optimization_suggestions": basic_analysis
            }
        except oracledb.Error as e:
            print(f"Error explaining query: {str(e)}", file=sys.stderr)
            return {
                "execution_plan": [],
                "optimization_suggestions": ["Unable to generate execution plan due to error."],
                "error": str(e)
            }
        finally:
            await conn.close()
            
    def _analyze_query_for_optimization(self, query: str) -> List[str]:
        """Simple heuristic analysis of query for basic optimization suggestions"""
        query = query.upper()
        suggestions = []
        
        # Check for common inefficient patterns
        if "SELECT *" in query:
            suggestions.append("Consider selecting only needed columns instead of SELECT *")
            
        if " LIKE '%something" in query or " LIKE '%something%'" in query:
            suggestions.append("Leading wildcards in LIKE predicates prevent index usage")
            
        if " IN (SELECT " in query and " EXISTS" not in query:
            suggestions.append("Consider using EXISTS instead of IN with subqueries for better performance")
            
        if " OR " in query:
            suggestions.append("OR conditions may prevent index usage. Consider UNION ALL of separated queries")
            
        if "/*+ " not in query and len(query) > 500:
            suggestions.append("Complex query could benefit from optimizer hints")
            
        if " JOIN " in query:
            if "/*+ LEADING" not in query and query.count("JOIN") > 2:
                suggestions.append("Multi-table joins may benefit from LEADING hint to control join order")
            
            if "/*+ USE_NL" not in query and "/*+ USE_HASH" not in query and query.count("JOIN") > 1:
                suggestions.append("Consider join method hints like USE_NL or USE_HASH for complex joins")
        
        # Count number of tables and joins
        join_count = query.count(" JOIN ")
        from_count = query.count(" FROM ")
        table_count = max(from_count, join_count + 1)
        
        if table_count > 4:
            suggestions.append(f"Query joins {table_count} tables - consider reviewing join order and conditions")
            
        return suggestions