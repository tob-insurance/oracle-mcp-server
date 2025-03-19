from mcp.server.fastmcp import FastMCP, Context
import json
import os
import sys
from typing import Dict, List, AsyncIterator, Optional
import time
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv

from db_context import DatabaseContext

# Load environment variables from .env file
load_dotenv()

ORACLE_CONNECTION_STRING = os.getenv('ORACLE_CONNECTION_STRING')
TARGET_SCHEMA = os.getenv('TARGET_SCHEMA')  # Optional schema override
CACHE_DIR = os.getenv('CACHE_DIR', '.cache')

@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[DatabaseContext]:
    """Manage application lifecycle and ensure DatabaseContext is properly initialized"""
    print("App Lifespan initialising", file=sys.stderr)
    connection_string = ORACLE_CONNECTION_STRING
    if not connection_string:
        raise ValueError("ORACLE_CONNECTION_STRING environment variable is required. Set it in .env file or environment.")
    
    cache_dir = Path(CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    db_context = DatabaseContext(
        connection_string=connection_string,
        cache_path=cache_dir / 'schema_cache.json',
        target_schema=TARGET_SCHEMA
    )
    
    try:
        # Initialize cache on startup
        print("Initialising database cache...", file=sys.stderr)
        await db_context.initialize()
        yield db_context
    finally:
        # Cleanup isn't needed in this case
        pass

# Initialize FastMCP server
mcp = FastMCP("db-context", lifespan=app_lifespan)
print("FastMCP server initialized", file=sys.stderr)

@mcp.tool()
async def get_table_schema(table_name: str, ctx: Context) -> str:
    """
    Get the schema information for a specific table including columns, data types, nullability, and relationships.
    Use this when you need to understand the structure of a particular table to write queries against it.
    
    Args:
        table_name: The name of the table to get schema information for (case-insensitive)
    
    Returns:
        A formatted string containing the table's schema information including columns and relationships
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    table_info = await db_context.get_schema_info(table_name)
    
    if not table_info:
        return f"Table '{table_name}' not found in the schema."
    
    # Format the response
    result = [f"Table: {table_name}"]
    result.append("\nColumns:")
    for column in table_info.columns:
        nullable = "NULL" if column["nullable"] else "NOT NULL"
        result.append(f"  - {column['name']}: {column['type']} {nullable}")
    
    if table_info.relationships:
        result.append("\nRelationships:")
        for ref_table, rel in table_info.relationships.items():
            result.append(
                f"  - References {ref_table}({rel['foreign_column']}) "
                f"through {rel['local_column']}"
            )
    
    return "\n".join(result)

@mcp.tool()
async def rebuild_schema_cache(ctx: Context) -> str:
    """
    Force a rebuild of the schema cache. This is super costly and should be used sparingly, at the user explicit request.
    
    Returns:
        A message indicating the result of the rebuild operation
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    try:
        await db_context.rebuild_cache()
        cache_size = len(db_context.schema_manager.cache.all_table_names) if db_context.schema_manager.cache else 0
        return f"Schema cache rebuilt successfully. Indexed {cache_size} tables."
    except Exception as e:
        return f"Failed to rebuild schema cache: {str(e)}"

@mcp.tool()
async def get_tables_schema(table_names: List[str], ctx: Context) -> str:
    """
    Get the schema information for multiple tables at once. 
    This is much faster than calling get_table_schema for each table.
    If you want to get the schema for multiple tables, always prefer this function.
    
    Args:
        table_names: A list of table names to get schema information for
    
    Returns:
        A formatted string containing the schema information for all requested tables
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    results = []
    
    for table_name in table_names:
        table_info = await db_context.get_schema_info(table_name)
        if not table_info:
            results.append(f"\nTable '{table_name}' not found in the schema.")
            continue
        
        # Format the table info
        results.append(f"\nTable: {table_name}")
        results.append("Columns:")
        for column in table_info.columns:
            nullable = "NULL" if column["nullable"] else "NOT NULL"
            results.append(f"  - {column['name']}: {column['type']} {nullable}")
        
        if table_info.relationships:
            results.append("Relationships:")
            for ref_table, rel in table_info.relationships.items():
                results.append(
                    f"  - References {ref_table}({rel['foreign_column']}) "
                    f"through {rel['local_column']}"
                )
    
    return "\n".join(results)

@mcp.tool()
async def search_tables_schema(search_term: str, ctx: Context) -> str:
    """
    Search for tables with names similar to the provided search terms and return their schema information.
    Multiple terms can be provided separated by commas or whitespace.
    Use this when you aren't sure of the exact table name but know part of it, or when exploring tables 
    related to a specific domain or function.
    
    Args:
        search_term: One or more strings to search for in table names (case-insensitive), separated by commas or spaces
    
    Returns:
        A formatted string containing the schema information for all matching tables (up to 20 tables)
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    # Split search term by commas and whitespace and remove empty strings
    search_terms = [term.strip() for term in search_term.replace(',', ' ').split()]
    search_terms = [term for term in search_terms if term]
    
    if not search_terms:
        return "No valid search terms provided"
    
    # Track all matching tables without duplicates
    matching_tables = set()
    
    # Search for each term
    for term in search_terms:
        tables = await db_context.search_tables(term, limit=20)
        matching_tables.update(tables)
    
    # Convert back to list and limit to 20 results
    matching_tables = list(matching_tables)[:20]
    
    if not matching_tables:
        return f"No tables found matching any of these terms: {', '.join(search_terms)}"
    
    results = [f"Found {len(matching_tables)} tables matching terms ({', '.join(search_terms)}):"]
    
    # Now load the schema for each matching table
    for table_name in matching_tables:
        table_info = await db_context.get_schema_info(table_name)
        if not table_info:
            continue
        
        results.append(f"\nTable: {table_name}")
        results.append("Columns:")
        for column in table_info.columns:
            nullable = "NULL" if column["nullable"] else "NOT NULL"
            results.append(f"  - {column['name']}: {column['type']} {nullable}")
        
        if table_info.relationships:
            results.append("Relationships:")
            for ref_table, rel in table_info.relationships.items():
                results.append(
                    f"  - References {ref_table}({rel['foreign_column']}) "
                    f"through {rel['local_column']}"
                )
    
    return "\n".join(results)

@mcp.tool()
async def get_database_vendor_info(ctx: Context) -> str:
    """
    Returns the database vendor type and version by querying the connected Oracle database.
    This is useful to determine the type of database being used and the right syntax for queries.
    
    Returns:
        A formatted string containing the database vendor type and version information
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        db_info = await db_context.get_database_info()
        
        if not db_info:
            return "Could not retrieve database vendor information."
        
        result = [f"Database Vendor: {db_info.get('vendor', 'Unknown')}"]
        result.append(f"Version: {db_info.get('version', 'Unknown')}")
        if "schema" in db_info:
            result.append(f"Schema: {db_info['schema']}")
        
        if "additional_info" in db_info and db_info["additional_info"]:
            result.append("\nAdditional Version Information:")
            for info in db_info["additional_info"]:
                result.append(f"- {info}")
                
        if "error" in db_info:
            result.append(f"\nError: {db_info['error']}")
            
        return "\n".join(result)
    except Exception as e:
        return f"Error retrieving database vendor information: {str(e)}"

@mcp.tool()
async def search_columns(search_term: str, ctx: Context) -> str:
    """
    Search for tables containing columns that match the provided search term.
    This is extremely useful when you know what data you need (like 'customer_id' or 'order_date') 
    but aren't sure which tables contain this information. Essential for exploring large databases.
    
    Args:
        search_term: A string to search for in column names (case-insensitive)
    
    Returns:
        A formatted string listing tables and their matching columns (up to 50 results)
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        matching_columns = await db_context.search_columns(search_term, limit=50)
        
        if not matching_columns:
            return f"No columns found matching '{search_term}'"
        
        results = [f"Found columns matching '{search_term}' in {len(matching_columns)} tables:"]
        
        for table_name, columns in matching_columns.items():
            results.append(f"\nTable: {table_name}")
            results.append("Matching columns:")
            for col in columns:
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                results.append(f"  - {col['name']}: {col['type']} {nullable}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error searching columns: {str(e)}"

@mcp.tool()
async def get_pl_sql_objects(object_type: str, name_pattern: Optional[str], ctx: Context) -> str:
    """
    Get information about PL/SQL objects (procedures, functions, packages, triggers, etc).
    Use this to discover existing database code objects for analysis or debugging purposes.
    
    Args:
        object_type: Type of object to search for (PROCEDURE, FUNCTION, PACKAGE, TRIGGER, TYPE, etc.)
        name_pattern: Pattern to filter object names (case-insensitive, supports % wildcards)
                     e.g., "CUSTOMER%" will find all objects starting with "CUSTOMER"
    
    Returns:
        A formatted string containing information about the matching PL/SQL objects
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        objects = await db_context.get_pl_sql_objects(object_type.upper(), name_pattern)
        
        if not objects:
            pattern_msg = f" matching '{name_pattern}'" if name_pattern else ""
            return f"No {object_type.upper()} objects found{pattern_msg}"
        
        results = [f"Found {len(objects)} {object_type.upper()} objects:"]
        
        for obj in objects:
            results.append(f"\n{obj['type']}: {obj['name']}")
            if 'owner' in obj:
                results.append(f"Owner: {obj['owner']}")
            if 'status' in obj:
                results.append(f"Status: {obj['status']}")
            if 'created' in obj:
                results.append(f"Created: {obj['created']}")
            if 'last_modified' in obj:
                results.append(f"Last Modified: {obj['last_modified']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving PL/SQL objects: {str(e)}"

@mcp.tool()
async def get_object_source(object_type: str, object_name: str, ctx: Context) -> str:
    """
    Get the source code for a PL/SQL object (procedure, function, package, trigger, etc.).
    Essential for debugging, understanding, or optimizing existing database code.
    
    Args:
        object_type: Type of object (PROCEDURE, FUNCTION, PACKAGE, TRIGGER, etc.)
        object_name: Name of the object to retrieve source for
    
    Returns:
        A string containing the complete source code of the requested object
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        source = await db_context.get_object_source(object_type.upper(), object_name.upper())
        
        if not source:
            return f"No source found for {object_type} {object_name}"
        
        return f"Source for {object_type} {object_name}:\n\n{source}"
    except Exception as e:
        return f"Error retrieving object source: {str(e)}"

@mcp.tool()
async def get_table_constraints(table_name: str, ctx: Context) -> str:
    """
    Get constraints (primary keys, foreign keys, unique constraints, check constraints) for a table.
    Use this to understand the data integrity rules, relationships, and business rules encoded in the database.
    Critical for writing valid INSERT/UPDATE statements and understanding join conditions.
    
    Args:
        table_name: The name of the table to get constraints for
    
    Returns:
        A formatted string containing the table's constraints with detailed information
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        constraints = await db_context.get_table_constraints(table_name)
        
        if not constraints:
            return f"No constraints found for table '{table_name}'"
        
        results = [f"Constraints for table '{table_name}':"]
        
        for constraint in constraints:
            constraint_type = constraint.get('type', 'UNKNOWN')
            name = constraint.get('name', 'UNNAMED')
            
            results.append(f"\n{constraint_type} Constraint: {name}")
            
            if 'columns' in constraint:
                results.append(f"Columns: {', '.join(constraint['columns'])}")
                
            if constraint_type == 'FOREIGN KEY' and 'references' in constraint:
                ref = constraint['references']
                results.append(f"References: {ref['table']}({', '.join(ref['columns'])})")
                
            if 'condition' in constraint:
                results.append(f"Condition: {constraint['condition']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving constraints: {str(e)}"

@mcp.tool()
async def get_table_indexes(table_name: str, ctx: Context) -> str:
    """
    Get indexes defined on a table. 
    Essential for query optimization and understanding performance characteristics of the table.
    Use this information to improve query performance by leveraging existing indexes or suggesting new ones.
    
    Args:
        table_name: The name of the table to get indexes for
    
    Returns:
        A formatted string containing the table's indexes including column information
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        indexes = await db_context.get_table_indexes(table_name)
        
        if not indexes:
            return f"No indexes found for table '{table_name}'"
        
        results = [f"Indexes for table '{table_name}':"]
        
        for idx in indexes:
            idx_type = "UNIQUE " if idx.get('unique', False) else ""
            results.append(f"\n{idx_type}Index: {idx['name']}")
            results.append(f"Columns: {', '.join(idx['columns'])}")
            
            if 'tablespace' in idx:
                results.append(f"Tablespace: {idx['tablespace']}")
                
            if 'status' in idx:
                results.append(f"Status: {idx['status']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving indexes: {str(e)}"

@mcp.tool()
async def get_dependent_objects(object_name: str, ctx: Context) -> str:
    """
    Get objects that depend on the specified object (find usage references).
    
    Args:
        object_name: Name of the object to find dependencies for
    
    Returns:
        A formatted string containing objects that depend on the specified object
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        dependencies = await db_context.get_dependent_objects(object_name.upper())
        
        if not dependencies:
            return f"No objects found that depend on '{object_name}'"
        
        results = [f"Objects that depend on '{object_name}':"]
        
        for dep in dependencies:
            results.append(f"\n{dep['type']}: {dep['name']}")
            if 'owner' in dep:
                results.append(f"Owner: {dep['owner']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving dependencies: {str(e)}"

@mcp.tool()
async def get_user_defined_types(type_pattern: Optional[str], ctx: Context) -> str:
    """
    Get information about user-defined types in the database.
    
    Args:
        type_pattern: Pattern to filter type names (case-insensitive, supports % wildcards)
    
    Returns:
        A formatted string containing information about user-defined types
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        types = await db_context.get_user_defined_types(type_pattern)
        
        if not types:
            pattern_msg = f" matching '{type_pattern}'" if type_pattern else ""
            return f"No user-defined types found{pattern_msg}"
        
        results = [f"User-defined types:"]
        
        for typ in types:
            results.append(f"\nType: {typ['name']}")
            results.append(f"Type category: {typ['type_category']}")
            if 'owner' in typ:
                results.append(f"Owner: {typ['owner']}")
            if 'attributes' in typ and typ['attributes']:
                results.append("Attributes:")
                for attr in typ['attributes']:
                    results.append(f"  - {attr['name']}: {attr['type']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving user-defined types: {str(e)}"

@mcp.tool()
async def get_related_tables(table_name: str, ctx: Context) -> str:
    """
    Get all tables that are related to the specified table through foreign keys.
    This tool is critical for understanding the database schema relationships and building proper JOINs.
    Shows both tables referenced by this table and tables that reference this table.
    
    Args:
        table_name: The name of the table to find relationships for
    
    Returns:
        A formatted string showing all related tables in both directions (incoming and outgoing relationships)
    """
    db_context: DatabaseContext = ctx.request_context.lifespan_context
    
    try:
        related = await db_context.get_related_tables(table_name)
        
        if not related['referenced_tables'] and not related['referencing_tables']:
            return f"No related tables found for '{table_name}'"
        
        results = [f"Tables related to '{table_name}':"]
        
        if related['referenced_tables']:
            results.append("\nTables referenced by this table (outgoing foreign keys):")
            for table in related['referenced_tables']:
                results.append(f"  - {table}")
        
        if related['referencing_tables']:
            results.append("\nTables that reference this table (incoming foreign keys):")
            for table in related['referencing_tables']:
                results.append(f"  - {table}")
        
        return "\n".join(results)
        
    except Exception as e:
        return f"Error getting related tables: {str(e)}"

if __name__ == "__main__":
    mcp.run()
