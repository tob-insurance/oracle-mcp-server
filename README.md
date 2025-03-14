# MCP Oracle DB Context

A powerful Model Context Protocol (MCP) server that provides contextual database schema information for large Oracle databases, enabling AI assistants to understand and work with databases containing thousands of tables.

## Overview

The MCP Oracle DB Context server solves a critical challenge when working with very large Oracle databases: how to provide AI models with accurate, relevant database schema information without overwhelming them with tens of thousands of tables and relationships.

By intelligently caching and serving database schema information, this server allows AI assistants to:
- Look up specific table schemas on demand
- Search for tables that match specific patterns
- Understand table relationships and foreign keys
- Get database vendor information

## Features

- **Smart Schema Caching**: Builds and maintains a local cache of your database schema to minimize database queries
- **Targeted Schema Lookup**: Retrieve schema for specific tables without loading the entire database structure
- **Table Search**: Find tables by name pattern matching
- **Relationship Mapping**: Understand foreign key relationships between tables
- **Oracle Database Support**: Built specifically for Oracle databases
- **MCP Integration**: Works seamlessly with GitHub Copilot in VSCode, Claude, ChatGPT, and other AI assistants that support MCP

## Installation

### Prerequisites

- Python 3.12+
- Oracle database access
- Oracle instant client (for the `oracledb` Python package)

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/mcp-db-context.git
   cd mcp-db-context
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Using uv (recommended)
   uv pip install -e .
   
   # Or with pip
   pip install -e .
   ```

3. Create a `.env` file with your Oracle connection string:
   ```
   ORACLE_CONNECTION_STRING=username/password@hostname:port/service_name
   CACHE_DIR=.cache  # Optional: defaults to .cache
   ```

## Usage

### Running the Server

To start the MCP server:

```bash
python main.py
```

For development and testing, you can use MCP's development tools:

```bash
# Install MCP CLI tools if you haven't already
pip install "mcp[cli]"

# Run in development mode with the MCP Inspector
mcp dev main.py

# Install in Claude Desktop
mcp install main.py
```

### Available Tools

When connected to an AI assistant like Claude, the following tools will be available:

#### `get_table_schema`
Get detailed schema information for a specific table.

Example:
```
Can you show me the schema for the EMPLOYEES table?
```

#### `get_tables_schema`
Get schema information for multiple tables at once.

Example:
```
Please provide the schemas for both EMPLOYEES and DEPARTMENTS tables.
```

#### `search_tables_schema`
Search for tables by name pattern and retrieve their schemas.

Example:
```
Find all tables that might be related to customers and show their schemas.
```

#### `rebuild_schema_cache`
Force a rebuild of the schema cache. Use sparingly as this is resource-intensive.

Example:
```
The database structure has changed. Could you rebuild the schema cache?
```

#### `get_database_vendor_info`
Get information about the connected Oracle database version.

Example:
```
What Oracle database version are we running?
```

## How It Works

This MCP server works by:

1. **Initial Cache Building**: When first started, it builds a cache of all table names in the database.
2. **On-Demand Schema Loading**: Detailed table information is only loaded when requested, minimizing database load.
3. **Persistent Cache**: Schema information is cached to disk to improve performance across restarts.
4. **Resource Optimization**: The architecture is designed for databases with thousands of tables where loading everything would be impractical.

The server uses a three-layer architecture:

- **DatabaseConnector**: Handles raw Oracle database connections and queries
- **SchemaManager**: Manages the schema cache and provides optimized schema access
- **DatabaseContext**: Provides a high-level interface for the MCP tools

## Technical Requirements

- **Oracle Database**: This MCP server requires an Oracle database and connection credentials
- **Memory Requirements**: For very large databases (10,000+ tables), ensure your machine has at least 4GB of available memory
- **Disk Space**: The schema cache is stored as a JSON file, typically a few MB in size

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.