# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an MCP (Model Context Protocol) server for Oracle databases that provides contextual database schema information to AI assistants. The server enables AI models to understand and work with large Oracle databases by intelligently caching and serving schema information on demand.

## Development Commands

### Environment Setup
- Install dependencies: `uv pip install -e .`
- Install development dependencies: `uv pip install -e ".[dev]"`
- Create virtual environment: `uv venv && source .venv/bin/activate` (Unix/macOS) or `.venv\Scripts\activate` (Windows)

### Testing
- Run all tests: `pytest`
- Run with verbose output: `pytest -v`
- Run specific test file: `pytest tests/unit/test_sql_classification.py`
- Run integration tests: `pytest tests/integration/`

### Code Quality
- Format code: `black .`
- Lint code: `ruff check .`
- Type checking: `mypy .`

### Development Server
- Run MCP server directly: `uv run main.py`
- Test with MCP Inspector: `mcp dev main.py` (requires `uv pip install mcp-cli`)

## Architecture

The codebase follows a three-layer architecture:

### Core Components

1. **DatabaseConnector** (`db_context/database.py`)
   - Manages Oracle database connections and query execution
   - Implements connection pooling with async support
   - Handles both thin mode (default) and thick mode connections
   - Provides read-only mode security (default: enabled)
   - Supports Oracle 11g+ (11g automatically uses thick mode)

2. **SchemaManager** (`db_context/schema/manager.py`)
   - Implements intelligent schema caching with lazy loading
   - Provides optimized schema lookup and search capabilities
   - Manages persistent cache on disk with TTL-based expiration
   - Handles cache statistics and performance monitoring

3. **DatabaseContext** (`db_context/__init__.py`)
   - High-level interface that coordinates between components
   - Exposes MCP tools and handles authorization
   - Manages application lifecycle (initialization, cleanup)

4. **MCP Server** (`main.py`)
   - FastMCP server implementation with 15+ database tools
   - Handles environment configuration and connection setup
   - Implements all MCP tool endpoints with proper error handling

### Key Design Patterns

- **Lazy Loading**: Schema information is loaded only when requested
- **Caching Strategy**: Multi-level caching (in-memory + disk) with TTL
- **Connection Pooling**: Async connection pool for optimal performance
- **Error Handling**: Comprehensive error handling with proper user feedback

## Environment Variables

Required:
- `ORACLE_CONNECTION_STRING`: Oracle database connection string

Optional:
- `TARGET_SCHEMA`: Schema override (defaults to user's schema)
- `CACHE_DIR`: Cache directory path (default: `.cache`)
- `THICK_MODE`: Enable thick mode (`1` or `true`)
- `ORACLE_CLIENT_LIB_DIR`: Custom Oracle client library path
- `READ_ONLY_MODE`: Security mode (`1` for read-only, `0` for write access, default: `1`)

## Testing Strategy

The test suite is organized into:

- **Unit Tests** (`tests/unit/`): Test individual components in isolation
- **Integration Tests** (`tests/integration/`): Test component interactions and database operations
- **Test Configuration**: Uses `pytest.ini` with `asyncio_mode = auto`

Key test areas:
- SQL query classification and security
- Schema formatting and sanitization
- Read-only vs write mode enforcement
- Query result formatting
- Foreign key relationship discovery

## Database Schema Caching

The schema manager implements a sophisticated caching strategy:

1. **Initial Build**: Loads all table names on startup (lazy schema loading)
2. **Lazy Loading**: Table details loaded only when requested
3. **Persistent Cache**: Schema data saved to disk for faster subsequent starts
4. **TTL Management**: Different cache timeouts for different data types
5. **Cache Stats**: Tracks hits/misses for performance monitoring

## MCP Tools Architecture

Each tool in `main.py` follows this pattern:
- Input validation and parameter processing
- Context retrieval from lifespan manager
- Database operation through DatabaseContext
- Error handling with user-friendly messages
- Result formatting (using `wrap_untrusted` for security)

## Security Considerations

- **Read-Only Mode**: Default security setting prevents write operations
- **SQL Classification**: Automatic detection of SELECT vs write operations
- **Input Sanitization**: All user inputs are properly sanitized
- **Error Wrapping**: Database errors are wrapped to prevent information leakage