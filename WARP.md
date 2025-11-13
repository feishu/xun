# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

Xun Database is an object-relational mapper (ORM) written in Go that supports JSON schema. It provides a query builder and schema builder that can change table structure at runtime, making it especially suitable for Low-Code applications. The name "Xun" (巽) comes from one of the eight trigrams, symbolizing wind and objects that fill everywhere.

## Core Architecture

### Three-Layer Architecture

1. **Capsule Layer** (`capsule/`) - Connection management
   - Manages database connections and connection pools
   - Provides global access to schema and query builders
   - Handles connection configuration (primary/read-only)
   - Entry point: `capsule.Global` for global manager access

2. **DBAL Layer** (`dbal/`) - Database Abstraction Layer
   - **Query Builder** (`dbal/query/`) - Fluent query construction
   - **Schema Builder** (`dbal/schema/`) - Dynamic schema management
   - Defines interfaces for database operations
   - Grammar-agnostic query and schema representations

3. **Grammar Layer** (`grammar/`) - Database-specific implementations
   - `grammar/mysql/` - MySQL-specific SQL generation
   - `grammar/postgres/` - PostgreSQL-specific SQL generation
   - `grammar/sqlite3/` - SQLite3-specific SQL generation
   - `grammar/sql/` - Base SQL grammar
   - Each grammar implements SQL compilation for queries and schema changes

### Key Design Patterns

- **Registry Pattern**: Grammars are registered via `dbal.Register()` during package initialization
- **Builder Pattern**: Query and Schema builders provide fluent interfaces
- **Factory Pattern**: `NewQuery()`, `NewTable()` create instances with proper defaults
- **Connection Pool**: Separate primary and read-only connection pools

### Core Data Types

- `xun.R` - Generic row type (alias for `map[string]interface{}`)
- `xun.N` - Numeric wrapper with type conversion utilities
- `xun.T` - Time wrapper supporting multiple formats
- `xun.P` - Paginator with metadata

## Development Commands

### Building and Testing

```bash
# Run all tests with coverage
make test

# Run tests for specific packages (query, schema, capsule)
# Tests are filtered via TESTFOLDER in Makefile

# Format code
make fmt

# Check formatting without modifying files
make fmt-check

# Run vet (static analysis)
make vet

# Run linter
make lint

# Check spelling
make misspell-check

# Fix spelling errors
make misspell

# Install development tools
make tools
```

### Running Specific Tests

Tests use environment variables for configuration:

```bash
# Set database driver (mysql, postgres, sqlite3)
export XUN_UNIT_DRIVER="mysql"

# Set connection string
export XUN_UNIT_SOURCE="root:123456@tcp(localhost:3306)/xun"

# Set database name (for test filtering)
export XUN_UNIT_NAME="MySQL8.0"

# Set log file path
export XUN_UNIT_LOG="/logs/mysql.log"

# Run a single test file
go test -v ./dbal/schema/blueprint_test.go

# Run a specific test function
go test -v -run TestBlueprintInteger ./dbal/schema/
```

### Testing Against Multiple Databases

The CI workflow tests against:
- MySQL 5.6, 5.7, 8.0
- PostgreSQL 9.6, 14.0
- SQLite3

Use `unit.Is()`, `unit.Not()`, `unit.DriverIs()`, `unit.DriverNot()` for conditional test logic.

## Code Organization

### Capsule Usage Pattern

```go
// Initialize global capsule
import "github.com/yaoapp/xun/capsule"

manager, err := capsule.Add("default", "mysql", dsn)
// Sets capsule.Global automatically

// Use global capsule
schema := capsule.Schema()
query := capsule.Query()
```

### Query Builder Pattern

```go
// Get all records
rows, err := query.Table("users").Get()

// Get first record
row, err := query.Table("users").First()

// Find by ID
row, err := query.Table("users").Find(1)

// Conditional queries
query.Table("users").Where("age", ">", 18).Get()
```

### Schema Builder Pattern

```go
// Create table
schema.CreateTable("users", func(table schema.Blueprint) {
    table.ID("id")
    table.String("name", 100)
    table.Integer("age")
    table.Timestamps()
})

// Modify table
schema.AlterTable("users", func(table schema.Blueprint) {
    table.String("email", 255).Nullable()
})

// Drop table
schema.DropTableIfExists("users")
```

## Testing Conventions

### Test File Organization

- Each implementation file has a corresponding `_test.go` file
- Tests are in the same package as the code being tested
- Integration tests use `unit.Is()` to conditionally run database-specific tests

### Common Test Patterns

```go
func TestFeature(t *testing.T) {
    // Use testify/assert for assertions
    assert := assert.New(t)
    
    // Skip tests conditionally
    if unit.Not("mysql") {
        t.Skip("This test requires MySQL")
    }
    
    // Get test builder
    builder := getTestBuilder()
    
    // Clean up after test
    defer builder.DropTableIfExists("test_table")
}
```

### Unit Testing Helpers

The `unit` package provides utilities:
- `unit.DSN()` - Get test database DSN from environment
- `unit.Driver()` - Get test database driver
- `unit.Is(name)` - Check if current test DB matches name
- `unit.DriverIs(name)` - Check if current driver matches name
- `unit.Catch()` - Recover and log panics in tests

## Grammar Implementation

When adding a new database grammar:

1. Create directory in `grammar/` (e.g., `grammar/kingbase/`)
2. Implement query compilation (SELECT, INSERT, UPDATE, DELETE)
3. Implement schema compilation (CREATE, ALTER, DROP table/column/index)
4. Implement hooks for query logging/debugging
5. Register grammar via `dbal.Register("grammarname", grammar)`
6. Import grammar in test files with `_ "github.com/yaoapp/xun/grammar/grammarname"`

## Important Notes

### Database Connection Management

- Connections are stored in `manager.Pool` with Primary and Readonly slices
- First connection added becomes the global default
- Connection timeout defaults to 1 second during initialization
- Use `context.WithTimeout` for connection health checks

### Type Conversions

- Use `xun.MakeR()` to convert structs/maps to `xun.R`
- Use `xun.MakeTime()` for flexible time parsing
- Use `xun.MakeN()` for safe numeric conversions
- All types support JSON marshaling/unmarshaling

### Query Bindings

- Bindings are organized by clause type: select, from, join, where, etc.
- Use `builder.GetBindings()` to retrieve flattened binding array
- Never concatenate values directly into SQL (prevents injection)

### Panic vs Error Handling

- Regular methods return `(result, error)`
- `Must*` variants panic on error (e.g., `MustGet()`, `MustFirst()`)
- Use `Must*` variants when errors should halt execution

## Dependencies

- `github.com/yaoapp/kun` - Utility library (logging via `kun/log`)
- `github.com/jmoiron/sqlx` - SQL extensions
- `github.com/stretchr/testify` - Testing assertions
- Database drivers: `mysql`, `postgres`, `sqlite3`

## Naming Conventions

- Files: `snake_case.go`
- Types: `PascalCase`
- Functions: `camelCase` (exported) or `CamelCase` (public)
- Test functions: `TestFeatureName(t *testing.T)`
- Struct tags: Use `json:"field_name"` for JSON serialization
