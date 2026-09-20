# Xun DBAL - Project Rules

Reply in Chinese; use Conventional Commits with Chinese or English descriptions (e.g., `feat(query): ...`, `fix(schema): ...`).

## Overview

Xun is an Eloquent-inspired Database Abstraction Layer (DBAL) for Go. It powers the data access tier of the Yao engine, offering a fluent SQL query builder, multi-dialect schema migration engine (Blueprint), global connection manager (Capsule), and high-throughput execution layer across MySQL, PostgreSQL, SQLite3, and Dameng.

- **Ecosystem Role**: Database access tier imported by `gou` and `yao`; relies on `kun`.

## Tech Stack

- **Core Runtime**: Go 1.24+ (Toolchain 1.25.5)
- **SQL Execution**: `jmoiron/sqlx`, `qustavo/sqlhooks/v2`
- **Supported Drivers**:
  - MySQL (`github.com/go-sql-driver/mysql`)
  - PostgreSQL (`github.com/lib/pq`)
  - SQLite3 (`github.com/mattn/go-sqlite3`)
  - Dameng (`gitee.com/chunanyong/dm`)
- **Foundations**: `github.com/yaoapp/kun`
- **Testing**: `github.com/stretchr/testify`

## Common Commands & Fast Feedback Loop

```bash
# Fast Feedback Inner Loop (<3s, SQLite / Fixtures)
XUN_MODE=test go test -v -run TestQueryWhere ./dbal/query/...  # Test fluent query building
XUN_MODE=test go test -v -run TestSchema ./dbal/schema/...     # Test schema migration DDL
go test -v ./capsule/...                                      # Test Capsule connection pool
make vet                                                      # Static analysis (go vet)

# Full DBAL Suite
make test                               # Run full DBAL test suite
make fmt                                # Format Go files with gofmt -s
make lint                               # Run golint check
```

## Navigation & Key Entrypoints

| Subsystem / Layer | Primary Entry File / Directory |
| :--- | :--- |
| **Global Connection Manager** | `capsule/capsule.go`, `capsule/query.go` |
| **Fluent Query Builder & AST** | `dbal/query/builder.go`, `dbal/query/where.go` |
| **Direct Write Execution Engine** | `dbal/query/exec.go` |
| **Schema Migration Blueprint** | `dbal/schema/blueprint.go`, `dbal/schema/schema.go` |
| **Dialect Grammar Compilers** | `grammar/mysql/`, `grammar/postgres/`, `grammar/sqlite3/`, `grammar/dm/` |
| **Row Scanning & Memory Allocation** | `dbal/query/support.go` |

## Directory Structure

```
xun/
├── capsule/           # Global database manager (Eloquent Capsule pattern)
├── dbal/              # Core DBAL interfaces, Connection, Driver & Quoter
│   ├── query/         # Fluent query builder (AST builder, Where, Join, Paginate, Exec)
│   └── schema/        # Schema definition & migration builder (Blueprint, Column, Index)
├── grammar/           # Dialect-specific SQL grammar compilers
│   ├── mysql/         # MySQL dialect syntax & schema inspection
│   ├── postgres/      # PostgreSQL dialect syntax & quoting
│   ├── sqlite3/       # SQLite3 dialect syntax & constraint emulation
│   └── dm/            # Dameng database dialect support
├── global/            # Global constants and type aliases
├── unit/              # Test suite fixtures, driver initialization & mock DBs
└── types.go           # Core data types (Row, Map, Collection)
```

## System Invariants (DO NOT BREAK)

1. **Dialect Isolation Axiom**: Never hardcode dialect-specific SQL syntax or quotes inside `dbal/query` or `dbal/schema`. All SQL compilation MUST be delegated to dialect grammars.
2. **Context-Aware Execution**: Every database query MUST propagate `context.Context` (`QueryContext`, `ExecContext`, `PaginateContext`) to ensure immediate cancellation on abort.
3. **Builder Concurrency Isolation**: `Builder` instances are NOT thread-safe. Always create a new `Builder` per query via `capsule.Query()` or `builder.Clone()`.

## Footguns & Anti-Patterns (DO NOT)

- **DO NOT** re-introduce `db.Prepare` into `Exec` or `ExecWrite` (direct `db.ExecContext` saves 2x RTT latency).
- **DO NOT** assemble raw SQL fragments with string interpolation (prevents SQL injection; always use parameter bindings).
- **DO NOT** ignore context cancellation in row-scanning loops in `dbal/query/support.go`.
