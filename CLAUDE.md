# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension named `dqtest` (Data Quality Test) that enables defining, running, and tracking data quality tests directly within DuckDB. It provides a SQL-native framework similar to dbt tests — users insert test definitions into a `dq_tests` table, then run them via `dq_run_tests()`. Results are persisted in `dq_test_results`.

## Key Information

- **Extension Name**: `dqtest` (internal name) / `DqTest` (display name)
- **Language**: C++ (requires C++11 or later)
- **Build System**: CMake with custom Makefile wrapper
- **Dependency Management**: VCPKG (for OpenSSL and other dependencies)
- **Testing Framework**: SQLLogicTest (.test files in test/sql/)

## Building

### Initial Setup

If dependencies are needed (this extension uses OpenSSL):
```sh
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build Commands

```sh
# Build release version (default)
make

# Build debug version
make debug

# Build with specific configuration
make release    # or: make debug
```

Build artifacts are located in:
- `./build/release/duckdb` - DuckDB shell with extension loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/dqtest/dqtest.duckdb_extension` - Loadable extension binary

## Testing

```sh
# Run all SQL tests (default: runs release tests)
make test

# Run debug tests
make test_debug
```

Tests are SQLLogicTest files located in `test/sql/*.test`. Each test file follows this format:
- Header comments with name, description, and group
- `statement error` or `statement ok` for statements
- `query I/T` for queries with expected results
- `require dqtest` ensures the extension is loaded

To run a single test file, use the unittest binary directly:
```sh
./build/release/test/unittest "test/sql/dq_test.test"
```

## Code Architecture

### Source Files

```
src/
├── dqtest_extension.cpp       # Entry point: LoadInternal() registers all functions
├── dq_schema.cpp              # dq_init() — creates dq_tests and dq_test_results tables
├── dq_functions.cpp           # dq_run_tests() table function with optional filters
├── dq_compiler.cpp            # Compiles test definitions to SQL queries
├── dq_executor.cpp            # Executes compiled SQL, determines pass/warn/fail status
└── include/
    ├── dqtest_extension.hpp   # DqtestExtension class declaration
    ├── dq_schema.hpp          # RegisterDQSchemaFunctions()
    ├── dq_functions.hpp       # RegisterDQFunctions()
    ├── dq_compiler.hpp        # DQCompiler class
    └── dq_executor.hpp        # DQExecutor class + DQTestResult struct
```

### Extension Entry Point (`src/dqtest_extension.cpp`)

`LoadInternal()` registers two function groups:
1. `RegisterDQSchemaFunctions(loader)` — registers `dq_init`
2. `RegisterDQFunctions(loader)` — registers `dq_run_tests`

### Database Schema (`src/dq_schema.cpp`)

`dq_init()` creates two tables and indexes (idempotent via `IF NOT EXISTS`):

**`dq_tests`** — test definitions:
| Column | Type | Notes |
|---|---|---|
| `test_id` | VARCHAR PK | auto UUID |
| `test_name` | VARCHAR | required |
| `table_name` | VARCHAR | required |
| `column_name` | VARCHAR | nullable (not needed for `row_count`, `custom_sql`) |
| `test_type` | VARCHAR | required — see test types below |
| `test_params` | VARCHAR | JSON string with type-specific parameters |
| `severity` | VARCHAR | `'error'` (default) or `'warn'` |
| `error_if` | VARCHAR | threshold expression, e.g. `>0`, `>=10%` |
| `warn_if` | VARCHAR | threshold expression |
| `tags` | VARCHAR[] | for filtering |
| `enabled` | BOOLEAN | default true |
| `description` | VARCHAR | optional |
| `created_at` | TIMESTAMP | auto |
| `updated_at` | TIMESTAMP | auto |

**`dq_test_results`** — execution history:
| Column | Type |
|---|---|
| `result_id` | VARCHAR PK |
| `test_id` | VARCHAR |
| `execution_id` | VARCHAR (UUID per batch run) |
| `status` | VARCHAR (`pass`, `warn`, `fail`) |
| `rows_failed` | INTEGER |
| `rows_total` | INTEGER |
| `failed_sample` | VARCHAR |
| `compiled_sql` | VARCHAR |
| `error_message` | VARCHAR |
| `execution_time_ms` | INTEGER |
| `executed_at` | TIMESTAMP |

### Test Runner (`src/dq_functions.cpp`)

`dq_run_tests()` is a table function. It:
1. Fetches enabled tests from `dq_tests` (optional filters via named parameters)
2. Generates a UUID `execution_id` for the batch
3. Calls `DQExecutor::ExecuteTest()` for each test
4. Stores results in `dq_test_results` via `DQExecutor::StoreResult()`
5. Returns results as a table with columns: `test_id`, `test_name`, `table_name`, `column_name`, `test_type`, `status`, `rows_failed`, `rows_total`, `compiled_sql`, `execution_time_ms`, `severity`, `error_message`

**Named parameters:**
- `table_name` — filter to tests for a specific table
- `tag` — filter by a tag (checks if tag is in the `tags` array)
- `test_id` — run a single test by ID (takes precedence over other filters)

### SQL Compiler (`src/dq_compiler.cpp`)

`DQCompiler::CompileTest()` converts a test definition into a SQL query that returns **failing rows** (zero rows = pass). Supported test types:

| Test Type | Required `test_params` | What It Checks |
|---|---|---|
| `unique` | none | duplicate values in column |
| `not_null` | none | NULL values in column |
| `accepted_values` | `{"values": ["a", "b"]}` | values not in the allowed list |
| `regex` | `{"pattern": "^..."}` | values not matching the regex |
| `range` | `{"min": 0, "max": 100}` | values outside the numeric range (NULLs also fail) |
| `relationship` | `{"to_table": "t", "to_column": "c"}` | FK violations (non-null values with no match) |
| `row_count` | `{"min": 1, "max": 1000}` | table row count outside the range |
| `custom_sql` | `{"sql": "SELECT * FROM {table} WHERE ..."}` | custom query; use `{table}` and `{column}` placeholders |

### Test Executor (`src/dq_executor.cpp`)

`DQExecutor::ExecuteTest()`:
1. Compiles SQL via `DQCompiler`
2. Counts total rows in the target table
3. Runs compiled SQL, counts returned (failed) rows
4. Calls `DetermineStatus()` to resolve `pass` / `warn` / `fail`

**Status determination logic:**
- Zero failed rows → `pass`
- Checks `error_if` threshold first → `fail`
- Checks `warn_if` threshold → `warn`
- Falls back to `severity` field (`error` → `fail`, else `warn`)

**Threshold format** (used in `error_if` / `warn_if`):
- Absolute count: `>0`, `>=5`, `<10`, `=0`
- Percentage: `>5%`, `>=10%` (percentage of total rows)

## User Workflow

```sql
-- 1. Initialize schema (once per database)
CALL dq_init();

-- 2. Define tests
INSERT INTO dq_tests (test_name, table_name, column_name, test_type)
VALUES ('orders_id_unique', 'orders', 'id', 'unique');

INSERT INTO dq_tests (test_name, table_name, column_name, test_type, test_params, severity)
VALUES ('orders_status_valid', 'orders', 'status', 'accepted_values',
        '{"values": ["pending", "shipped", "delivered"]}', 'warn');

-- 3. Run all tests
SELECT * FROM dq_run_tests();

-- 3a. Run tests for a specific table
SELECT * FROM dq_run_tests(table_name='orders');

-- 3b. Run a specific test
SELECT * FROM dq_run_tests(test_id='<uuid>');

-- 4. Query results history
SELECT t.test_name, r.status, r.rows_failed, r.executed_at
FROM dq_tests t
LEFT JOIN dq_test_results r ON t.test_id = r.test_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.test_name ORDER BY r.executed_at DESC NULLS LAST) = 1;
```

## Build Configuration

- **extension_config.cmake**: Defines which extensions to load (just dqtest by default)
- **CMakeLists.txt**: Extension-specific CMake configuration
  - Sets `TARGET_NAME` to `dqtest`
  - Links OpenSSL libraries (can be removed if not needed)
  - Builds both static and loadable versions
- **Makefile**: Thin wrapper that includes `extension-ci-tools/makefiles/duckdb_extension.Makefile`

## Git Submodules

This repository contains two submodules:
- `duckdb/` - The DuckDB source code (required for building)
- `extension-ci-tools/` - CI tools and reusable Makefiles from DuckDB

Update submodules with: `git submodule update --init --recursive`

## DuckDB Version

The extension is currently configured to build against DuckDB v2.0.0 (see `.github/workflows/MainDistributionPipeline.yml`). The CI workflow uses `extension-ci-tools` branch `v2.0-cyanoptera`. When updating DuckDB versions, the submodule reference, extension-ci-tools submodule, and CI workflow version must all be updated together.

## Extension Distribution

Extensions can be distributed via:
1. **Binary distribution**: Built by CI for multiple platforms
2. **Loadable extension**: Users install with `INSTALL dqtest; LOAD dqtest;`
3. **Statically linked**: Compiled directly into DuckDB binary

The CI pipeline (`.github/workflows/MainDistributionPipeline.yml`) uses DuckDB's extension-ci-tools to build and test across platforms.
