# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension named `dqtest` (Data Quality Test) built using the DuckDB extension template. DuckDB extensions are C++ modules that extend DuckDB's functionality by adding new scalar functions, aggregate functions, table functions, or other features.

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

### Extension Entry Point

The extension follows DuckDB's standard extension structure:

1. **Extension Class** (`src/include/dqtest_extension.hpp`):
   - `DqtestExtension` inherits from `duckdb::Extension`
   - Implements `Load()`, `Name()`, and `Version()` methods

2. **Load Function** (`src/dqtest_extension.cpp`):
   - `LoadInternal()` registers all functions with the `ExtensionLoader`
   - Functions are registered as `ScalarFunction`, `AggregateFunction`, or `TableFunction` objects
   - External C entry point `DUCKDB_CPP_EXTENSION_ENTRY` for loadable extension

### Adding New Functions

To add a new scalar function:

1. Define the function implementation in `src/dqtest_extension.cpp`:
```cpp
inline void MyFunctionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    // Use UnaryExecutor, BinaryExecutor, or TernaryExecutor
    // Process args.data[0], args.data[1], etc.
    // Write results to result vector
}
```

2. Register it in `LoadInternal()`:
```cpp
auto my_function = ScalarFunction("my_function",
    {LogicalType::VARCHAR},  // input types
    LogicalType::VARCHAR,    // return type
    MyFunctionScalarFun);
loader.RegisterFunction(my_function);
```

3. Add corresponding SQL tests in `test/sql/`

### Key DuckDB Concepts

- **DataChunk**: Column-oriented batch of data (vectors)
- **Vector**: Single column of data with type information
- **Executors**: `UnaryExecutor`, `BinaryExecutor`, `TernaryExecutor` for processing vectors
- **LogicalType**: DuckDB's type system (VARCHAR, INTEGER, DOUBLE, etc.)
- **StringVector::AddString()**: Add strings to result vectors (manages memory)

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

The extension is currently configured to build against DuckDB v1.4.4 (see `.github/workflows/MainDistributionPipeline.yml`). When updating DuckDB versions, the submodule reference and CI workflow version must be updated together.

## Extension Distribution

Extensions can be distributed via:
1. **Binary distribution**: Built by CI for multiple platforms
2. **Loadable extension**: Users install with `INSTALL dqtest; LOAD dqtest;`
3. **Statically linked**: Compiled directly into DuckDB binary

The CI pipeline (`.github/workflows/MainDistributionPipeline.yml`) uses DuckDB's extension-ci-tools to build and test across platforms.
