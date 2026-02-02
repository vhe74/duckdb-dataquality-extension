# DuckDB Data Quality Extension

A DuckDB extension for defining, running, and tracking data quality tests directly within your database.

## Description

The Data Quality Extension (`dqtest`) enables you to implement comprehensive data quality testing in DuckDB. Define tests using SQL expressions, run them against your data, and track results over time - all without leaving your database environment.

### Key Features

- **SQL-based test definitions**: Write data quality tests using familiar SQL syntax
- **Flexible test framework**: Define expectations, assertions, and validations on any table or query
- **Test execution engine**: Run individual tests or entire test suites
- **Results tracking**: View test results, failure details, and execution history
- **Built-in reporting**: Access test summaries and identify failing tests through convenient views

### Core Functions

- `dq_init()` - Initialize the data quality testing schema
- `dq_run_tests()` - Execute all defined data quality tests
- `dq_run_test(test_name)` - Run a specific test by name
- `dq_last_run_summary()` - View summary of the most recent test run
- `dq_failing_tests()` - Query currently failing tests
- `dq_test_history()` - Access historical test execution data

### Use Cases

- Validate data integrity after ETL pipelines
- Monitor data quality in production databases
- Enforce data contracts and expectations
- Track data quality metrics over time
- Catch data anomalies and schema changes early

---

This repository is based on https://github.com/duckdb/extension-template.


## Quick Start Example

```sql
-- Initialize the data quality testing framework
CALL dq_init();

-- Create a sample table
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    status VARCHAR,
    age INTEGER
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    status VARCHAR
);

# Insert sample data
INSERT INTO customers VALUES
    (1, 'Alice', 'alice@example.com', 'active', 25),
    (2, 'Bob', NULL, 'active', 30),
    (3, 'Charlie', 'charlie@example.com', 'inactive', 35);

INSERT INTO orders VALUES
    (1, 1, 100.00, 'pending'),
    (2, 1, 200.00, 'shipped'),
    (3, 2, 150.00, 'delivered'),
    (4, 99, 50.00, 'pending');

-- Define and run data quality tests
-- (Test definition syntax depends on your implementation)
INSERT INTO dq_tests (test_name, table_name, column_name, test_type)
VALUES ('customers_id_unique', 'customers', 'id', 'unique');
INSERT INTO dq_tests (test_name, table_name, column_name, test_type)
  VALUES ('customers_email_not_null', 'customers', 'email', 'not_null');
-- more examples in test/sql/dq_test.test


-- Run all tests
CALL dq_run_tests();

-- View test results
SELECT t.test_name, r.status, r.executed_at
  FROM dq_tests t 
  LEFT JOIN dq_test_results r ON t.test_id = r.test_id;
```


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/dqtest/dqtest.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `dqtest.duckdb_extension` is the loadable binary as it would be distributed.

## Using the extension

To use the extension, start the DuckDB shell with `./build/release/duckdb`.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make testdebug
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL dq_test;
LOAD dq_test;
```

## Setting up CLion

### Opening project
Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_exentension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
