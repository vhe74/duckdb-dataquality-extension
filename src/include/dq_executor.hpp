#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {

struct DQTestResult {
	string test_id;
	string test_name;
	string table_name;
	string column_name;
	string test_type;
	string status;           // 'pass', 'warn', 'fail'
	int64_t rows_failed;
	int64_t rows_total;
	string compiled_sql;
	string error_message;
	int64_t execution_time_ms;
	string severity;
};

class DQExecutor {
public:
	static DQTestResult ExecuteTest(ClientContext &context, const string &test_id, const string &test_name,
	                                 const string &table_name, const string &column_name, const string &test_type,
	                                 const string &test_params_json, const string &severity, const string &warn_if,
	                                 const string &error_if);

	static void StoreResult(ClientContext &context, const DQTestResult &result, const string &execution_id);

private:
	static string DetermineStatus(int64_t rows_failed, int64_t rows_total, const string &severity,
	                              const string &warn_if, const string &error_if);

	static bool EvaluateThreshold(const string &threshold, int64_t rows_failed, int64_t rows_total);
};

} // namespace duckdb
