#include "dq_executor.hpp"
#include "dq_compiler.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include <chrono>

namespace duckdb {

DQTestResult DQExecutor::ExecuteTest(ClientContext &context, const string &test_id, const string &test_name,
                                     const string &table_name, const string &column_name, const string &test_type,
                                     const string &test_params_json, const string &severity, const string &warn_if,
                                     const string &error_if) {
	DQTestResult result;
	result.test_id = test_id;
	result.test_name = test_name;
	result.table_name = table_name;
	result.column_name = column_name;
	result.test_type = test_type;
	result.severity = severity;
	result.rows_failed = 0;
	result.rows_total = 0;

	auto start = std::chrono::high_resolution_clock::now();

	try {
		// Compile the test to SQL
		result.compiled_sql = DQCompiler::CompileTest(test_type, table_name, column_name, test_params_json);

		//printf("Compiled SQL for test '%s': %s\n", test_name.c_str(), result.compiled_sql.c_str());

		// Execute the query
		Connection con(context.db->GetDatabase(context));

		// First, get the total row count of the table
		auto count_query = "SELECT COUNT(*) FROM " + table_name;
		auto count_result = con.Query(count_query);
		if (count_result->HasError()) {
			result.error_message = "Error counting total rows: " + count_result->GetError();
			result.status = "fail";
			return result;
		}

		auto count_chunk = count_result->Fetch();
		if (count_chunk && count_chunk->size() > 0) {
			result.rows_total = count_chunk->GetValue(0, 0).GetValue<int64_t>();
		}

		// Execute the test query (returns failed rows)
		auto test_result = con.Query(result.compiled_sql);

		if (test_result->HasError()) {
			result.error_message = test_result->GetError();
			result.status = "fail";
		} else {
			// Count failed rows
			idx_t failed_count = 0;
			while (true) {
				auto chunk = test_result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}
				failed_count += chunk->size();
			}

			result.rows_failed = static_cast<int64_t>(failed_count);

			// Determine status based on thresholds
			result.status = DetermineStatus(result.rows_failed, result.rows_total, severity, warn_if, error_if);
		}

	} catch (std::exception &e) {
		result.error_message = string("Exception during test execution: ") + e.what();
		result.status = "fail";
	}

	auto end = std::chrono::high_resolution_clock::now();
	result.execution_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

	return result;
}

string DQExecutor::DetermineStatus(int64_t rows_failed, int64_t rows_total, const string &severity,
                                   const string &warn_if, const string &error_if) {
	if (rows_failed == 0) {
		return "pass";
	}

	// Check error_if threshold first
	if (!error_if.empty() && EvaluateThreshold(error_if, rows_failed, rows_total)) {
		return "fail";
	}

	// Check warn_if threshold
	if (!warn_if.empty() && EvaluateThreshold(warn_if, rows_failed, rows_total)) {
		return "warn";
	}

	// Use default severity
	if (severity == "error") {
		return "fail";
	} else {
		return "warn";
	}
}

bool DQExecutor::EvaluateThreshold(const string &threshold, int64_t rows_failed, int64_t rows_total) {
	if (threshold.empty()) {
		return false;
	}

	// Check if threshold is percentage-based
	bool is_percentage = threshold.find('%') != string::npos;

	// Extract operator and value
	string op;
	string value_str = threshold;

	if (threshold.substr(0, 2) == ">=") {
		op = ">=";
		value_str = threshold.substr(2);
	} else if (threshold.substr(0, 2) == "<=") {
		op = "<=";
		value_str = threshold.substr(2);
	} else if (threshold[0] == '>') {
		op = ">";
		value_str = threshold.substr(1);
	} else if (threshold[0] == '<') {
		op = "<";
		value_str = threshold.substr(1);
	} else if (threshold[0] == '=') {
		op = "=";
		value_str = threshold.substr(1);
	} else {
		// Default to >=
		op = ">=";
	}

	// Remove % if present
	if (is_percentage) {
		value_str.erase(value_str.find('%'));
	}

	// Parse the numeric value
	double threshold_value = std::stod(value_str);

	// Calculate actual value to compare
	double actual_value;
	if (is_percentage && rows_total > 0) {
		actual_value = (static_cast<double>(rows_failed) / static_cast<double>(rows_total)) * 100.0;
	} else {
		actual_value = static_cast<double>(rows_failed);
	}

	// Evaluate the condition
	if (op == ">") {
		return actual_value > threshold_value;
	} else if (op == ">=") {
		return actual_value >= threshold_value;
	} else if (op == "<") {
		return actual_value < threshold_value;
	} else if (op == "<=") {
		return actual_value <= threshold_value;
	} else if (op == "=") {
		return actual_value == threshold_value;
	}

	return false;
}

void DQExecutor::StoreResult(ClientContext &context, const DQTestResult &result, const string &execution_id) {
	Connection con(context.db->GetDatabase(context));

	string insert_sql = "INSERT INTO dq_test_results (test_id, execution_id, status, rows_failed, rows_total, "
	                    "compiled_sql, error_message, execution_time_ms) VALUES ('" +
	                    result.test_id + "', '" + execution_id + "', '" + result.status + "', " +
	                    std::to_string(result.rows_failed) + ", " + std::to_string(result.rows_total) + ", $$" +
	                    result.compiled_sql + "$$, ";

	if (result.error_message.empty()) {
		insert_sql += "NULL, ";
	} else {
		// Escape single quotes in error message
		string escaped_error = result.error_message;
		size_t pos = 0;
		while ((pos = escaped_error.find("'", pos)) != string::npos) {
			escaped_error.replace(pos, 1, "''");
			pos += 2;
		}
		insert_sql += "'" + escaped_error + "', ";
	}

	insert_sql += std::to_string(result.execution_time_ms) + ")";
	//printf("Storing result for test '%s': %s\n", result.test_name.c_str(), insert_sql.c_str());

	con.Query(insert_sql);
}

} // namespace duckdb
