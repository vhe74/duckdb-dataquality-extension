#include "dq_functions.hpp"
#include "dq_executor.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include <vector>
#include <chrono>

namespace duckdb {

// Global state for run_tests function
struct RunTestsGlobalState : public GlobalTableFunctionState {
	vector<DQTestResult> results;
	bool finished = false;
	idx_t current_idx = 0;

	RunTestsGlobalState() : GlobalTableFunctionState() {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Bind data to store filter parameters
struct RunTestsBindData : public FunctionData {
	string table_name_filter;
	string tag_filter;
	string test_id_filter;

	RunTestsBindData() {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<RunTestsBindData>();
		result->table_name_filter = table_name_filter;
		result->tag_filter = tag_filter;
		result->test_id_filter = test_id_filter;
		return result;
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RunTestsBindData>();
		return table_name_filter == other.table_name_filter && tag_filter == other.tag_filter &&
		       test_id_filter == other.test_id_filter;
	}
};

unique_ptr<FunctionData> RunTestsBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<RunTestsBindData>();

	// Check for named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "table_name") {
			bind_data->table_name_filter = StringValue::Get(kv.second);
		} else if (kv.first == "tag") {
			bind_data->tag_filter = StringValue::Get(kv.second);
		} else if (kv.first == "test_id") {
			bind_data->test_id_filter = StringValue::Get(kv.second);
		}
	}

	// Define return columns
	names.push_back("test_id");
	names.push_back("test_name");
	names.push_back("table_name");
	names.push_back("column_name");
	names.push_back("test_type");
	names.push_back("status");
	names.push_back("rows_failed");
	names.push_back("rows_total");
	names.push_back("compiled_sql");
	names.push_back("execution_time_ms");
	names.push_back("severity");
	names.push_back("error_message");

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);

	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> RunTestsGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RunTestsGlobalState>();
	auto &bind_data = input.bind_data->Cast<RunTestsBindData>();

	Connection con(context.db->GetDatabase(context));

	// Build query to fetch tests
	string query = "SELECT test_id, test_name, table_name, column_name, test_type, test_params, severity, warn_if, "
	               "error_if FROM dq_tests WHERE enabled = true";

	if (!bind_data.test_id_filter.empty()) {
		query += " AND test_id = '" + bind_data.test_id_filter + "'";
	} else {
		if (!bind_data.table_name_filter.empty()) {
			query += " AND table_name = '" + bind_data.table_name_filter + "'";
		}
		if (!bind_data.tag_filter.empty()) {
			query += " AND '" + bind_data.tag_filter + "' = ANY(tags)";
		}
	}

	auto result = con.Query(query);

	if (result->HasError()) {
		throw InvalidInputException("Error fetching tests: " + result->GetError());
	}

	// Generate execution ID for this batch using SQL
	auto uuid_result = con.Query("SELECT gen_random_uuid()::VARCHAR");
	string execution_id;
	if (!uuid_result->HasError()) {
		auto uuid_chunk = uuid_result->Fetch();
		if (uuid_chunk && uuid_chunk->size() > 0) {
			execution_id = uuid_chunk->GetValue(0, 0).ToString();
		}
	} else {
		// Fallback to timestamp-based ID if UUID generation fails
		execution_id = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
	}

	// Execute all tests
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		for (idx_t i = 0; i < chunk->size(); i++) {
			string test_id = chunk->GetValue(0, i).ToString();
			string test_name = chunk->GetValue(1, i).ToString();
			string table_name = chunk->GetValue(2, i).ToString();
			string column_name = chunk->GetValue(3, i).IsNull() ? "" : chunk->GetValue(3, i).ToString();
			string test_type = chunk->GetValue(4, i).ToString();
			string test_params = chunk->GetValue(5, i).IsNull() ? "{}" : chunk->GetValue(5, i).ToString();
			string severity = chunk->GetValue(6, i).ToString();
			string warn_if = chunk->GetValue(7, i).IsNull() ? "" : chunk->GetValue(7, i).ToString();
			string error_if = chunk->GetValue(8, i).IsNull() ? "" : chunk->GetValue(8, i).ToString();

			// Execute the test
			auto test_result = DQExecutor::ExecuteTest(context, test_id, test_name, table_name, column_name, test_type,
			                                           test_params, severity, warn_if, error_if);

			// Store result in database
			DQExecutor::StoreResult(context, test_result, execution_id);

			// Add to results vector
			state->results.push_back(test_result);
		}
	}

	return state;
}

void RunTestsFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<RunTestsGlobalState>();

	if (global_state.current_idx >= global_state.results.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	while (global_state.current_idx < global_state.results.size() && count < STANDARD_VECTOR_SIZE) {
		auto &result = global_state.results[global_state.current_idx];

		output.data[0].SetValue(count, Value(result.test_id));
		output.data[1].SetValue(count, Value(result.test_name));
		output.data[2].SetValue(count, Value(result.table_name));
		output.data[3].SetValue(count, result.column_name.empty() ? Value() : Value(result.column_name));
		output.data[4].SetValue(count, Value(result.test_type));
		output.data[5].SetValue(count, Value(result.status));
		output.data[6].SetValue(count, Value::BIGINT(result.rows_failed));
		output.data[7].SetValue(count, Value::BIGINT(result.rows_total));
		output.data[8].SetValue(count, Value(result.compiled_sql));
		output.data[9].SetValue(count, Value::BIGINT(result.execution_time_ms));
		output.data[10].SetValue(count, Value(result.severity));
		output.data[11].SetValue(count, result.error_message.empty() ? Value() : Value(result.error_message));

		global_state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

void RegisterDQFunctions(ExtensionLoader &loader) {
	// Register run_tests table function
	TableFunction run_tests_func("dq_run_tests", {}, RunTestsFunc, RunTestsBind, RunTestsGlobalInit);

	// Add named parameters
	run_tests_func.named_parameters["table_name"] = LogicalType::VARCHAR;
	run_tests_func.named_parameters["tag"] = LogicalType::VARCHAR;
	run_tests_func.named_parameters["test_id"] = LogicalType::VARCHAR;

	loader.RegisterFunction(run_tests_func);

	// Register run_test as an alias with test_id parameter
	TableFunction run_test_func("dq_run_test", {}, RunTestsFunc, RunTestsBind, RunTestsGlobalInit);
	run_test_func.named_parameters["test_id"] = LogicalType::VARCHAR;
	loader.RegisterFunction(run_test_func);
}

} // namespace duckdb
