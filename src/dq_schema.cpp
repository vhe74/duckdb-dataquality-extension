#include "dq_schema.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

struct DQInitGlobalState : public GlobalTableFunctionState {
	string status_message;
	bool finished = false;

	DQInitGlobalState() : GlobalTableFunctionState() {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> DQInitBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("status");
	return_types.push_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DQInitGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<DQInitGlobalState>();

	try {
		// Create connection and execute DDL - same pattern as sqlexec
		Connection con(context.db->GetDatabase(context));

		vector<string> ddl_statements = {
		    R"(CREATE TABLE IF NOT EXISTS dq_tests (
				test_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
				test_name VARCHAR NOT NULL,
				table_name VARCHAR NOT NULL,
				column_name VARCHAR,
				test_type VARCHAR NOT NULL,
				test_params VARCHAR,
				severity VARCHAR NOT NULL DEFAULT 'error',
				error_if VARCHAR,
				warn_if VARCHAR,
				tags VARCHAR[],
				enabled BOOLEAN DEFAULT true,
				description VARCHAR,
				created_at TIMESTAMP DEFAULT now(),
				updated_at TIMESTAMP DEFAULT now()
			))",
		    R"(CREATE TABLE IF NOT EXISTS dq_test_results (
				result_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
				test_id VARCHAR NOT NULL,
				execution_id VARCHAR NOT NULL,
				status VARCHAR NOT NULL,
				rows_failed INTEGER,
				rows_total INTEGER,
				failed_sample VARCHAR,
				compiled_sql VARCHAR,
				error_message VARCHAR,
				execution_time_ms INTEGER,
				executed_at TIMESTAMP DEFAULT now()
			))",
		    "CREATE INDEX IF NOT EXISTS idx_dq_test_results_test_id ON dq_test_results(test_id)",
		    "CREATE INDEX IF NOT EXISTS idx_dq_test_results_execution_id ON dq_test_results(execution_id)",
		    "CREATE INDEX IF NOT EXISTS idx_dq_tests_table_name ON dq_tests(table_name)",
		    "CREATE INDEX IF NOT EXISTS idx_dq_tests_enabled ON dq_tests(enabled)"};

		for (auto &sql : ddl_statements) {
			auto result = con.Query(sql);
			if (result->HasError()) {
				state->status_message = "ERROR: " + result->GetError();
				return state;
			}
		}

		state->status_message = "SUCCESS: DQ tables initialized";
	} catch (std::exception &e) {
		state->status_message = string("ERROR: ") + e.what();
	}

	return state;
}

void DQInitFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<DQInitGlobalState>();

	if (global_state.finished) {
		output.SetCardinality(0);
		return;
	}

	output.SetCardinality(1);
	output.data[0].SetValue(0, Value(global_state.status_message));
	global_state.finished = true;
}

void RegisterDQSchemaFunctions(ExtensionLoader &loader) {
	TableFunction dq_init_func("dq_init", {}, DQInitFunc, DQInitBind, DQInitGlobalInit);
	loader.RegisterFunction(dq_init_func);
}

} // namespace duckdb
