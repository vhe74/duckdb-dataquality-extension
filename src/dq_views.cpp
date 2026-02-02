#include "dq_views.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

// View: dq_last_run_summary - Summary of the last execution
void DQLastRunSummaryFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	Connection con(context.db->GetDatabase(context));

	auto query = R"(
		SELECT
			t.test_id,
			t.test_name,
			t.table_name,
			r.status,
			r.rows_failed,
			r.rows_total,
			r.execution_time_ms,
			r.executed_at
		FROM dq_test_results r
		JOIN dq_tests t ON r.test_id = t.test_id
		WHERE r.execution_id = (
			SELECT execution_id
			FROM dq_test_results
			ORDER BY executed_at DESC
			LIMIT 1
		)
		ORDER BY r.executed_at DESC
	)";

	auto result = con.Query(query);

	if (result->HasError()) {
		output.SetCardinality(0);
		return;
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		output.SetCardinality(0);
		return;
	}

	idx_t row_count = chunk->size();
	for (idx_t i = 0; i < row_count && i < STANDARD_VECTOR_SIZE; i++) {
		for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
			output.data[col].SetValue(i, chunk->GetValue(col, i));
		}
	}

	output.SetCardinality(row_count);
}

unique_ptr<FunctionData> DQLastRunSummaryBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("test_id");
	names.push_back("test_name");
	names.push_back("table_name");
	names.push_back("status");
	names.push_back("rows_failed");
	names.push_back("rows_total");
	names.push_back("execution_time_ms");
	names.push_back("executed_at");

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::TIMESTAMP);

	return nullptr;
}

// View: dq_failing_tests - Currently failing tests
void DQFailingTestsFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	Connection con(context.db->GetDatabase(context));

	auto query = R"(
		WITH latest_results AS (
			SELECT
				test_id,
				status,
				rows_failed,
				rows_total,
				executed_at,
				ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY executed_at DESC) as rn
			FROM dq_test_results
		)
		SELECT
			t.test_id,
			t.test_name,
			t.table_name,
			t.column_name,
			t.test_type,
			lr.status,
			lr.rows_failed,
			lr.rows_total,
			lr.executed_at
		FROM dq_tests t
		JOIN latest_results lr ON t.test_id = lr.test_id
		WHERE lr.rn = 1 AND lr.status IN ('fail', 'warn')
		ORDER BY lr.executed_at DESC
	)";

	auto result = con.Query(query);

	if (result->HasError()) {
		output.SetCardinality(0);
		return;
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		output.SetCardinality(0);
		return;
	}

	idx_t row_count = chunk->size();
	for (idx_t i = 0; i < row_count && i < STANDARD_VECTOR_SIZE; i++) {
		for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
			output.data[col].SetValue(i, chunk->GetValue(col, i));
		}
	}

	output.SetCardinality(row_count);
}

unique_ptr<FunctionData> DQFailingTestsBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("test_id");
	names.push_back("test_name");
	names.push_back("table_name");
	names.push_back("column_name");
	names.push_back("test_type");
	names.push_back("status");
	names.push_back("rows_failed");
	names.push_back("rows_total");
	names.push_back("executed_at");

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::TIMESTAMP);

	return nullptr;
}

// View: dq_test_history - Aggregated history per test
void DQTestHistoryFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	Connection con(context.db->GetDatabase(context));

	auto query = R"(
		SELECT
			t.test_id,
			t.test_name,
			t.table_name,
			COUNT(*) as total_runs,
			SUM(CASE WHEN r.status = 'pass' THEN 1 ELSE 0 END) as passes,
			SUM(CASE WHEN r.status = 'warn' THEN 1 ELSE 0 END) as warns,
			SUM(CASE WHEN r.status = 'fail' THEN 1 ELSE 0 END) as fails,
			ROUND(100.0 * SUM(CASE WHEN r.status = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate,
			MAX(r.executed_at) as last_run
		FROM dq_tests t
		JOIN dq_test_results r ON t.test_id = r.test_id
		GROUP BY t.test_id, t.test_name, t.table_name
		ORDER BY success_rate ASC, total_runs DESC
	)";

	auto result = con.Query(query);

	if (result->HasError()) {
		output.SetCardinality(0);
		return;
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		output.SetCardinality(0);
		return;
	}

	idx_t row_count = chunk->size();
	for (idx_t i = 0; i < row_count && i < STANDARD_VECTOR_SIZE; i++) {
		for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
			output.data[col].SetValue(i, chunk->GetValue(col, i));
		}
	}

	output.SetCardinality(row_count);
}

unique_ptr<FunctionData> DQTestHistoryBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("test_id");
	names.push_back("test_name");
	names.push_back("table_name");
	names.push_back("total_runs");
	names.push_back("passes");
	names.push_back("warns");
	names.push_back("fails");
	names.push_back("success_rate");
	names.push_back("last_run");

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::TIMESTAMP);

	return nullptr;
}

void RegisterDQViews(ExtensionLoader &loader) {
	// Register dq_last_run_summary view
	TableFunction last_run_summary("dq_last_run_summary", {}, DQLastRunSummaryFunc, DQLastRunSummaryBind);
	loader.RegisterFunction(last_run_summary);

	// Register dq_failing_tests view
	TableFunction failing_tests("dq_failing_tests", {}, DQFailingTestsFunc, DQFailingTestsBind);
	loader.RegisterFunction(failing_tests);

	// Register dq_test_history view
	TableFunction test_history("dq_test_history", {}, DQTestHistoryFunc, DQTestHistoryBind);
	loader.RegisterFunction(test_history);
}

} // namespace duckdb
