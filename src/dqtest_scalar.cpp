#include "dqtest_scalar.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static void DqtestScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Dqtest " + name.GetString() + " 🐥");
	});
}

ScalarFunction GetDqtestScalarFunction() {
	return ScalarFunction("dqtest", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DqtestScalarFun);
}

} // namespace duckdb
