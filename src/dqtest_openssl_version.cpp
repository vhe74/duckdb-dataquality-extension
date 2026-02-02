#include "dqtest_openssl_version.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static void DqtestOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Dqtest " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

ScalarFunction GetDqtestOpenSSLVersionFunction() {
	return ScalarFunction("dqtest_openssl_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      DqtestOpenSSLVersionScalarFun);
}

} // namespace duckdb
