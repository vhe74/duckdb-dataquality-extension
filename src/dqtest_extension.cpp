#define DUCKDB_EXTENSION_MAIN

#include "dqtest_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void DqtestScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Dqtest " + name.GetString() + " 🐥");
	});
}

inline void DqtestOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Dqtest " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto dqtest_scalar_function = ScalarFunction("dqtest", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DqtestScalarFun);
	loader.RegisterFunction(dqtest_scalar_function);

	// Register another scalar function
	auto dqtest_openssl_version_scalar_function = ScalarFunction("dqtest_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, DqtestOpenSSLVersionScalarFun);
	loader.RegisterFunction(dqtest_openssl_version_scalar_function);
}

void DqtestExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DqtestExtension::Name() {
	return "dqtest";
}

std::string DqtestExtension::Version() const {
#ifdef EXT_VERSION_DQTEST
	return EXT_VERSION_DQTEST;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(dqtest, loader) {
	duckdb::LoadInternal(loader);
}
}