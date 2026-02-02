#define DUCKDB_EXTENSION_MAIN

#include "dqtest_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// Include function headers
#include "dqtest_scalar.hpp"
#include "dqtest_openssl_version.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register scalar functions
	loader.RegisterFunction(GetDqtestScalarFunction());
	loader.RegisterFunction(GetDqtestOpenSSLVersionFunction());
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