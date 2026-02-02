#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

ScalarFunction GetDqtestOpenSSLVersionFunction();

} // namespace duckdb
