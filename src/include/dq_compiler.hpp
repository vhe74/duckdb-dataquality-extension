#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {

class DQCompiler {
public:
	static string CompileTest(const string &test_type, const string &table_name, const string &column_name,
	                          const string &test_params_json);

private:
	static string CompileUnique(const string &table_name, const string &column_name);
	static string CompileNotNull(const string &table_name, const string &column_name);
	static string CompileAcceptedValues(const string &table_name, const string &column_name,
	                                    const string &test_params_json);
	static string CompileRegex(const string &table_name, const string &column_name, const string &test_params_json);
	static string CompileRange(const string &table_name, const string &column_name, const string &test_params_json);
	static string CompileRelationship(const string &table_name, const string &column_name,
	                                  const string &test_params_json);
	static string CompileRowCount(const string &table_name, const string &test_params_json);
	static string CompileCustomSQL(const string &table_name, const string &column_name,
	                               const string &test_params_json);

	static string SubstituteVariables(const string &sql, const string &table_name, const string &column_name);
};

} // namespace duckdb
