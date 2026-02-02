#include "dq_compiler.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string DQCompiler::CompileTest(const string &test_type, const string &table_name, const string &column_name,
                               const string &test_params_json) {
	if (test_type == "unique") {
		return CompileUnique(table_name, column_name);
	} else if (test_type == "not_null") {
		return CompileNotNull(table_name, column_name);
	} else if (test_type == "accepted_values") {
		return CompileAcceptedValues(table_name, column_name, test_params_json);
	} else if (test_type == "regex") {
		return CompileRegex(table_name, column_name, test_params_json);
	} else if (test_type == "range") {
		return CompileRange(table_name, column_name, test_params_json);
	} else if (test_type == "relationship") {
		return CompileRelationship(table_name, column_name, test_params_json);
	} else if (test_type == "row_count") {
		return CompileRowCount(table_name, test_params_json);
	} else if (test_type == "custom_sql") {
		return CompileCustomSQL(table_name, column_name, test_params_json);
	} else {
		throw InvalidInputException("Unknown test type: " + test_type);
	}
}

string DQCompiler::CompileUnique(const string &table_name, const string &column_name) {
	return "SELECT " + column_name + ", COUNT(*) AS cnt FROM " + table_name + " GROUP BY " + column_name +
	       " HAVING COUNT(*) > 1";
}

string DQCompiler::CompileNotNull(const string &table_name, const string &column_name) {
	return "SELECT * FROM " + table_name + " WHERE " + column_name + " IS NULL";
}

string DQCompiler::CompileAcceptedValues(const string &table_name, const string &column_name,
                                         const string &test_params_json) {
	// Parse JSON to extract values array
	// For now, simple implementation - in production, use proper JSON parsing
	// Expected format: {"values": ["a", "b", "c"]}

	// Simple extraction (this should be replaced with proper JSON parsing)
	auto values_start = test_params_json.find("[");
	auto values_end = test_params_json.find("]");

	if (values_start == string::npos || values_end == string::npos) {
		throw InvalidInputException("Invalid test_params for accepted_values: must contain 'values' array");
	}

	string values_list = test_params_json.substr(values_start + 1, values_end - values_start - 1);

	return "SELECT * FROM " + table_name + " WHERE " + column_name + " NOT IN (" + values_list + ") OR " + column_name +
	       " IS NULL";
}

string DQCompiler::CompileRegex(const string &table_name, const string &column_name, const string &test_params_json) {
	// Extract pattern from JSON
	// Expected format: {"pattern": "^[A-Z]{2}[0-9]+$"}
	auto pattern_start = test_params_json.find("\"pattern\"");
	if (pattern_start == string::npos) {
		throw InvalidInputException("Invalid test_params for regex: must contain 'pattern'");
	}

	auto colon_pos = test_params_json.find(":", pattern_start);
	auto value_start = test_params_json.find("\"", colon_pos + 1);
	auto value_end = test_params_json.find("\"", value_start + 1);

	string pattern = test_params_json.substr(value_start + 1, value_end - value_start - 1);

	return "SELECT * FROM " + table_name + " WHERE NOT regexp_matches(" + column_name + ", '" + pattern + "')";
}

string DQCompiler::CompileRange(const string &table_name, const string &column_name, const string &test_params_json) {
	// Extract min and max from JSON
	// Expected format: {"min": 0, "max": 100}

	// Simple extraction (should use proper JSON parsing)
	auto min_start = test_params_json.find("\"min\"");
	auto max_start = test_params_json.find("\"max\"");

	string min_val = "NULL";
	string max_val = "NULL";

	if (min_start != string::npos) {
		auto colon = test_params_json.find(":", min_start);
		auto comma_or_end = test_params_json.find_first_of(",}", colon);
		min_val = test_params_json.substr(colon + 1, comma_or_end - colon - 1);
		// Trim whitespace
		min_val.erase(0, min_val.find_first_not_of(" \t\n\r"));
		min_val.erase(min_val.find_last_not_of(" \t\n\r") + 1);
	}

	if (max_start != string::npos) {
		auto colon = test_params_json.find(":", max_start);
		auto comma_or_end = test_params_json.find_first_of(",}", colon);
		max_val = test_params_json.substr(colon + 1, comma_or_end - colon - 1);
		// Trim whitespace
		max_val.erase(0, max_val.find_first_not_of(" \t\n\r"));
		max_val.erase(max_val.find_last_not_of(" \t\n\r") + 1);
	}

	string conditions;
	if (min_val != "NULL" && min_val != "null") {
		conditions = column_name + " < " + min_val;
	}
	if (max_val != "NULL" && max_val != "null") {
		if (!conditions.empty()) {
			conditions += " OR ";
		}
		conditions += column_name + " > " + max_val;
	}
	if (!conditions.empty()) {
		conditions += " OR ";
	}
	conditions += column_name + " IS NULL";

	return "SELECT * FROM " + table_name + " WHERE " + conditions;
}

string DQCompiler::CompileRelationship(const string &table_name, const string &column_name,
                                       const string &test_params_json) {
	// Extract to_table and to_column from JSON
	// Expected format: {"to_table": "customers", "to_column": "id"}

	auto to_table_start = test_params_json.find("\"to_table\"");
	auto to_column_start = test_params_json.find("\"to_column\"");

	if (to_table_start == string::npos || to_column_start == string::npos) {
		throw InvalidInputException("Invalid test_params for relationship: must contain 'to_table' and 'to_column'");
	}

	// Extract to_table value
	auto tt_colon = test_params_json.find(":", to_table_start);
	auto tt_value_start = test_params_json.find("\"", tt_colon + 1);
	auto tt_value_end = test_params_json.find("\"", tt_value_start + 1);
	string to_table = test_params_json.substr(tt_value_start + 1, tt_value_end - tt_value_start - 1);

	// Extract to_column value
	auto tc_colon = test_params_json.find(":", to_column_start);
	auto tc_value_start = test_params_json.find("\"", tc_colon + 1);
	auto tc_value_end = test_params_json.find("\"", tc_value_start + 1);
	string to_column = test_params_json.substr(tc_value_start + 1, tc_value_end - tc_value_start - 1);

	return "SELECT t.* FROM " + table_name + " t WHERE t." + column_name +
	       " IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + to_table + " r WHERE r." + to_column + " = t." +
	       column_name + ")";
}

string DQCompiler::CompileRowCount(const string &table_name, const string &test_params_json) {
	// Extract min and max from JSON
	auto min_start = test_params_json.find("\"min\"");
	auto max_start = test_params_json.find("\"max\"");

	string min_val = "0";
	string max_val = "NULL";

	if (min_start != string::npos) {
		auto colon = test_params_json.find(":", min_start);
		auto comma_or_end = test_params_json.find_first_of(",}", colon);
		min_val = test_params_json.substr(colon + 1, comma_or_end - colon - 1);
		min_val.erase(0, min_val.find_first_not_of(" \t\n\r"));
		min_val.erase(min_val.find_last_not_of(" \t\n\r") + 1);
	}

	if (max_start != string::npos) {
		auto colon = test_params_json.find(":", max_start);
		auto comma_or_end = test_params_json.find_first_of(",}", colon);
		max_val = test_params_json.substr(colon + 1, comma_or_end - colon - 1);
		max_val.erase(0, max_val.find_first_not_of(" \t\n\r"));
		max_val.erase(max_val.find_last_not_of(" \t\n\r") + 1);
	}

	string conditions;
	if (min_val != "NULL" && min_val != "null") {
		conditions = "COUNT(*) < " + min_val;
	}
	if (max_val != "NULL" && max_val != "null") {
		if (!conditions.empty()) {
			conditions += " OR ";
		}
		conditions += "COUNT(*) > " + max_val;
	}

	return "SELECT CASE WHEN " + conditions + " THEN 1 ELSE 0 END AS fails FROM " + table_name;
}

string DQCompiler::CompileCustomSQL(const string &table_name, const string &column_name,
                                    const string &test_params_json) {
	// Extract SQL from JSON
	// Expected format: {"sql": "SELECT * FROM {table} WHERE ..."}

	auto sql_start = test_params_json.find("\"sql\"");
	if (sql_start == string::npos) {
		throw InvalidInputException("Invalid test_params for custom_sql: must contain 'sql'");
	}

	auto colon_pos = test_params_json.find(":", sql_start);
	auto value_start = test_params_json.find("\"", colon_pos + 1);
	auto value_end = test_params_json.rfind("\"");

	string sql = test_params_json.substr(value_start + 1, value_end - value_start - 1);

	return SubstituteVariables(sql, table_name, column_name);
}

string DQCompiler::SubstituteVariables(const string &sql, const string &table_name, const string &column_name) {
	string result = sql;

	// Replace {table}
	size_t pos = 0;
	while ((pos = result.find("{table}", pos)) != string::npos) {
		result.replace(pos, 7, table_name);
		pos += table_name.length();
	}

	// Replace {column}
	pos = 0;
	while ((pos = result.find("{column}", pos)) != string::npos) {
		result.replace(pos, 8, column_name);
		pos += column_name.length();
	}

	// Note: {schema} substitution would require additional context

	return result;
}

} // namespace duckdb
