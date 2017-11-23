//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2015 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <vector>

#include <ripple/app/misc/STTx2SQL.h>
#include <ripple/app/misc/TxStore.h>

#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include <ripple/basics/base_uint.h>
#include <ripple/protocol/STArray.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/impl/json_assert.h>
#include <ripple/protocol/JsonFields.h>
#include <ripple/protocol/ErrorCodes.h>
#include <ripple/net/RPCErr.h>
#include <ripple/core/DatabaseCon.h>

#define TABLE_PREFIX    "t_"

namespace ripple {

// only hole-type, no sense
class InnerDateTime 
{
public:
	InnerDateTime() {}
	~InnerDateTime() {}
};

class InnerDecimal
{
public:
	InnerDecimal(double v)
	: value_(v) {
	}

	~InnerDecimal() {}
	const double value() {
		return value_;
	}

	const double value() const {
		return value_;
	}

private:
	InnerDecimal() {}
	double value_;
};

class FieldValue {
public:
	explicit FieldValue()
	: value_type_(INNER_UNKOWN) {};

	explicit FieldValue(const std::string& value)
	: value_type_(STRING) {
		value_.str = new std::string;
		if (value_.str) {
			value_.str->assign(value);
		}
	}

	enum {fVARCHAR, fTEXT, fBLOB, fDECIMAL};
	explicit FieldValue(const std::string& value, int flag)
	: value_type_(STRING) {

		if (flag == fVARCHAR)
			value_type_ = VARCHAR;
		else if (flag == fTEXT)
			value_type_ = TEXT;
		else if (flag == fBLOB)
			value_type_ = BLOB;

		value_.str = new std::string;
		if (value_.str) {
			value_.str->assign(value);
		}
	}

	explicit FieldValue(const int value)
	: value_type_(INT) {
		value_.i = value;
	}
	
	explicit FieldValue(const float f)
	: value_type_(FLOAT) {
		value_.f = f;
	}

	explicit FieldValue(const double d)
	: value_type_(DOUBLE) {
		value_.d = d;
	}

	explicit FieldValue(const int64_t value)
	: value_type_(LONG64) {
		value_.i64 = value;
	}

	explicit FieldValue(const InnerDateTime& datetime)
	: value_type_(DATETIME) {
		value_.datetime = nullptr;
	}

	explicit FieldValue(const InnerDecimal& decimal)
	: value_type_(DECIMAL) {
		value_.d = decimal.value();
	}

	explicit FieldValue(const FieldValue& value)
	: value_type_(value.value_type_) {
		assign(value);
	}

	FieldValue& operator =(const FieldValue& value) {
		value_type_ = value.value_type_;
		assign(value);
		return *this;
	}

	void assign(const FieldValue& value) {
		if (value_type_ == INT) {
			value_.i = value.value_.i;
		} else if (value_type_ == STRING || value_type_ == VARCHAR
			|| value_type_ == TEXT || value_type_ == BLOB) {

			value_.str = new std::string;
			if (value_.str) {
				value_.str->assign(value.value_.str->c_str());
			}
		} else if (value_type_ == DATETIME) {
			value_.datetime = value.value_.datetime;
		} else if (value_type_ == LONG64) {
			value_.i64 = value.value_.i64;
		} else if (value_type_ == FLOAT) {
			value_.f = value.value_.f;
		} else if (value_type_ == DOUBLE || value_type_ == DECIMAL) {
			value_.d = value.value_.d;
		}
	}

	FieldValue& operator =(const std::string& value) {
		value_type_ = STRING;
		value_.str = new std::string;
		if (value_.str) {
			value_.str->assign(value);
		}
		return *this;
	}

	FieldValue& operator =(const int value) {
		value_type_ = INT;
		value_.i = value;
		return *this;
	}

	FieldValue& operator =(const float value) {
		value_type_ = FLOAT;
		value_.f = value;
		return *this;
	}

	FieldValue& operator =(const double value) {
		value_type_ = DOUBLE;
		value_.d = value;
		return *this;
	}

	FieldValue& operator =(const InnerDateTime& value) {
		value_type_ = DATETIME;
		value_.datetime = nullptr;
		return *this;
	}

	FieldValue& operator =(const InnerDecimal& value) {
		value_type_ = DECIMAL;
		value_.d = value.value();
		return *this;
	}

	FieldValue& operator =(const int64_t value) {
		value_type_ = LONG64;
		value_.i64 = value;
		return *this;
	}

	~FieldValue() {
		if ((value_type_ == STRING || value_type_ == VARCHAR 
			|| value_type_ == TEXT || value_type_ == BLOB)
			&& value_.str) {
			delete value_.str;
			value_.str = nullptr;
		}
	}

	bool isNumeric() {
		return (value_type_ == INT || value_type_ == LONG64
			|| value_type_ == FLOAT || value_type_ == DOUBLE || value_type_ == DECIMAL);
	}

	bool isNumeric() const {
		return (value_type_ == INT || value_type_ == LONG64
			|| value_type_ == FLOAT || value_type_ == DOUBLE || value_type_ == DECIMAL);
	}

	bool isInt() {
		return value_type_ == INT;
	}

	bool isInt() const {
		return value_type_ == INT;
	}

	bool isFloat() {
		return value_type_ == FLOAT;
	}

	bool isFloat() const {
		return value_type_ == FLOAT;
	}

	bool isDouble() {
		return value_type_ == DOUBLE;
	}

	bool isDouble() const {
		return value_type_ == DOUBLE;
	}

	bool isDecimal() {
		return value_type_ == DECIMAL;
	}

	bool isDecimal() const {
		return value_type_ == DECIMAL;
	}

	bool isInt64() {
		return value_type_ == LONG64;
	}

	bool isInt64() const {
		return value_type_ == LONG64;
	}

	bool isString() {
		return value_type_ == STRING;
	}

	bool isString() const {
		return value_type_ == STRING;
	}

	bool isVarchar() {
		return value_type_ == VARCHAR;
	}

	bool isVarchar() const {
		return value_type_ == VARCHAR;
	}

	bool isText() {
		return value_type_ == TEXT;
	}

	bool isText() const {
		return value_type_ == TEXT;
	}

	bool isBlob() {
		return value_type_ == BLOB;
	}

	bool isBlob() const {
		return value_type_ == BLOB;
	}

	bool isDateTime() {
		return value_type_ == DATETIME;
	}

	bool isDateTime() const {
		return value_type_ == DATETIME;
	}

	const int& asInt() {
		return value_.i;
	}

	const int& asInt() const {
		return value_.i;
	}

	const int64_t& asInt64() {
		return value_.i64;
	}

	const int64_t& asInt64() const {
		return value_.i64;
	}

	const float& asFloat() {
		return value_.f;
	}

	const float& asFloat() const {
		return value_.f;
	}

	const double& asDouble() {
		return value_.d;
	}

	const double& asDouble() const {
		return value_.d;
	}

	const std::string& asString() {
		return *value_.str;
	}

	const std::string& asString() const {
		return *value_.str;
	}

private:

	enum inner_type { 
		INNER_UNKOWN,
		INT,
		FLOAT,
		DOUBLE,
		LONG64,
		DECIMAL,
		DATETIME,
		TEXT,
		VARCHAR,
		BLOB,
		STRING
	};

	int value_type_;
	union inner_value {
		int i;
		int64_t i64;
		float f;
		double d;
		InnerDateTime *datetime;
		std::string *str; // varchar/text/blob/decimal
	} value_;
};

class BuildField {

// propertiese of field
#define PK	0x00000001		// primary key
#define NN	0x00000002		// not null 
#define UQ	0x00000004		// unique
#define AI	0x00000008		// auto increase
#define ID	0x00000010		// index
#define DF	0x00000020		// has default value

public:
	explicit BuildField(const std::string& name)
	: name_(name)
	, value_()
	, length_(0)
	, flag_(0) {

	}

	explicit BuildField(const BuildField& field) 
	: name_(field.name_)
	, value_(field.value_)
	, length_(field.length_)
	, flag_(field.flag_) {

	}

	~BuildField() {
	}

	void SetFieldValue(const std::string& value, int flag) {
		value_ = FieldValue(value, flag);
	}

	void SetFieldValue(const std::string& value) {
		value_ = value;
	}

	void SetFieldValue(const int value) {
		value_ = value;
	}

	void SetFieldValue(const float value) {
		value_ = value;
	}

	void SetFieldValue(const double value) {
		value_ = value;
	}

	void SetFieldValue(const InnerDateTime& value) {
		value_ = value;
	}

	void SetFieldValue(const InnerDecimal& value) {
		value_ = value;
	}

	void SetFieldValue(const int64_t value) {
		value_ = value;
	}

	void SetLength(const int length) {
		length_ = length;
	}

	BuildField& operator =(const BuildField& field) {
		name_ = field.name_;
		value_ = field.value_;
		length_ = field.length_;
		flag_ = field.flag_;
		return *this;
	}

	const std::string& asString() {
		return value_.asString();
	}

	const std::string& asString() const {
		return value_.asString();
	}

	const int& asInt() {
		return value_.asInt();
	}

	const int& asInt() const {
		return value_.asInt();
	}

	const int64_t& asInt64() {
		return value_.asInt64();
	}

	const int64_t& asInt64() const {
		return value_.asInt64();
	}

	const float& asFloat() {
		return value_.asFloat();
	}

	const float& asFloat() const {
		return value_.asFloat();
	}

	const double& asDouble() {
		return value_.asDouble();
	}

	const double& asDouble() const {
		return value_.asDouble();
	}

	const std::string& Name() {
		return name_;
	}

	const std::string& Name() const {
		return name_;
	}

	bool isNumeric() {
		return value_.isNumeric();
	}

	bool isNumeric() const {
		return value_.isNumeric();
	}

	bool isInt() {
		return value_.isInt();
	}

	bool isInt() const {
		return value_.isInt();
	}

	bool isFloat() {
		return value_.isFloat();;
	}

	bool isFloat() const {
		return value_.isFloat();;
	}

	bool isDouble() {
		return value_.isDouble();
	}

	bool isDouble() const {
		return value_.isDouble();
	}

	bool isDecimal() {
		return value_.isDecimal();
	}

	bool isDecimal() const {
		return value_.isDecimal();
	}

	bool isInt64() {
		return value_.isInt64();
	}

	bool isInt64() const {
		return value_.isInt64();
	}

	bool isString() {
		return value_.isString();
	}

	bool isString() const {
		return value_.isString();
	}

	bool isVarchar() {
		return value_.isVarchar();
	}

	bool isVarchar() const {
		return value_.isVarchar();
	}

	bool isText() {
		return value_.isText();
	}

	bool isText() const {
		return value_.isText();
	}

	bool isBlob() {
		return value_.isBlob();
	}

	bool isBlob() const {
		return value_.isBlob();
	}

	bool isDateTime() {
		return value_.isDateTime();
	}

	bool isDateTime() const {
		return value_.isDateTime();
	}

	void SetPrimaryKey() {
		flag_ |= PK;
	}

	void SetNotNull() {
		flag_ |= NN;
	}

	void SetUnique() {
		flag_ |= UQ;
	}

	void SetAutoIncrease() {
		flag_ |= AI;
	}

	void SetIndex() {
		flag_ |= ID;
	}

	void SetDefault() {
		flag_ |= DF;
	}

	bool isPrimaryKey() {
		return (flag_ & PK) == PK;
	}

	bool isNotNull() {
		return (flag_ & NN) == NN;
	}

	bool isUnique() {
		return (flag_ & UQ) == UQ;
	}

	bool isAutoIncrease() {
		return (flag_ & AI) == AI;
	}

	bool isIndex() {
		return (flag_ & ID) == ID;
	}

	bool isDefault() {
		return (flag_ & DF) == DF;
	}

	int length() {
		return length_;
	}

private:
	std::string name_;	// field name
	FieldValue value_;	// value
	int length_;		// value holds how much bytes
	int flag_;			// PK NN UQ AI Index default
};

class BuildSQL {
public:
	typedef std::vector<BuildField> AndCondtionsType;
	typedef std::vector<AndCondtionsType> OrConditionsType;

	enum BUILDTYPE
	{
		BUILD_UNKOWN,
		BUILD_CREATETABLE_SQL = 1,
		BUILD_DROPTABLE_SQL = 2,
		BUILD_RENAMETABLE_SQL = 3,
		BUILD_ASSIGN_SQL = 4,
		BUILD_CANCEL_ASSIGN_SQL = 5,
		BUILD_INSERT_SQL = 6,
		BUILD_UPDATE_SQL = 8,
		BUILD_DELETE_SQL = 9,
		BUILD_SELECT_SQL,
		BUILD_NOSQL
	};

	explicit BuildSQL() {
	}

	virtual ~BuildSQL() {
	}

	virtual void AddTable(const std::string& tablename) = 0;
	virtual void AddField(const BuildField& field) = 0;
	virtual void AddCondition(const AndCondtionsType& condition) = 0;

	virtual const std::vector<std::string>& Tables() const = 0;
	virtual const std::vector<BuildField>&  Fields() const = 0;
	virtual const OrConditionsType& Conditions() const = 0;

	virtual std::string asString() = 0;
	virtual int execSQL() = 0;
	virtual void clear() = 0;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////
// class BuildMySQL
//////////////////////////////////////////////////////////////////////////////////////////////////////

class DisposeSQL {
public:
	DisposeSQL(BuildSQL::BUILDTYPE type, DatabaseCon* dbconn)
	: tables_()
	, fields_()
	, build_type_(type)
	, index_(0)	
	, conditions_()
	, db_conn_(dbconn) {

	}

	virtual ~DisposeSQL() {

	}

	void AddTable(const std::string& tablename) {
		tables_.push_back(tablename);
	}

	void AddField(const BuildField& field) {
		fields_.push_back(field);
	}

	void AddCondition(const BuildSQL::AndCondtionsType& condition) {
		conditions_.push_back(condition);
	}

	const std::vector<std::string>& Tables() const {
		return tables_;
	}

	const std::vector<BuildField>&  Fields() const {
		return fields_;
	}

	const BuildSQL::OrConditionsType& Conditions() const {
		return conditions_;
	}

	std::string asString() {
		std::string sql;
		switch (build_type_)
		{
		case BuildSQL::BUILD_CREATETABLE_SQL:
			sql = build_createtable_sql();
			break;
		case BuildSQL::BUILD_DROPTABLE_SQL:
			sql = build_droptable_sql();
			break;
		case BuildSQL::BUILD_RENAMETABLE_SQL:
			sql = build_renametable_sql();
			break;
		case BuildSQL::BUILD_INSERT_SQL:
			sql = build_insert_sql();
			break;
		case BuildSQL::BUILD_UPDATE_SQL:
			sql = build_update_sql();
			break;
		case BuildSQL::BUILD_DELETE_SQL:
			sql = build_delete_sql();
			break;
		case BuildSQL::BUILD_SELECT_SQL:
			sql = build_select_sql();
			break;
		default:
			break;
		}
		return sql;
	}

	int execSQL() {
		int ret = -1;

		if (db_conn_ == nullptr)
			return ret;

		switch (build_type_)
		{
		case BuildSQL::BUILD_CREATETABLE_SQL:
			ret = execute_createtable_sql();
			break;
		case BuildSQL::BUILD_DROPTABLE_SQL:
			ret = execute_droptable_sql();
			break;
		case BuildSQL::BUILD_RENAMETABLE_SQL:
			ret = execute_renametable_sql();
			break;
		case BuildSQL::BUILD_INSERT_SQL:
			ret = execute_insert_sql();
			break;
		case BuildSQL::BUILD_UPDATE_SQL:
			ret = execute_update_sql();
			break;
		case BuildSQL::BUILD_DELETE_SQL:
			ret = execute_delete_sql();
			break;
		case BuildSQL::BUILD_SELECT_SQL:
			break;
		default:
			break;
		}
		return ret;
	}

	void clear() {
		tables_.clear();
		fields_.clear();
		conditions_.clear();
	}

protected:
	DisposeSQL() {};

	virtual std::string build_createtable_sql() = 0;
	virtual int execute_createtable_sql() = 0;

	std::vector<std::string> tables_;
	std::vector<BuildField> fields_;

private:

	std::string build_droptable_sql() {
		std::string sql;
		if (tables_.size() == 0)
			return sql;
		sql = (boost::format("drop table if exists %s") % tables_[0]).str();
		return sql;
	}

	int execute_droptable_sql() {
		if (tables_.size() == 0)
			return -1;
        LockedSociSession sql = db_conn_->checkoutDb();
		*sql << build_droptable_sql();
		return 0;
	}

	std::string build_renametable_sql() {
		std::string sql;
		if (tables_.size() == 0)
			return sql;
		sql = (boost::format("rename table %s to %s") 
			% tables_[0] 
			% tables_[1]).str();
		return sql;
	}

	int execute_renametable_sql() {
		if (tables_.size() == 0)
			return -1;

        LockedSociSession sql = db_conn_->checkoutDb();
		*sql << "rename table :old to :new", soci::use(tables_[0]), soci::use(tables_[1]);
		return 0;
	}

	std::string build_insert_sql() {
		std::string sql;
		if (tables_.size() == 0)
			return sql;
		if (fields_.size() == 0)
			return sql;

		std::string& tablename = tables_[0];
		std::string fields_str;
		std::string values_str;
		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];
			fields_str += field.Name();
			if(field.isString() || field.isVarchar() 
				|| field.isBlob() || field.isText())
				values_str += (boost::format("\"%1%\"") % field.asString()).str();
			else if (field.isInt())
				values_str += (boost::format("%d") % field.asInt()).str();
			else if (field.isFloat())
				values_str += (boost::format("%f") % field.asFloat()).str();
			else if (field.isDouble() || field.isDecimal())
				values_str += (boost::format("%f") % field.asDouble()).str();
			else if(field.isInt64() || field.isDateTime())
				values_str += (boost::format("%1%") % field.asInt64()).str();

			if (idx != fields_.size() - 1) {
				fields_str += ",";
				values_str += ",";
			}
		}
		sql = (boost::format("insert into %s (%s) values (%s)")
			%tablename
			%fields_str
			%values_str).str();
		return sql;
	}

	int execute_insert_sql() {
		std::string sql_str;
		if (tables_.size() == 0)
			return -1;
		if (fields_.size() == 0)
			return -1;

		std::string& tablename = tables_[0];
		std::string fields_str;
		std::string values_str;
		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];
			fields_str += field.Name();
			values_str += ":" + field.Name();
			if (idx != fields_.size() - 1) {
				fields_str += ",";
				values_str += ",";
			}
		}
		sql_str = (boost::format("insert into %s (%s) values (%s)")
			% tablename
			%fields_str
			%values_str).str();

        LockedSociSession sql = db_conn_->checkoutDb();
        auto tmp = *sql << sql_str;
		soci::details::once_temp_type& t = tmp;
		bind_fields_value(t);
		return 0;
	}

	std::string build_update_sql() {
		std::string sql;
		if (tables_.size() == 0)
			return sql;

		if (fields_.size() == 0)
			return sql;

		std::string& tablename = tables_[0];
		std::string update_fields;

		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];
			if(field.isString() || field.isVarchar()
				|| field.isBlob() || field.isText()) {
				update_fields += (boost::format("%1%=\"%2%\"")
					%field.Name()
					%field.asString()).str();
			} else if (field.isInt()) {
				update_fields += (boost::format("%1%=%2%")
					%field.Name()
					%field.asInt()).str();
			} else if (field.isFloat()) { 
				update_fields += (boost::format("%s=%f")
					%field.Name()
					%field.asFloat()).str();
			} else if (field.isDouble() || field.isDecimal()) {
				update_fields += (boost::format("%s=%f")
					%field.Name()
					%field.asDouble()).str();
			} else if (field.isDateTime() || field.isInt64()) {
				update_fields += (boost::format("%1%=%2%")
					%field.Name()
					%field.asInt64()).str();
			}

			if (idx != fields_.size() - 1)
				update_fields += ",";
		}

		std::string conditions = build_conditions();
		if (conditions.empty()) {
			sql = (boost::format("update %s set %s")
				%tablename
				%update_fields).str();
		} else {
			sql = (boost::format("update %s set %s where %s")
				%tablename
				%update_fields
				%conditions).str();
		}

		return sql;
	}

	int execute_update_sql() {
		std::string sql_str;
		if (tables_.size() == 0)
			return -1;

		if (fields_.size() == 0)
			return -1;

		std::string& tablename = tables_[0];
		std::string update_fields;
		
		index_ = 0;
		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];
			update_fields += field.Name() + std::string("=:") + std::to_string(++index_);
			if (idx != fields_.size() - 1)
				update_fields += ",";
		}

		std::string conditions = build_execute_conditions();
		if (conditions.empty()) {
			sql_str = (boost::format("update %s set %s")
				%tablename
				%update_fields).str();
		} else {
			sql_str = (boost::format("update %s set %s where %s")
				%tablename
				%update_fields
				%conditions).str();
		}

        LockedSociSession sql = db_conn_->checkoutDb();
		auto tmp = *sql << sql_str;
		auto& st = tmp;
		bind_fields_value(st);
		if(conditions.empty() == false)
			bind_execute_conditions_value(st);
		return 0;
	}

	std::string build_delete_sql() {
		std::string sql;
		if (tables_.size() == 0)
			return sql;

		std::string& tablename = tables_[0];
		std::string conditions = build_conditions();
		if (conditions.empty()) {
			sql = (boost::format("delete from %s")
				%tablename).str();
		} else {
			sql = (boost::format("delete from %s where %s")
				%tablename
				%conditions).str();
		}
		return sql;
	}

	int execute_delete_sql() {
		std::string sql_str;
		if (tables_.size() == 0)
			return -1;

		std::string& tablename = tables_[0];
		index_ = 0;
		std::string conditions = build_execute_conditions();
		if (conditions.empty()) {
			sql_str = (boost::format("delete from %s")
				%tablename).str();
		} else {
			sql_str = (boost::format("delete from %s where %s")
				%tablename
				%conditions).str();
		}
		
        LockedSociSession sql = db_conn_->checkoutDb();
		auto tmp = *sql << sql_str;
		auto& st = tmp;
		if (conditions.empty() == false)
			bind_execute_conditions_value(st);

		return 0;
	}

	std::string build_select_sql() {
		std::string sql;
		if (tables_.size() == 0)
			return sql;
		std::string& tablename = tables_[0];
		std::string fields;
		if (fields_.size() == 0)
			fields = " * ";
		else {
			for (size_t idx = 0; idx < fields_.size(); idx++) {
				BuildField& field = fields_[idx];
				fields += field.Name();
				if (idx != fields_.size() - 1)
					fields += ",";
			}
		}

		std::string conditions = build_conditions();
		if (conditions.empty()) {
			sql = (boost::format("select %s from %s")
				%fields
				%tablename).str();
		} else {
			sql = (boost::format("select %s from %s where %s")
				%fields
				%tablename
				%conditions).str();
		}
		
		return sql;
	}

	int execute_select_sql() {
		std::string sql_str;
		if (tables_.size() == 0)
			return -1;
		std::string& tablename = tables_[0];
		std::string fields;
		if (fields_.size() == 0)
			fields = " * ";
		else {
			for (size_t idx = 0; idx < fields_.size(); idx++) {
				BuildField& field = fields_[idx];
				fields += field.Name();
				if (idx != fields_.size() - 1)
					fields += ",";
			}
		}

		index_ = 0;
		std::string conditions = build_execute_conditions();
		if (conditions.empty()) {
			sql_str = (boost::format("select %s from %s")
				%fields
				%tablename).str();
		} else {
			sql_str = (boost::format("select %s from %s where %s")
				%fields
				%tablename
				%conditions).str();
		}

        LockedSociSession sql = db_conn_->checkoutDb();
		auto tmp = *sql <<sql_str;
		auto& t = tmp;
		if (conditions.empty() == false)
			bind_execute_conditions_value(t);

		return 0;
	}

	std::string build_conditions() {
		std::string conditions;
		for(size_t idx = 0; idx < conditions_.size(); idx++) {
			BuildSQL::AndCondtionsType& and_conditions = conditions_[idx];
			for (size_t i = 0; i < and_conditions.size(); i++) {
				BuildField& field = and_conditions[i];

				if (i == 0)
					conditions += "(";

				if (field.isString() || field.isVarchar()
					|| field.isBlob() || field.isText()) {
					conditions += (boost::format("%s=\"%s\"")
						%field.Name()
						%field.asString()).str();
				} else if (field.isInt()) {
					conditions += (boost::format("%s=%d")
						%field.Name()
						%field.asInt()).str();
				} else if (field.isFloat()) {
					conditions += (boost::format("%s=%f")
						%field.Name()
						%field.asFloat()).str();
				} else if(field.isDouble() || field.isDecimal()) {
					conditions += (boost::format("%s=%f")
						%field.Name()
						%field.asDouble()).str();
				} else if (field.isInt64() || field.isDateTime()) {
					conditions += (boost::format("%1%=%2%")
						%field.Name()
						%field.asInt64()).str();
				}

				if (i != and_conditions.size() - 1) {
					conditions += " and ";
				} else {
					conditions += ")";
				}
			}

			if (idx != conditions_.size() - 1)
				conditions += " or ";
		}

		return conditions;
	}

	std::string build_execute_conditions() {
		std::string conditions;
		for (size_t idx = 0; idx < conditions_.size(); idx++) {
			BuildSQL::AndCondtionsType& and_conditions = conditions_[idx];
			for (size_t i = 0; i < and_conditions.size(); i++) {
				BuildField& field = and_conditions[i];

				if (i == 0)
					conditions += "(";
				conditions += field.Name() + std::string("=:") + std::to_string(++index_);
				if (i != and_conditions.size() - 1) {
					conditions += " and ";
				}
				else {
					conditions += ")";
				}
			}

			if (idx != conditions_.size() - 1)
				conditions += " or ";
		}

		return conditions;
	}

	void bind_execute_conditions_value(soci::details::once_temp_type& t) {
		for (size_t idx = 0; idx < conditions_.size(); idx++) {
			BuildSQL::AndCondtionsType& and_conditions = conditions_[idx];
			for (size_t i = 0; i < and_conditions.size(); i++) {
				BuildField& field = and_conditions[i];
				if (field.isString() || field.isVarchar()
					|| field.isBlob() || field.isText())
					t = t, soci::use(field.asString());
				else if (field.isInt())
					t = t, soci::use(field.asInt());
				else if (field.isFloat())
					t = t, soci::use(static_cast<double>(field.asFloat()));
				else if (field.isDouble() || field.isDecimal())
					t = t, soci::use(field.asDouble());
				else if (field.isInt64() || field.isDateTime())
					t = t, soci::use(field.asInt64());
			}
		}
	}

	void bind_fields_value(soci::details::once_temp_type& t) {
		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];
			if (field.isString() || field.isVarchar()
				|| field.isBlob() || field.isText())
				t = t, soci::use(field.asString());
			else if (field.isInt())
				t = t, soci::use(field.asInt());
			else if (field.isFloat())
				t = t, soci::use(static_cast<double>(field.asFloat()));
			else if (field.isDouble() || field.isDecimal())
				t = t, soci::use(field.asDouble());
			else if (field.isInt64() || field.isDateTime())
				t = t, soci::use(field.asInt64());
		}
	}

	int build_type_;
	int index_;
	BuildSQL::OrConditionsType conditions_;
	DatabaseCon* db_conn_;
};

class DisposeMySQL : public DisposeSQL {
public:
	DisposeMySQL(BuildSQL::BUILDTYPE type, DatabaseCon* dbconn)
		: DisposeSQL(type, dbconn)
		, db_conn_(dbconn) {

	}

	~DisposeMySQL() {

	}

protected:

	std::string build_createtable_sql() override {
		std::string sql;
		if (tables_.size() == 0)
			return sql;
		if (fields_.size() == 0)
			return sql;

		std::string& tablename = tables_[0];
		std::vector<std::string> fields;
		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];

			fields.push_back(field.Name());
			int length = field.length();
			if (field.isString() || field.isVarchar()) {
				std::string str;
				if (length > 0)
					str = (boost::format("VARCHAR(%d)") % length).str();
				else
					str = "VARCHAR";
				fields.push_back(str);
			} else if (field.isText()) {
				std::string str;
				if (length > 0)
					str = (boost::format("TEXT(%d)") % length).str();
				else
					str = "TEXT";
				fields.push_back(str);
			} else if (field.isBlob()) {
				std::string str = "BLOB";
				fields.push_back(str);
			} else if (field.isInt()) {
				std::string str;
				if (length > 0)
					str = (boost::format("INT(%d)") % length).str();
				else
					str = "INT";
				fields.push_back(str);
			} else if (field.isFloat()) {
				std::string str = "FLOAT";
				fields.push_back(str);
			} else if (field.isDouble()) {
				std::string str = "DOUBLE";
				fields.push_back(str);
			} else if (field.isDecimal()) {
				std::string str = "DECIMAL";
				if (length > 0)
					str = (boost::format("DECIMAL(%d)") % length).str();
				fields.push_back(str);
			} else if (field.isDateTime()) {
				fields.push_back(std::string("datetime"));
			}

			if (field.isPrimaryKey())
				fields.push_back(std::string("PRIMARY KEY"));
			if (field.isNotNull())
				fields.push_back(std::string("NOT NULL"));
			if (field.isUnique())
				fields.push_back(std::string("UNIQUE"));
			if (field.isAutoIncrease())
				fields.push_back(std::string("AUTO_INCREMENT"));
			if (field.isIndex())
				fields.push_back(std::string("INDEX"));
			if (field.isDefault()) {
				std::string str;
				if (field.isNumeric()) {
					str = (boost::format("DEFAULT %d") % field.asInt()).str();
				}
				else if (field.isString()) {
					std::string default_value = field.asString();
					if (default_value.empty()
						|| boost::iequals(default_value, "null")
						|| boost::iequals(default_value, "nil"))
						str = "DEFAULT NULL";
					else
						str = (boost::format("DEFAULT \"%s\"") % default_value).str();
				}
				fields.push_back(str);
			}

			if (idx != fields_.size() - 1)
				fields.push_back(std::string(","));
		}

		if (fields.size()) {
			std::string columns;
			for (size_t idx = 0; idx < fields.size(); idx++) {
				std::string& element = fields[idx];
				columns += element;
				columns += std::string(" ");
			}

			sql = (boost::format("CREATE TABLE if not exists %s (%s)")
				% tablename
				% columns).str();
		}
		return sql;
	}

	int execute_createtable_sql() override {
		std::string sql_str = build_createtable_sql();
		if (sql_str.empty())
			return -1;
        LockedSociSession sql = db_conn_->checkoutDb();
		*sql << sql_str;
		return 0;
	}

private:
	DisposeMySQL()
		: DisposeSQL() {
	}

	DatabaseCon* db_conn_;
};

class DisposeSqlite : public DisposeSQL {
public:
	DisposeSqlite(BuildSQL::BUILDTYPE type, DatabaseCon* dbconn)
		: DisposeSQL(type, dbconn)
		, db_conn_(dbconn) {

	}

	~DisposeSqlite() {

	}

protected:

	std::string build_createtable_sql() override {
		std::string sql;
		if (tables_.size() == 0)
			return sql;
		if (fields_.size() == 0)
			return sql;

		std::string& tablename = tables_[0];
		std::vector<std::string> fields;
		for (size_t idx = 0; idx < fields_.size(); idx++) {
			BuildField& field = fields_[idx];

			fields.push_back(field.Name());
			//int length = field.length();
			if (field.isString() || field.isVarchar() || field.isText()) {
				std::string str = "TEXT";
				fields.push_back(str);
			} else if (field.isBlob()) {
				std::string str = "BLOB";
				fields.push_back(str);
			} else if (field.isInt()) {
				std::string str = "INTEGER";
				fields.push_back(str);
			} else if (field.isFloat() || field.isDouble() || field.isDecimal()) {
				std::string str = "REAL";
				fields.push_back(str);
			} else if (field.isDateTime()) {
				std::string str = "NUMERIC";
				fields.push_back(str);
			}

			if (field.isPrimaryKey())
				fields.push_back(std::string("PRIMARY KEY"));
			if (field.isNotNull())
				fields.push_back(std::string("NOT NULL"));
			if (field.isUnique())
				fields.push_back(std::string("UNIQUE"));
			if (field.isAutoIncrease())
				fields.push_back(std::string("AUTOINCREMENT"));
			//if (field.isIndex())
			//	fields.push_back(std::string("INDEX"));
			if (field.isDefault()) {
				std::string str;
				if (field.isNumeric()) {
					str = (boost::format("DEFAULT %d") % field.asInt()).str();
				}
				else if (field.isString()) {
					std::string default_value = field.asString();
					if (default_value.empty()
						|| boost::iequals(default_value, "null")
						|| boost::iequals(default_value, "nil"))
						str = "DEFAULT NULL";
					else
						str = (boost::format("DEFAULT \"%s\"") % default_value).str();
				}
				fields.push_back(str);
			}

			if (idx != fields_.size() - 1)
				fields.push_back(std::string(","));
		}

		if (fields.size()) {
			std::string columns;
			for (size_t idx = 0; idx < fields.size(); idx++) {
				std::string& element = fields[idx];
				columns += element;
				columns += std::string(" ");
			}

			sql = (boost::format("CREATE TABLE if not exists %s (%s)")
				% tablename
				% columns).str();
		}
		return sql;
	}

	int execute_createtable_sql() override {
		std::string sql_str = build_createtable_sql();
		if (sql_str.empty())
			return -1;
        LockedSociSession sql = db_conn_->checkoutDb();
		*sql << sql_str;
		return 0;
	}

private:
	DisposeSqlite()
		: DisposeSQL() {
	}
	DatabaseCon* db_conn_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////
// class BuildMySQL
//////////////////////////////////////////////////////////////////////////////////////////////////////

static std::vector<std::string>		EMPTY_TABLES;
static std::vector<BuildField>		EMPTY_FIELDS;
static BuildSQL::OrConditionsType	EMPYT_CONDITIONS;

class BuildMySQL : public BuildSQL {
public:
	explicit BuildMySQL(BuildSQL::BUILDTYPE type, DatabaseCon* dbconn) 
	: BuildSQL()
	, disposesql_(std::make_shared<DisposeMySQL>(type, dbconn)) {
	}

	~BuildMySQL() {

	}

	void AddTable(const std::string& tablename) override {
		if (disposesql_)
			disposesql_->AddTable(tablename);
	}

	void AddField(const BuildField& field) override {
		if (disposesql_)
			disposesql_->AddField(field);
	}

	void AddCondition(const AndCondtionsType& condition) override {
		if (disposesql_)
			disposesql_->AddCondition(condition);
	}

	const std::vector<std::string>& Tables() const override {
		if (disposesql_)
			return disposesql_->Tables();

		return EMPTY_TABLES;
	}

	const std::vector<BuildField>&  Fields() const override {
		if (disposesql_)
			return disposesql_->Fields();

		return EMPTY_FIELDS;
	}

	const OrConditionsType& Conditions() const override {
		if (disposesql_)
			return disposesql_->Conditions();

		return EMPYT_CONDITIONS;
	}

	std::string asString() override {
		std::string sql;
		if (disposesql_)
			sql = disposesql_->asString();
		return sql;
	}

	int execSQL() override {
		int ret = -1;
		if (disposesql_)
			ret = disposesql_->execSQL();
		return ret;
	}

	void clear() override {
		if (disposesql_)
			disposesql_->clear();
	}
 
private:
	explicit BuildMySQL() {};
	std::shared_ptr<DisposeMySQL> disposesql_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////
// class BuildSqlite
//////////////////////////////////////////////////////////////////////////////////////////////////////

class BuildSqlite : public BuildSQL {
public:
	BuildSqlite(BuildSQL::BUILDTYPE type, DatabaseCon* dbconn)
	: BuildSQL()
	, disposesql_(std::make_shared<DisposeSqlite>(type, dbconn)) {

	}

	~BuildSqlite() {

	}

	void AddTable(const std::string& tablename) override {
		if (disposesql_)
			disposesql_->AddTable(tablename);
	}

	void AddField(const BuildField& field) override {
		if (disposesql_)
			disposesql_->AddField(field);
	}

	void AddCondition(const AndCondtionsType& condition) override {
		if (disposesql_)
			disposesql_->AddCondition(condition);
	}

	const std::vector<std::string>& Tables() const override {
		if (disposesql_)
			return disposesql_->Tables();

		return EMPTY_TABLES;
	}

	const std::vector<BuildField>&  Fields() const override {
		if (disposesql_)
			return disposesql_->Fields();

		return EMPTY_FIELDS;
	}

	const OrConditionsType& Conditions() const override {
		if (disposesql_)
			return disposesql_->Conditions();

		return EMPYT_CONDITIONS;
	}

	std::string asString() override {
		std::string sql;
		if (disposesql_)
			sql = disposesql_->asString();
		return sql;
	}

	int execSQL() override {
		int ret = -1;
		if (disposesql_)
			ret = disposesql_->execSQL();
		return ret;
	}

	void clear() override {
		if (disposesql_)
			disposesql_->clear();
	}


private:
	explicit BuildSqlite() {};
	std::shared_ptr<DisposeSqlite> disposesql_;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////
// class STTx2SQL
//////////////////////////////////////////////////////////////////////////////////////////////////////

STTx2SQL::STTx2SQL(const std::string& db_type)
: db_type_(db_type)
, db_conn_(nullptr) {

}

STTx2SQL::STTx2SQL(const std::string& db_type, DatabaseCon* dbconn)
: db_type_(db_type)
, db_conn_(dbconn) {

}

STTx2SQL::~STTx2SQL() {
}

int STTx2SQL::GenerateCreateTableSql(const Json::Value& Raw, BuildSQL *buildsql) {
	int ret = -1;
	if (Raw.isArray()) {
		for (Json::UInt index = 0; index < Raw.size(); index++) {
			Json::Value v = Raw[index];
			// both field and type are requirment 
			if (v.isMember("field") == false && v.isMember("type") == false)
				return ret;
			
			std::string fieldname = v["field"].asString();
			std::string type = v["type"].asString();
			BuildField buildfield(fieldname);
			// set default value when create table
			if (boost::iequals(type, "int"))
				buildfield.SetFieldValue(0);
			else if (boost::iequals(type, "float"))
				buildfield.SetFieldValue(0.0f);
			else if (boost::iequals(type, "double"))
				buildfield.SetFieldValue((double)0.0f);
			else if(boost::iequals(type, "text"))
				buildfield.SetFieldValue("", FieldValue::fTEXT);
			else if (boost::iequals(type, "varchar"))
				buildfield.SetFieldValue("", FieldValue::fVARCHAR);
			else if (boost::iequals(type, "blob"))
				buildfield.SetFieldValue("", FieldValue::fBLOB);
			else if (boost::iequals(type, "datetime"))
				buildfield.SetFieldValue(InnerDateTime());
			else if (boost::iequals(type, "decimal"))
				buildfield.SetFieldValue(InnerDecimal(0.0));

			if (v.isMember("PK"))
				buildfield.SetPrimaryKey();
			if (v.isMember("index"))
				buildfield.SetIndex();
			if (v.isMember("NN"))
				buildfield.SetNotNull();
			if (v.isMember("AI"))
				buildfield.SetAutoIncrease();
			if (v.isMember("UQ"))
				buildfield.SetUnique();
			if (v.isMember("default")) {
				buildfield.SetDefault();
				if (v["default"].isString())
					buildfield.SetFieldValue(v["default"].asString());
				else if (v["default"].isNumeric())
					buildfield.SetFieldValue(v["default"].asInt());
			}
			if (v.isMember("length"))
				buildfield.SetLength(v["length"].asInt());

			buildsql->AddField(buildfield);
		}
		ret = 0;
	}

	return ret;
}

int STTx2SQL::GenerateInsertSql(const Json::Value& raw, BuildSQL *buildsql) {
	std::vector<std::string> members = raw.getMemberNames();
	// retrieve members in object
	for (size_t i = 0; i < members.size(); i++) {
		std::string field_name = members[i];

		BuildField insert_field(field_name);

		if (raw[field_name].isString()) {
			std::string value = raw[field_name].asString();
			insert_field.SetFieldValue(value);
		} else if (raw[field_name].isInt() || raw[field_name].isIntegral()) {
			int value = raw[field_name].asInt();
			insert_field.SetFieldValue(value);
		} else if(raw[field_name].isUInt()) {
			int value =  static_cast<int>(raw[field_name].asUInt());
			insert_field.SetFieldValue(value);
		} else if (raw[field_name].isDouble()) {
			double value = raw[field_name].asDouble();
			insert_field.SetFieldValue(value);
		}

		buildsql->AddField(insert_field);
	}
	return 0;

}

int STTx2SQL::GenerateUpdateSql(const Json::Value& raw, BuildSQL *buildsql) {

	BuildSQL::AndCondtionsType and_conditions;
	// parse record
	for (Json::UInt idx = 0; idx < raw.size(); idx++) {
		auto& v = raw[idx];
		if (v.isObject() == false) {
			//JSON_ASSERT(v.isObject());
			return -1;
		}
		std::vector<std::string> members = v.getMemberNames();

		for (size_t i = 0; i < members.size(); i++) {
			std::string field_name = members[i];
			BuildField field(field_name);

			if (v[field_name].isString()) {
				std::string value = v[field_name].asString();
				field.SetFieldValue(value);
			} else if (v[field_name].isInt() || v[field_name].isIntegral()) {
				int value = v[field_name].asInt();
				field.SetFieldValue(value);
			} else if (v[field_name].isUInt()) {
				int value = static_cast<int>(v[field_name].asUInt());
				field.SetFieldValue(value);
			} else if (v[field_name].isDouble()) {
				double value = v[field_name].asDouble();
				field.SetFieldValue(value);
			}

			if (idx == 0) // update field
				buildsql->AddField(field);
			else
				and_conditions.push_back(field);
		}

		if (idx != 0) {
			buildsql->AddCondition(and_conditions);
			and_conditions.clear();
		}
	}

	return 0;
}

int STTx2SQL::GenerateDeleteSql(const Json::Value& raw, BuildSQL *buildsql) {
	BuildSQL::AndCondtionsType and_conditions;
	// parse record
	for (Json::UInt idx = 0; idx < raw.size(); idx++) {
		auto& v = raw[idx];
		if (v.isObject() == false) {
			return -1;
		}

		std::vector<std::string> members = v.getMemberNames();
		for (size_t i = 0; i < members.size(); i++) {
			std::string field_name = members[i];
			BuildField field(field_name);
			if (v[field_name].isString()) {
				std::string value = v[field_name].asString();
				field.SetFieldValue(value);
			} else if (v[field_name].isInt() || v[field_name].isIntegral()) {
				int value = v[field_name].asInt();
				field.SetFieldValue(value);
			} else if (v[field_name].isUInt()) {
				int value = static_cast<int>(v[field_name].asUInt());
				field.SetFieldValue(value);
			} else if (v[field_name].isDouble()) {
				double value = v[field_name].asDouble();
				field.SetFieldValue(value);
			} 
			and_conditions.push_back(field);
		}
		buildsql->AddCondition(and_conditions);
		and_conditions.clear();
	}
	return 0;
}

int STTx2SQL::GenerateSelectSql(const Json::Value& raw, BuildSQL *buildsql) {
	BuildSQL::AndCondtionsType and_conditions;
	// parse record
	for (Json::UInt idx = 0; idx < raw.size(); idx++) {
		auto& v = raw[idx];
		if(idx == 0) {
			if (v.isArray()) {
				for (Json::UInt i = 0; i < v.size(); i++) {
					std::string field_name = v[i].asString();
					BuildField field(field_name);
					buildsql->AddField(field);
				}
			}
		} else {
			if (v.isObject() == false)
				return -1;

			std::vector<std::string> members = v.getMemberNames();

			for (size_t i = 0; i < members.size(); i++) {
				std::string field_name = members[i];
				BuildField field(field_name);

				if (v[field_name].isString()) {
					std::string value = v[field_name].asString();
					field.SetFieldValue(value);
				}
				else if (v[field_name].isNumeric()) {
					int value = v[field_name].asInt();
					field.SetFieldValue(value);
				}
				and_conditions.push_back(field);
			}

			if (idx != 0) {
				buildsql->AddCondition(and_conditions);
				and_conditions.clear();
			}
		}
	}
	return 0;
}

std::pair<int /*retcode*/, std::string /*sql*/> STTx2SQL::ExecuteSQL(const ripple::STTx& tx) {
	std::pair<int, std::string> ret = { -1, "inner error" };
	if (tx.getTxnType() != ttTABLELISTSET && tx.getTxnType() != ttSQLSTATEMENT) {
		ret = { -1, "Transaction's type is error." };
		return ret;
	}

	uint16_t optype = tx.getFieldU16(sfOpType);
	const ripple::STArray& tables = tx.getFieldArray(sfTables);
	ripple::uint160 hex_tablename = tables[0].getFieldH160(sfNameInDB);
	//ripple::uint160 hex_tablename = tx.getFieldH160(sfNameInDB);
	std::string tn = ripple::to_string(hex_tablename);
	if (tn.empty()) {
		ret = { -1, "Tablename is empty." };
		return ret;
	}

	std::string txt_tablename = std::string(TABLE_PREFIX) + tn;

	ripple::Blob raw;
	Json::Value raw_json;
	if (tx.isFieldPresent(sfRaw))
		raw = tx.getFieldVL(sfRaw);
	if (raw.size()) {
		Json::Reader().parse(std::string(raw.begin(), raw.end()), raw_json);
		if (raw_json.isArray() == false) {
			ret = { -1, "Raw data is malformal." };
			return ret;
		}
	}
	else if (optype != 2) {	// delete sql hasn't raw
		ret = { -1, "Raw data is empty except delete-sql." };
		return ret;
	}

	BuildSQL::BUILDTYPE build_type = BuildSQL::BUILD_UNKOWN;
	switch (optype)
	{
	case 1:
		build_type = BuildSQL::BUILD_CREATETABLE_SQL;
		break;
	case 2:
		build_type = BuildSQL::BUILD_DROPTABLE_SQL;
		break;
	case 3:
		build_type = BuildSQL::BUILD_RENAMETABLE_SQL;	// ignore handle
		break;
	case 4:
		build_type = BuildSQL::BUILD_ASSIGN_SQL;		// ignore handle
		break;
	case 5:
		build_type = BuildSQL::BUILD_CANCEL_ASSIGN_SQL;	// ignore handle
		break;
	case 6:
		build_type = BuildSQL::BUILD_INSERT_SQL;
		break;
	case 8:
		build_type = BuildSQL::BUILD_UPDATE_SQL;
		break;
	case 9:
		build_type = BuildSQL::BUILD_DELETE_SQL;
		break;
	default:
		break;
	}

	std::shared_ptr<BuildSQL> buildsql = nullptr;
	if (boost::iequals(db_type_, "mysql")) {
		buildsql = std::make_shared<BuildMySQL>(build_type, db_conn_);
	}
	else if (boost::iequals(db_type_, "sqlite")) {
		buildsql = std::make_shared<BuildSqlite>(build_type, db_conn_);
	}

	if (buildsql == nullptr) {
		ret = { -1, "Resource may be exhausted." };
		return ret;
	}
	buildsql->AddTable(txt_tablename);
   
	if (build_type == BuildSQL::BUILD_INSERT_SQL) {
		std::string sql;
        bool bHasAutoField = false;
        std::string sAutoFillField;
        if (tx.isFieldPresent(sfAutoFillField))
        {
            auto blob = tx.getFieldVL(sfAutoFillField);;
            sAutoFillField.assign(blob.begin(), blob.end());
            auto sql_str = (boost::format("select * from information_schema.columns WHERE table_name ='%s'AND column_name ='%s'")
                % txt_tablename
                % sAutoFillField).str();
            LockedSociSession sql = db_conn_->checkoutDb();
            soci::rowset<soci::row> records = ((*sql).prepare << sql_str);
            bHasAutoField = records.end() != records.begin();
        }

		for (Json::UInt idx = 0; idx < raw_json.size(); idx++) {
			auto& v = raw_json[idx];
			if (v.isObject() == false) {
				//JSON_ASSERT(v.isObject());
				ret = { -1, "Element of raw may be malformal." };
				return ret;
			}

			if(GenerateInsertSql(v, buildsql.get()) != 0) {
				ret = { -1, "Insert-sql was generated unssuccessfully,transaction may be malformal." };
				return ret;
			}
            if (bHasAutoField)
            {
                BuildField insert_field(sAutoFillField);
                insert_field.SetFieldValue(to_string(tx.getTransactionID()));
                buildsql->AddField(insert_field);
            }
			
			sql += buildsql->asString();
			if(buildsql->execSQL() != 0) {
				ret = { -1, std::string("Executing SQL was failure.") + sql };
				return ret;
			}

			buildsql->clear();
			buildsql->AddTable(txt_tablename);

			if (idx != raw_json.size() - 1)
				sql += ";";
		}

		return{0, sql};
	}

	int result = -1;
	switch (build_type)
	{
	case BuildSQL::BUILD_CREATETABLE_SQL:
		result = GenerateCreateTableSql(raw_json, buildsql.get());
		break;
	case BuildSQL::BUILD_DROPTABLE_SQL:
		result = 0;	// only has tablename
		break;
	case BuildSQL::BUILD_RENAMETABLE_SQL:
		break;
	case BuildSQL::BUILD_ASSIGN_SQL:
		break;
	case BuildSQL::BUILD_CANCEL_ASSIGN_SQL:
		break;
	case BuildSQL::BUILD_UPDATE_SQL:
		result = GenerateUpdateSql(raw_json, buildsql.get());
		break;
	case BuildSQL::BUILD_DELETE_SQL:
		result = GenerateDeleteSql(raw_json, buildsql.get());
		break;
	default:
		break;
	}

	if (result == 0 && buildsql->execSQL() == 0) {
		ret = { 0, buildsql->asString() };
	} else {
		ret = { -1, buildsql->asString() };
	}

	return ret;
}

///////////////////////////////////////////////////////////////////////////////////
// TxStore::TxHistory
///////////////////////////////////////////////////////////////////////////////////

int ParseTxJson(const Json::Value& tx_json, BuildSQL &buildsql) {
	int ret = -1;
	do {
		Json::Value obj_tables = tx_json["Tables"];
		if (obj_tables.isArray() == false)
			break;

		for (Json::UInt idx = 0; idx < obj_tables.size(); idx++) {
			const Json::Value& e = obj_tables[idx];
			if (e.isObject() == false)
				break;

			Json::Value v = e["Table"];
			if (v.isObject() == false)
				break;

			Json::Value tn = v["TableName"];
			if (tn.isString() == false)
				break;
			buildsql.AddTable(std::string(TABLE_PREFIX) + tn.asString());
		}

		Json::Value raw = tx_json["Raw"];
		if (raw.isString() == false)
			break;

		Json::Value obj_raw;
		if (Json::Reader().parse(raw.asString(), obj_raw) == false)
			break;

		if (obj_raw.isArray() == false)
			break;

		for (Json::UInt idx = 0; idx < obj_raw.size(); idx++) {
			const Json::Value& v = obj_raw[idx];
			if (idx == 0) {
				// query field
				if (v.isArray() == false)
					break;

				for (Json::UInt i = 0; i < v.size(); i++) {
					const Json::Value& fieldname = v[i];
					if (fieldname.isString() == false)
						break;
					BuildField field(fieldname.asString());
					buildsql.AddField(field);
				}
			} else {
				// query conditions
				BuildSQL::AndCondtionsType and_conditions;
				if (v.isObject() == false)
					return -1;
				std::vector<std::string> members = v.getMemberNames();

				for (size_t i = 0; i < members.size(); i++) {
					std::string field_name = members[i];
					BuildField field(field_name);

					if (v[field_name].isString()) {
						std::string value = v[field_name].asString();
						field.SetFieldValue(value);
					}
					else if (v[field_name].isInt() || v[field_name].isIntegral()) {
						int value = v[field_name].asInt();
						field.SetFieldValue(value);
					}
					else if (v[field_name].isDouble()) {
						double value = v[field_name].asDouble();
						field.SetFieldValue(value);
					}
					and_conditions.push_back(field);
				}

				if (idx != 0) {
					buildsql.AddCondition(and_conditions);
				}
			}
		}

		ret = 0;
	} while (0);
	return ret;
}

Json::Value TxStore::txHistory(RPC::Context& context) {
	Json::Value obj;
	Json::Value results;
	Json::Value lines;
	if(databasecon_ == nullptr)
		return rpcError(rpcINTERNAL);

    Json::Value& tx_json = context.params[jss::tx_json];

	std::shared_ptr<BuildSQL> buildsql = nullptr;
	if (boost::iequals(db_type_, "mysql"))
		buildsql = std::make_shared<BuildMySQL>(BuildSQL::BUILD_SELECT_SQL, databasecon_);
	else if (boost::iequals(db_type_, "sqlite"))
		buildsql = std::make_shared<BuildSqlite>(BuildSQL::BUILD_SELECT_SQL, databasecon_);

    if (buildsql == nullptr)
    {
        obj[jss::error] = "there is no DB in this node";
        return obj;
    }

	if (ParseTxJson(tx_json, *buildsql) != 0)
		return rpcError(rpcNO_PERMISSION);
	
	try {
		std::string sql = buildsql->asString();
        LockedSociSession query = databasecon_->checkoutDb();
		soci::rowset<soci::row> records = ((*query).prepare << sql);

		soci::rowset<soci::row>::const_iterator r = records.begin();
		for (; r != records.end(); r++) {
			Json::Value e;
			for (size_t i = 0; i < r->size(); i++) {
				if (r->get_properties(i).get_data_type() == soci::dt_string 
					|| r->get_properties(i).get_data_type() == soci::dt_blob) {
                    if (r->get_indicator(i) == soci::i_ok)
                        e[r->get_properties(i).get_name()] = r->get<std::string>(i);
                    else
                        e[r->get_properties(i).get_name()] = "null";
				}
				else if (r->get_properties(i).get_data_type() == soci::dt_integer) {
                    if (r->get_indicator(i) == soci::i_ok)
					    e[r->get_properties(i).get_name()] = r->get<int>(i);
                    else
                        e[r->get_properties(i).get_name()] = 0;
				}
				else if (r->get_properties(i).get_data_type() == soci::dt_double) {
                    if (r->get_indicator(i) == soci::i_ok)
                        e[r->get_properties(i).get_name()] = r->get<double>(i);
                    else
                        e[r->get_properties(i).get_name()] = 0.0;
				}
				else if (r->get_properties(i).get_data_type() == soci::dt_long_long
					|| r->get_properties(i).get_data_type() == soci::dt_unsigned_long_long) {
                    if (r->get_indicator(i) == soci::i_ok)
                        e[r->get_properties(i).get_name()] = static_cast<int>(r->get<long>(i));
                    else
                        e[r->get_properties(i).get_name()] = 0;
				}
				else if (r->get_properties(i).get_data_type() == soci::dt_date) {
					std::tm tm = { 0 };
					std::string datetime = "NULL";
					if(r->get_indicator(i) == soci::i_ok) {
						tm = r->get<std::tm>(i);
						 datetime = (boost::format("%d/%d/%d %d:%d:%d")
							% (tm.tm_year + 1900) % (tm.tm_mon + 1) % tm.tm_mday
							%tm.tm_hour % (tm.tm_min + 1) % tm.tm_sec).str();
					}
					
					e[r->get_properties(i).get_name()] = datetime;
				}
			}
			lines.append(e);
		}

		results["lines"] = lines;
		obj[jss::result] = results;
		obj[jss::status] = "success";

	} catch(soci::soci_error& e) {
        obj[jss::error] = e.what();
	}
	return obj;
}

}	// namespace ripple
