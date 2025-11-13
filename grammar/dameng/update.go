package dameng

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/yaoapp/kun/log"
	"github.com/yaoapp/xun"
	"github.com/yaoapp/xun/dbal"
)

// Upsert Upsert new records or update the existing ones.
// 达梦数据库使用MERGE INTO语句实现UPSERT
func (grammarSQL Dameng) Upsert(query *dbal.Query, values []xun.R, uniqueBy []interface{}, updateValues interface{}) (sql.Result, error) {

	columns := values[0].Keys()
	insertValues := [][]interface{}{}
	for _, row := range values {
		insertValue := []interface{}{}
		for _, column := range columns {
			insertValue = append(insertValue, row.Get(column))
		}
		insertValues = append(insertValues, insertValue)
	}

	sql, bindings := grammarSQL.CompileUpsert(query, columns, insertValues, uniqueBy, updateValues)
	defer log.Debug(sql)
	return grammarSQL.DB.Exec(sql, bindings...)
}

// CompileUpsert Compile an upsert statement into SQL.
// 达梦数据库使用 MERGE INTO 语法（Oracle/DM 标准）
func (grammarSQL Dameng) CompileUpsert(query *dbal.Query, columns []interface{}, values [][]interface{}, uniqueBy []interface{}, updateValues interface{}) (string, []interface{}) {

	if len(values) == 0 {
		return fmt.Sprintf("insert into %s default values", grammarSQL.WrapTable(query.From)), []interface{}{}
	}

	bindings := []interface{}{}
	tableName := grammarSQL.WrapTable(query.From)

	// MERGE INTO table_name
	sql := fmt.Sprintf("MERGE INTO %s USING (", tableName)

	// 构造 USING 子句: SELECT ?, ?, ? FROM DUAL UNION ALL SELECT ?, ?, ? FROM DUAL ...
	valueClauses := []string{}
	for _, row := range values {
		placeholders := []string{}
		for range columns {
			placeholders = append(placeholders, "?")
		}
		valueClauses = append(valueClauses, fmt.Sprintf("SELECT %s FROM DUAL", strings.Join(placeholders, ", ")))
		bindings = append(bindings, row...)
	}
	sql += strings.Join(valueClauses, " UNION ALL ")

	// AS "excluded" (col1, col2, ...)
	sql += ") AS "
	sql += grammarSQL.ID("excluded")
	sql += " ("
	columnNames := []string{}
	for _, col := range columns {
		columnNames = append(columnNames, grammarSQL.Wrap(col))
	}
	sql += strings.Join(columnNames, ", ")
	sql += ") ON ("

	// ON 条件: table.key1 = excluded.key1 AND table.key2 = excluded.key2
	onClauses := []string{}
	for _, key := range uniqueBy {
		colName := grammarSQL.Wrap(key)
		onClauses = append(onClauses, fmt.Sprintf("%s.%s = %s.%s",
			tableName, colName,
			grammarSQL.ID("excluded"), colName))
	}
	sql += strings.Join(onClauses, " AND ")
	sql += ")"

	// WHEN MATCHED THEN UPDATE SET ...
	update := reflect.ValueOf(updateValues)
	kind := update.Kind()
	updateSegments := []string{}

	if kind == reflect.Array || kind == reflect.Slice {
		// updateValues 是列名数组：[“name”, “age”]
		for i := 0; i < update.Len(); i++ {
			column := fmt.Sprintf("%v", update.Index(i).Interface())
			// 跳过 uniqueBy 中的列（达梦不能更新关联条件中的列）
			if grammarSQL.isInUniqueBy(column, uniqueBy) {
				continue
			}
			colName := grammarSQL.Wrap(column)
			updateSegments = append(updateSegments, fmt.Sprintf("%s = %s.%s",
				colName, grammarSQL.ID("excluded"), colName))
		}
	} else if kind == reflect.Map {
		// updateValues 是 map: {"name": "new_value", "age": expr}
		for _, key := range update.MapKeys() {
			column := fmt.Sprintf("%v", key)
			// 跳过 uniqueBy 中的列
			if grammarSQL.isInUniqueBy(column, uniqueBy) {
				continue
			}
			value := update.MapIndex(key).Interface()
			colName := grammarSQL.Wrap(column)
			if dbal.IsExpression(value) {
				updateSegments = append(updateSegments, fmt.Sprintf("%s = %s", colName, value.(dbal.Expression).GetValue()))
			} else {
				updateSegments = append(updateSegments, fmt.Sprintf("%s = ?", colName))
				bindings = append(bindings, value)
			}
		}
	}

	if len(updateSegments) > 0 {
		sql += " WHEN MATCHED THEN UPDATE SET "
		sql += strings.Join(updateSegments, ", ")
	}

	// WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
	sql += " WHEN NOT MATCHED THEN INSERT ("
	insertColumns := []string{}
	for _, col := range columns {
		insertColumns = append(insertColumns, grammarSQL.Wrap(col))
	}
	sql += strings.Join(insertColumns, ", ")
	sql += ") VALUES ("
	insertValues := []string{}
	for _, col := range columns {
		colName := grammarSQL.Wrap(col)
		insertValues = append(insertValues, fmt.Sprintf("%s.%s", grammarSQL.ID("excluded"), colName))
	}
	sql += strings.Join(insertValues, ", ")
	sql += ")"

	return sql, bindings
}

// isInUniqueBy 检查列是否在 uniqueBy 中
func (grammarSQL Dameng) isInUniqueBy(column string, uniqueBy []interface{}) bool {
	for _, key := range uniqueBy {
		if fmt.Sprintf("%v", key) == column {
			return true
		}
	}
	return false
}

// CompileUpdate Compile an update statement into SQL.
func (grammarSQL Dameng) CompileUpdate(query *dbal.Query, values map[string]interface{}) (string, []interface{}) {

	if len(query.Joins) == 0 && query.Limit < 0 {
		return grammarSQL.SQL.CompileUpdate(query, values)
	}

	// 达梦数据库不支持UPDATE ... LIMIT，需要使用子查询
	// 类似PostgreSQL的实现方式，使用ROWID
	offset := 0
	bindings := []interface{}{}
	table := grammarSQL.WrapTable(query.From)

	columns, columnsBindings := grammarSQL.CompileUpdateColumns(query, values, &offset)
	bindings = append(bindings, columnsBindings...)

	alias := query.From.Alias
	if alias != "" {
		query.Columns = []interface{}{fmt.Sprintf("%s.rowid", alias)}
	} else {
		query.Columns = []interface{}{"rowid"}
	}

	selectSQL := grammarSQL.CompileSelectOffset(query, &offset)

	bindings = append(bindings, query.GetBindings()...)
	sql := fmt.Sprintf("update %s set %s where %s in (%s)", table, columns, grammarSQL.Wrap("rowid"), selectSQL)

	return sql, bindings
}
