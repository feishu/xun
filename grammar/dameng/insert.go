package dameng

import (
	"fmt"
	"strings"

	"github.com/yaoapp/xun/dbal"
)

// CompileInsertOrIgnore Compile an insert ignore statement into SQL.
// 达梦数据库使用 MERGE INTO 实现无冲突安全插入（WHEN NOT MATCHED THEN INSERT）
func (grammarSQL Dameng) CompileInsertOrIgnore(query *dbal.Query, columns []interface{}, values [][]interface{}) (string, []interface{}) {
	if len(values) == 0 {
		return fmt.Sprintf("insert into %s default values", grammarSQL.WrapTable(query.From)), []interface{}{}
	}

	// 查找主键或唯一标识列作为 ON 条件（默认优先使用 id）
	var uniqueCol interface{}
	for _, col := range columns {
		colStr := fmt.Sprintf("%v", col)
		if strings.EqualFold(colStr, "id") {
			uniqueCol = col
			break
		}
	}
	if uniqueCol == nil && len(columns) > 0 {
		uniqueCol = columns[0]
	}

	// 如果找到了关键列，使用 MERGE INTO 构建忽略插入
	if uniqueCol != nil {
		bindings := []interface{}{}
		tableName := grammarSQL.WrapTable(query.From)

		sql := fmt.Sprintf("MERGE INTO %s USING (", tableName)

		valueClauses := []string{}
		for i, row := range values {
			placeholders := []string{}
			for _, col := range columns {
				if i == 0 {
					placeholders = append(placeholders, fmt.Sprintf("? AS %s", grammarSQL.Wrap(col)))
				} else {
					placeholders = append(placeholders, "?")
				}
			}
			valueClauses = append(valueClauses, fmt.Sprintf("SELECT %s FROM DUAL", strings.Join(placeholders, ", ")))
			bindings = append(bindings, row...)
		}
		sql += strings.Join(valueClauses, " UNION ALL ")

		sql += ") "
		sql += grammarSQL.ID("excluded")
		sql += " ON ("

		colName := grammarSQL.Wrap(uniqueCol)
		sql += fmt.Sprintf("%s.%s = %s.%s", tableName, colName, grammarSQL.ID("excluded"), colName)
		sql += ")"

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
			cName := grammarSQL.Wrap(col)
			insertValues = append(insertValues, fmt.Sprintf("%s.%s", grammarSQL.ID("excluded"), cName))
		}
		sql += strings.Join(insertValues, ", ")
		sql += ")"

		return sql, bindings
	}

	return grammarSQL.CompileInsert(query, columns, values)
}

// CompileInsertGetID Compile an insert and get ID statement into SQL.
func (grammarSQL Dameng) CompileInsertGetID(query *dbal.Query, columns []interface{}, values [][]interface{}, sequence string) (string, []interface{}) {
	return grammarSQL.CompileInsert(query, columns, values)
}

// ProcessInsertGetID Execute an insert and get ID statement and return the id
func (grammarSQL Dameng) ProcessInsertGetID(sql string, bindings []interface{}, sequence string) (int64, error) {
	stmt, err := grammarSQL.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(bindings...)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

// SetIdentityInsert Enable IDENTITY_INSERT for a table
// 允许显式插入自增列的值（数据迁移场景）
// 使用示例:
//   grammarSQL.SetIdentityInsert("users", true)  // 开启
//   // 执行插入操作...
//   grammarSQL.SetIdentityInsert("users", false) // 关闭
func (grammarSQL Dameng) SetIdentityInsert(tableName string, enable bool) error {
	var sql string
	if enable {
		sql = fmt.Sprintf("SET IDENTITY_INSERT %s ON", grammarSQL.ID(tableName))
	} else {
		sql = fmt.Sprintf("SET IDENTITY_INSERT %s OFF", grammarSQL.ID(tableName))
	}
	_, err := grammarSQL.DB.Exec(sql)
	return err
}
