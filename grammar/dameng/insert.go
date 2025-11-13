package dameng

import (
	"fmt"

	"github.com/yaoapp/xun/dbal"
)

// CompileInsertOrIgnore Compile an insert ignore statement into SQL.
// 达梦数据库使用ON DUPLICATE KEY IGNORE（类似MySQL）
func (grammarSQL Dameng) CompileInsertOrIgnore(query *dbal.Query, columns []interface{}, values [][]interface{}) (string, []interface{}) {
	sql, bindings := grammarSQL.CompileInsert(query, columns, values)
	// 达梦数据库可以使用IGNORE关键字（类似MySQL）
	sql = fmt.Sprintf("insert ignore %s", sql[6:]) // 替换insert为insert ignore
	return sql, bindings
}

// CompileInsertGetID Compile an insert and get ID statement into SQL.
// 达梦数据库支持RETURNING子句（类似PostgreSQL）
func (grammarSQL Dameng) CompileInsertGetID(query *dbal.Query, columns []interface{}, values [][]interface{}, sequence string) (string, []interface{}) {
	sql, bindings := grammarSQL.CompileInsert(query, columns, values)
	sql = fmt.Sprintf("%s returning %s", sql, grammarSQL.ID(sequence))
	return sql, bindings
}

// ProcessInsertGetID Execute an insert and get ID statement and return the id
func (grammarSQL Dameng) ProcessInsertGetID(sql string, bindings []interface{}, sequence string) (int64, error) {
	var seq int64
	err := grammarSQL.DB.Get(&seq, sql, bindings...)
	if err != nil {
		return 0, err
	}
	return seq, nil
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
