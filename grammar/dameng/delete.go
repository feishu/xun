package dameng

import (
	"fmt"

	"github.com/yaoapp/xun/dbal"
)

// CompileDelete  Compile a delete statement into SQL.
func (grammarSQL Dameng) CompileDelete(query *dbal.Query) (string, []interface{}) {

	if len(query.Joins) == 0 && query.Limit < 0 {
		return grammarSQL.SQL.CompileDelete(query)
	}

	// 达梦数据库不支持DELETE ... LIMIT，需要使用子查询
	// 类似PostgreSQL的实现方式，使用ROWID
	offset := 0
	bindings := []interface{}{}
	table := grammarSQL.WrapTable(query.From)

	alias := query.From.Alias
	if alias != "" {
		query.Columns = []interface{}{fmt.Sprintf("%s.rowid", alias)}
	} else {
		query.Columns = []interface{}{"rowid"}
	}

	selectSQL := grammarSQL.CompileSelectOffset(query, &offset)

	bindings = append(bindings, query.GetBindings()...)
	sql := fmt.Sprintf("delete from %s where %s in (%s)", table, grammarSQL.Wrap("rowid"), selectSQL)

	return sql, bindings
}

// CompileTruncate Compile a truncate table statement into SQL.
func (grammarSQL Dameng) CompileTruncate(query *dbal.Query) ([]string, [][]interface{}) {
	// 达梦数据库的TRUNCATE语法类似Oracle
	sql := fmt.Sprintf("truncate table %s", grammarSQL.WrapTable(query.From))
	return []string{sql}, [][]interface{}{{}}
}
