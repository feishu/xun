package dameng

import (
	"fmt"
	"strings"

	"github.com/yaoapp/xun/dbal"
)

// CompileSelect Compile a select query into SQL.
func (grammarSQL Dameng) CompileSelect(query *dbal.Query) string {
	bindingOffset := 0
	return grammarSQL.CompileSelectOffset(query, &bindingOffset)
}

// CompileSelectOffset Compile a select query into SQL with binding offset.
func (grammarSQL Dameng) CompileSelectOffset(query *dbal.Query, offset *int) string {

	// SQL STMT
	if query.SQL != "" {
		return cleanBackticks(query.SQL)
	}

	if len(query.Unions) > 0 && query.Aggregate.Func != "" {
		return grammarSQL.CompileUnionAggregate(query)
	}

	sqls := map[string]string{}

	// If the query does not have any columns set, we'll set the columns to the
	// * character to just get all of the columns from the database.
	columns := query.Columns
	if len(columns) == 0 {
		query.AddColumn(grammarSQL.Raw("*"))
	}

	// To compile the query, we'll spin through each component of the query and
	// see if that component exists. If it does we'll just call the compiler
	// function for the component which is responsible for making the SQL.
	sqls["aggregate"] = grammarSQL.CompileAggregate(query, query.Aggregate)
	sqls["columns"] = grammarSQL.CompileColumns(query, query.Columns, offset)
	sqls["from"] = grammarSQL.CompileFrom(query, query.From, offset)
	sqls["joins"] = grammarSQL.CompileJoins(query, query.Joins, offset)
	sqls["wheres"] = grammarSQL.CompileWheres(query, query.Wheres, offset)
	sqls["groups"] = grammarSQL.CompileGroups(query, query.Groups, offset)
	sqls["havings"] = grammarSQL.CompileHavings(query, query.Havings, offset)
	sqls["orders"] = grammarSQL.CompileOrders(query, query.Orders, offset)
	// 达梦数据库DM8+支持标准SQL分页语法 LIMIT/OFFSET
	sqls["limit"] = grammarSQL.CompileLimit(query, query.Limit, offset)
	sqls["offset"] = grammarSQL.CompileOffset(query, query.Offset)
	sqls["lock"] = grammarSQL.CompileLock(query, query.Lock)

	sql := ""
	for _, name := range []string{"aggregate", "columns", "from", "joins", "wheres", "groups", "havings", "orders", "limit", "offset", "lock"} {
		segment, has := sqls[name]
		if has && segment != "" {
			sql = sql + segment + " "
		}
	}

	// Compile unions
	if len(query.Unions) > 0 {
		sql = fmt.Sprintf("%s %s", grammarSQL.WrapUnion(sql), grammarSQL.CompileUnions(query, query.Unions, offset))
	}

	// reset columns
	query.Columns = columns
	return strings.Trim(sql, " ")
}

// CompileColumns Compile the "select *" portion of the query.
func (grammarSQL Dameng) CompileColumns(query *dbal.Query, columns []interface{}, bindingOffset *int) string {

	// If the query is actually performing an aggregating select, we will let that
	// compiler handle the building of the select clauses.
	if query.Aggregate.Func != "" {
		return ""
	}

	sql := "select"
	// 达梦数据库不支持 DISTINCT ON (PostgreSQL特性)，只支持 DISTINCT
	if query.Distinct || len(query.DistinctColumns) > 0 {
		sql = "select distinct"
	}

	sql = fmt.Sprintf("%s %s", sql, grammarSQL.Columnize(columns))

	for _, col := range columns {
		switch col.(type) {
		case dbal.Select:
			*bindingOffset = *bindingOffset + col.(dbal.Select).Offset
		}
	}

	return sql
}

// CompileLock the lock into SQL.
// 达梦数据库支持FOR UPDATE，不支持FOR SHARE
func (grammarSQL Dameng) CompileLock(query *dbal.Query, lock interface{}) string {
	lockType, ok := lock.(string)
	if ok == false {
		return ""
	} else if lockType == "share" || lockType == "update" {
		// 达梦数据库不支持FOR SHARE，统一使用FOR UPDATE
		return "for update"
	}
	return ""
}

// SelectFromDummyTable Get the "from" value for a select with no from clause.
// 达梦数据库需要使用 DUAL 表（类似Oracle）
func (grammarSQL Dameng) SelectFromDummyTable() string {
	return "from DUAL"
}

// cleanBackticks 将 SQL 中单引号字符串字面量外部的反引号（`）安全替换为达梦标准双引号（"）
// 该函数严格限定在达梦方言编译器内部，用于保证上层生成或传入带有反引号的 Raw SQL 时，
// 能自动安全适配达梦语法，且单引号内的文本字面量保持原样不变。
func cleanBackticks(sql string) string {
	if !strings.Contains(sql, "`") {
		return sql
	}
	var sb strings.Builder
	sb.Grow(len(sql))
	inSingleQuote := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			// 处理 SQL 标准单引号转义 ''
			if inSingleQuote && i+1 < len(sql) && sql[i+1] == '\'' {
				sb.WriteByte('\'')
				sb.WriteByte('\'')
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
			sb.WriteByte('\'')
			continue
		}
		if ch == '`' && !inSingleQuote {
			sb.WriteByte('"')
		} else {
			sb.WriteByte(ch)
		}
	}
	return sb.String()
}
