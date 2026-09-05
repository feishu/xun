package dameng

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yaoapp/xun/dbal"
)

func TestGrammarTypes(t *testing.T) {
	grammar := New()
	dm, ok := grammar.(Dameng)
	assert.True(t, ok)

	assert.Equal(t, "CLOB", dm.Types["json"])
	assert.Equal(t, "CLOB", dm.Types["jsonb"])
	assert.Equal(t, "VARCHAR", dm.Types["uuid"])
	assert.Equal(t, "VARCHAR", dm.Types["enum"])
	assert.Equal(t, "VARCHAR", dm.Types["string"])
}

func TestSQLAddColumn(t *testing.T) {
	grammar := New().(Dameng)

	// 1. 测试未指定长度的 VARCHAR 默认设为 VARCHAR(255)
	colString := &dbal.Column{
		Name:     "title",
		Type:     "string",
		Nullable: false,
	}
	sql := grammar.SQLAddColumn(colString)
	assert.Contains(t, sql, "\"title\" VARCHAR(255) NOT NULL")

	// 2. 测试 UUID 默认设为 VARCHAR(36)
	colUUID := &dbal.Column{
		Name:     "guid",
		Type:     "uuid",
		Nullable: true,
	}
	sql = grammar.SQLAddColumn(colUUID)
	assert.Contains(t, sql, "\"guid\" VARCHAR(36) NULL")

	// 3. 测试 JSON 映射为 CLOB
	colJSON := &dbal.Column{
		Name:     "payload",
		Type:     "json",
		Nullable: true,
	}
	sql = grammar.SQLAddColumn(colJSON)
	assert.Contains(t, sql, "\"payload\" CLOB NULL")

	// 4. 测试自增主键
	extra := "auto_increment"
	colID := &dbal.Column{
		Name:     "id",
		Type:     "bigInteger",
		Nullable: false,
		Extra:    &extra,
	}
	sql = grammar.SQLAddColumn(colID)
	assert.Contains(t, sql, "\"id\" BIGINT IDENTITY(1,1)")
}

func TestQuoterVAL(t *testing.T) {
	quoter := Quoter{}
	escaped := quoter.VAL("it's a test")
	// 验证单引号被转义为 ''
	assert.Equal(t, "'it''s a test'", escaped)
}

func TestCompileInsertGetID(t *testing.T) {
	grammar := New().(Dameng)
	query := &dbal.Query{
		From: dbal.From{Name: "users"},
	}
	columns := []interface{}{"name", "email"}
	values := [][]interface{}{{"alice", "alice@example.com"}}

	sql, bindings := grammar.CompileInsertGetID(query, columns, values, "id")
	assert.False(t, strings.Contains(strings.ToLower(sql), "returning"))
	assert.Equal(t, "insert into \"users\" (\"name\", \"email\") values (?,?)", sql)
	assert.Equal(t, 2, len(bindings))
}

func TestCompileInsertOrIgnore(t *testing.T) {
	grammar := New().(Dameng)
	query := &dbal.Query{
		From: dbal.From{Name: "users"},
	}
	columns := []interface{}{"id", "name"}
	values := [][]interface{}{{1, "alice"}, {2, "bob"}}

	sql, bindings := grammar.CompileInsertOrIgnore(query, columns, values)
	assert.Contains(t, sql, "MERGE INTO \"users\" USING (")
	assert.Contains(t, sql, "WHEN NOT MATCHED THEN INSERT")
	assert.False(t, strings.Contains(sql, "WHEN MATCHED THEN UPDATE"))
	assert.Equal(t, 4, len(bindings))
}

func TestQuoterID_Backticks(t *testing.T) {
	quoter := Quoter{}

	// 1. 基础反引号清洗
	assert.Equal(t, "\"id\"", quoter.ID("`id`"))
	assert.Equal(t, "\"users\"", quoter.ID("`users`"))
	assert.Equal(t, "\"users\"", quoter.ID("\"users\""))

	// 2. 带表别名的字段转义清洗
	assert.Equal(t, "\"t1\".\"id\"", quoter.Wrap("`t1`.`id`"))
	assert.Equal(t, "\"t1\".\"id\" as \"uid\"", quoter.Wrap("`t1`.`id` as `uid`"))
	assert.Equal(t, "\"users\" as \"u\"", quoter.WrapTable("`users` as `u`"))

	// 3. dbal.Name 结构体别名转义
	nameWithTable := dbal.NewName("`t1`.`id` as `my_id`")
	assert.Equal(t, "\"t1\".\"id\" as \"my_id\"", quoter.Wrap(nameWithTable))

	nameWithoutTable := dbal.NewName("`id` as `my_id`")
	assert.Equal(t, "\"id\" as \"my_id\"", quoter.Wrap(nameWithoutTable))
}

func TestQuoterParameterize_WithExpression(t *testing.T) {
	quoter := Quoter{}

	values := []interface{}{
		"18814847482",
		1,
		dbal.Raw("CURRENT_TIMESTAMP"),
		1,
	}

	result := quoter.Parameterize(values, 0)
	// 验证 Expression 直接内联，非 Expression 为 "?"
	assert.Equal(t, "?,?,CURRENT_TIMESTAMP,?", result)
}

func TestCompileComplexJoinSelect_Dameng(t *testing.T) {
	grammar := New().(Dameng)
	query := dbal.NewQuery()
	query.From = dbal.From{
		Type: "basic",
		Name: dbal.NewName("sys_user_auths as t2"),
	}
	query.Columns = []interface{}{
		"`t1`.`id`",
		"`t2`.`uuid`",
		"`t2`.`account`",
	}

	joinQuery := dbal.NewQuery()
	joinQuery.IsJoinClause = true
	joinQuery.Wheres = []dbal.Where{
		{
			Type:     "Column",
			Boolean:  "and",
			First:    "`t1`.`id`",
			Operator: "=",
			Second:   "`t2`.`uid`",
		},
	}
	query.Joins = []dbal.Join{
		{
			Type:  "inner",
			Name:  dbal.NewName("sys_users as t1"),
			Query: joinQuery,
		},
	}
	query.Wheres = []dbal.Where{
		{
			Type:     "Basic",
			Boolean:  "and",
			Column:   "`t2`.`account`",
			Operator: "=",
			Value:    "18814847482",
		},
		{
			Type:     "In",
			Boolean:  "and",
			Column:   "`t2`.`auth_type`",
			ValuesIn: []interface{}{1},
		},
		{
			Type:    "Null",
			Boolean: "and",
			Column:  "`t2`.`deleted_at`",
		},
	}

	sql := grammar.CompileSelect(query)

	// 1. 绝对不允许出现任何反引号
	assert.False(t, strings.Contains(sql, "`"), "Generated SQL must not contain backticks: %s", sql)

	// 2. 字段访问必须是标准双引号形态 "t1"."id" 而非 "`t1`"."`id`"
	assert.Contains(t, sql, "\"t1\".\"id\"")
	assert.Contains(t, sql, "\"t2\".\"uuid\"")
	assert.Contains(t, sql, "\"t2\".\"account\"")

	// 3. JOIN ON 条件
	assert.Contains(t, sql, "inner join \"sys_users\" as \"t1\" on \"t1\".\"id\" = \"t2\".\"uid\"")

	// 4. WHERE 条件
	assert.Contains(t, sql, "\"t2\".\"account\" = ?")
	assert.Contains(t, sql, "\"t2\".\"auth_type\" in (?)")
	assert.Contains(t, sql, "\"t2\".\"deleted_at\" is null")
}

func TestCompileInsertWithRawExpression_Dameng(t *testing.T) {
	grammar := New().(Dameng)
	query := dbal.NewQuery()
	query.From = dbal.From{
		Type: "basic",
		Name: dbal.NewName("sms_logs"),
	}

	// 模拟 14 个普通字段 + 1 个 CURRENT_TIMESTAMP 表达式，共 15 个字段
	columns := []interface{}{}
	row := []interface{}{}
	for i := 1; i <= 14; i++ {
		columns = append(columns, fmt.Sprintf("col_%d", i))
		row = append(row, fmt.Sprintf("val_%d", i))
	}
	columns = append(columns, "created_at")
	row = append(row, dbal.Raw("CURRENT_TIMESTAMP"))

	sql, bindings := grammar.CompileInsert(query, columns, [][]interface{}{row})

	// 1. 参数绑定数量必须与非表达式字段数（14）严格对齐
	assert.Equal(t, 14, len(bindings))

	// 2. SQL 占位符数量必须严格为 14 个 '?'
	assert.Equal(t, 14, strings.Count(sql, "?"))

	// 3. 表达式必须被内联渲染
	assert.Contains(t, sql, "CURRENT_TIMESTAMP")
}

func TestCleanBackticks(t *testing.T) {
	// 1. 无反引号原样返回
	assert.Equal(t, "SELECT * FROM \"users\"", cleanBackticks("SELECT * FROM \"users\""))

	// 2. 字段与别名反引号清洗（精准复现用户当前遇到的 SQL）
	rawSQL := "SELECT COUNT (*) AS `total` FROM `sys_users` AS `u` WHERE `name` = 'hello'"
	expected := "SELECT COUNT (*) AS \"total\" FROM \"sys_users\" AS \"u\" WHERE \"name\" = 'hello'"
	assert.Equal(t, expected, cleanBackticks(rawSQL))

	// 3. 单引号字符串内部的反引号保持不变
	literalSQL := "SELECT `id` FROM `users` WHERE `desc` = 'it`s a `test`' AND `val` = ''"
	expectedLiteral := "SELECT \"id\" FROM \"users\" WHERE \"desc\" = 'it`s a `test`' AND \"val\" = ''"
	assert.Equal(t, expectedLiteral, cleanBackticks(literalSQL))
}

func TestCompileRawSQL_Dameng(t *testing.T) {
	grammar := New().(Dameng)
	query := dbal.NewQuery()
	query.SQL = "SELECT COUNT (*) AS `total` FROM \"sys_users\" AS \"u\""

	sql := grammar.CompileSelect(query)
	assert.Equal(t, "SELECT COUNT (*) AS \"total\" FROM \"sys_users\" AS \"u\"", sql)
	assert.False(t, strings.Contains(sql, "`"))
}

