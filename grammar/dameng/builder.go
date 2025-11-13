package dameng

import (
	"fmt"
	"strings"

	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/utils"
)

// SQLAddColumn return the add column sql for table create
func (grammarSQL Dameng) SQLAddColumn(column *dbal.Column) string {
	types := grammarSQL.Types
	quoter := grammarSQL.Quoter

	// `id` bigint(20) unsigned NOT NULL,
	typ, has := types[column.Type]
	if !has {
		typ = "VARCHAR" // 默认使用VARCHAR（与GORM保持一致）
	}

	decimalTypes := []string{"DECIMAL", "FLOAT", "NUMBER", "DOUBLE"}

	if column.Precision != nil && column.Scale != nil && utils.StringHave(decimalTypes, typ) {
		typ = fmt.Sprintf("%s(%d,%d)", typ, utils.IntVal(column.Precision), utils.IntVal(column.Scale))
	} else if typ == "BLOB" || typ == "CLOB" {
		// BLOB和CLOB不需要长度，保持原样
	} else if column.Length != nil {
		typ = fmt.Sprintf("%s(%d)", typ, utils.IntVal(column.Length))
	}

	unsigned := ""
	nullable := utils.GetIF(column.Nullable, "NULL", "NOT NULL").(string)

	defaultValue := grammarSQL.GetDefaultValue(column)
	collation := utils.GetIF(utils.StringVal(column.Collation) != "", fmt.Sprintf("COLLATE %s", utils.StringVal(column.Collation)), "").(string)
	extra := ""

	// 达梦数据库的自增列使用IDENTITY语法（与GORM一致，SQL Server风格）
	if utils.StringVal(column.Extra) != "" {
		if typ == "BIGINT" {
			typ = "BIGINT IDENTITY(1,1)"
		} else if typ == "SMALLINT" {
			typ = "SMALLINT IDENTITY(1,1)"
		} else {
			typ = "INT IDENTITY(1,1)"
		}
		nullable = ""
		defaultValue = ""
	}

	if typ == "IPADDRESS" { // ipAddress
		typ = "integer"
	} else if typ == "YEAR" { // 2021 -1046 smallInt (2-byte)
		typ = "SMALLINT"
	}

	sql := fmt.Sprintf(
		"%s %s %s %s %s %s %s",
		quoter.ID(column.Name), typ, unsigned, nullable, defaultValue, extra, collation)

	sql = strings.Trim(sql, " ")
	return sql
}

// SQLAddComment return the add comment sql for table create
func (grammarSQL Dameng) SQLAddComment(column *dbal.Column) string {
	comment := utils.GetIF(
		utils.StringVal(column.Comment) != "",
		fmt.Sprintf(
			"COMMENT on column %s.%s is %s;",
			grammarSQL.ID(column.TableName),
			grammarSQL.ID(column.Name),
			grammarSQL.VAL(column.Comment),
		), "").(string)

	mappingTypes := []string{"ipAddress", "year"}
	if utils.StringHave(mappingTypes, column.Type) {
		comment = fmt.Sprintf("COMMENT on column %s.%s is %s;",
			grammarSQL.ID(column.TableName),
			grammarSQL.ID(column.Name),
			grammarSQL.VAL(fmt.Sprintf("T:%s|%s", column.Type, utils.StringVal(column.Comment))),
		)
	}
	return comment
}

// SQLAddIndex  return the add index sql for table create
func (grammarSQL Dameng) SQLAddIndex(index *dbal.Index) string {
	quoter := grammarSQL.Quoter
	indexTypes := grammarSQL.IndexTypes
	typ, has := indexTypes[index.Type]
	if !has {
		typ = "INDEX"
	}

	// UNIQUE KEY `unionid` (`unionid`) COMMENT 'xxxx'
	columns := []string{}
	for _, column := range index.Columns {
		columns = append(columns, quoter.ID(column.Name))
	}

	comment := ""
	if index.Comment != nil {
		comment = fmt.Sprintf("COMMENT %s", quoter.VAL(index.Comment))
	}
	name := quoter.ID(fmt.Sprintf("%s_%s", index.TableName, index.Name))
	sql := ""
	if typ == "PRIMARY KEY" {
		sql = fmt.Sprintf(
			"%s (%s) %s",
			typ, strings.Join(columns, ","), comment)
	} else {
		sql = fmt.Sprintf(
			"CREATE %s %s ON %s (%s)",
			typ, name, quoter.ID(index.TableName), strings.Join(columns, ","))
	}
	return sql
}

// SQLAddPrimary return the add primary key sql for table create
func (grammarSQL Dameng) SQLAddPrimary(primary *dbal.Primary) string {
	quoter := grammarSQL.Quoter

	// PRIMARY KEY `unionid` (`unionid`) COMMENT 'xxxx'
	columns := []string{}
	for _, column := range primary.Columns {
		columns = append(columns, quoter.ID(column.Name))
	}

	sql := fmt.Sprintf(
		"PRIMARY KEY (%s)",
		strings.Join(columns, ","))

	return sql
}
