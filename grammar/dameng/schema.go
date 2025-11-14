package dameng

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/yaoapp/kun/log"
	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/utils"
)

// GetVersion get the version of the connection database
func (grammarSQL Dameng) GetVersion() (*dbal.Version, error) {
	sql := "SELECT ID_CODE FROM V$VERSION"
	rows := []string{}
	err := grammarSQL.DB.Select(&rows, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) < 1 {
		return nil, fmt.Errorf("Can't get the version")
	}

	// 默认版本8.0.0
	ver, _ := semver.Make("8.0.0")

	return &dbal.Version{
		Version: ver,
		Driver:  grammarSQL.Driver,
	}, nil
}

// GetTables Get all of the table names for the database.
func (grammarSQL Dameng) GetTables() ([]string, error) {
	sql := "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = USER ORDER BY TABLE_NAME"
	defer log.Debug(sql)
	tables := []string{}
	err := grammarSQL.DB.Select(&tables, sql)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

// TableExists check if the table exists
func (grammarSQL Dameng) TableExists(name string) (bool, error) {
	sql := fmt.Sprintf(
		"SELECT COUNT(*) as cnt FROM ALL_TABLES WHERE OWNER = USER AND TABLE_NAME = %s",
		grammarSQL.VAL(strings.ToUpper(name)),
	)
	defer log.Debug(sql)
	var cnt int
	err := grammarSQL.DB.Get(&cnt, sql)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

// CreateTable create a new table on the schema
func (grammarSQL Dameng) CreateTable(table *dbal.Table, options ...dbal.CreateTableOption) error {
	name := grammarSQL.ID(table.TableName)
	sql := fmt.Sprintf("CREATE TABLE %s (\n", name)
	if len(options) > 0 {
		option := options[0]
		if option.Temporary {
			sql = fmt.Sprintf("CREATE GLOBAL TEMPORARY TABLE %s (\n", name)
		}
	}

	stmts := []string{}
	commentStmts := []string{}

	var primary *dbal.Primary = nil
	columns := []*dbal.Column{}
	indexes := []*dbal.Index{}
	cbCommands := []*dbal.Command{}

	for _, command := range table.Commands {
		switch command.Name {
		case "AddColumn":
			columns = append(columns, command.Params[0].(*dbal.Column))
			cbCommands = append(cbCommands, command)
			break
		case "CreateIndex":
			indexes = append(indexes, command.Params[0].(*dbal.Index))
			cbCommands = append(cbCommands, command)
			break
		case "CreatePrimary":
			primary = command.Params[0].(*dbal.Primary)
			cbCommands = append(cbCommands, command)
			break
		}
	}

	err := grammarSQL.createTableAddColumn(table, &stmts, &commentStmts, columns)
	if err != nil {
		return err
	}

	// Primary key
	if primary != nil {
		stmts = append(stmts, grammarSQL.SQLAddPrimary(primary))
	}
	sql = sql + strings.Join(stmts, ",\n")
	sql = sql + fmt.Sprintf("\n)")

	// Create table
	defer log.Debug(sql)
	_, err = grammarSQL.DB.Exec(sql)
	if err != nil {
		return err
	}

	// indexes
	err = grammarSQL.createTableCreateIndex(table, indexes)
	if err != nil {
		return err
	}

	// Comments
	err = grammarSQL.createTableAddComment(table, commentStmts)
	if err != nil {
		return err
	}

	// Callback
	for _, cmd := range cbCommands {
		cmd.Callback(err)
	}

	return nil
}

func (grammarSQL Dameng) createTableAddColumn(table *dbal.Table, stmts *[]string, commentStmts *[]string, columns []*dbal.Column) error {
	// Columns
	for _, column := range columns {
		*stmts = append(*stmts,
			grammarSQL.SQLAddColumn(column),
		)

		commentStmt := grammarSQL.SQLAddComment(column)
		if commentStmt != "" {
			*commentStmts = append(*commentStmts, commentStmt)
		}
	}

	return nil
}

func (grammarSQL Dameng) createTableCreateIndex(table *dbal.Table, indexes []*dbal.Index) error {
	indexStmts := []string{}

	for _, index := range indexes {
		if index.Type == "primary" {
			continue
		}
		indexStmt := grammarSQL.SQLAddIndex(index)
		if indexStmt != "" {
			indexStmts = append(indexStmts, indexStmt)
		}
	}
	if len(indexStmts) > 0 {
		sql := strings.Join(indexStmts, ";\n")
		defer log.Debug(sql)
		_, err := grammarSQL.DB.Exec(sql)
		return err
	}
	return nil
}

func (grammarSQL Dameng) createTableAddComment(table *dbal.Table, commentStmts []string) error {
	if len(commentStmts) > 0 {
		sql := strings.Join(commentStmts, ";\n")
		defer log.Debug(sql)
		_, err := grammarSQL.DB.Exec(sql)
		return err

		// for _, sql := range commentStmts {
		// 	defer log.Debug(sql)
		// 	_, err := grammarSQL.DB.Exec(sql)
		// 	if err != nil {
		// 		return err
		// 	}
		// }
	}
	return nil
}

// RenameTable rename a table on the schema.
func (grammarSQL Dameng) RenameTable(old string, new string) error {
	sql := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", grammarSQL.ID(old), grammarSQL.ID(new))
	defer log.Debug(sql)
	_, err := grammarSQL.DB.Exec(sql)
	return err
}

// DropTable drop a table on the schema.
func (grammarSQL Dameng) DropTable(name string) error {
	sql := fmt.Sprintf("DROP TABLE %s", grammarSQL.ID(name))
	defer log.Debug(sql)
	_, err := grammarSQL.DB.Exec(sql)
	return err
}

// DropTableIfExists drop a table on the schema if exists.
func (grammarSQL Dameng) DropTableIfExists(name string) error {
	exists, err := grammarSQL.TableExists(name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	return grammarSQL.DropTable(name)
}

// GetTable get a table on the schema
func (grammarSQL Dameng) GetTable(name string) (*dbal.Table, error) {
	has, err := grammarSQL.TableExists(name)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, fmt.Errorf("the table %s does not exists", name)
	}

	table := dbal.NewTable(name, grammarSQL.GetSchema(), grammarSQL.GetDatabase())
	columns, err := grammarSQL.GetColumnListing(table.SchemaName, table.TableName)
	if err != nil {
		return nil, err
	}
	indexes, err := grammarSQL.GetIndexListing(table.SchemaName, table.TableName)
	if err != nil {
		return nil, err
	}

	primaryKeyName := ""

	// attaching columns
	for _, column := range columns {
		column.Indexes = []*dbal.Index{}
		table.PushColumn(column)
	}

	// attaching indexes
	for i := range indexes {
		idx := indexes[i]
		if !table.HasColumn(idx.ColumnName) {
			return nil, fmt.Errorf("the column %s does not exists", idx.ColumnName)
		}
		column := table.ColumnMap[idx.ColumnName]
		if !table.HasIndex(idx.Name) {
			index := *idx
			index.Columns = []*dbal.Column{}
			column.Indexes = append(column.Indexes, &index)
			table.PushIndex(&index)
		}
		index := table.IndexMap[idx.Name]
		index.Columns = append(index.Columns, column)
		if index.Type == "primary" {
			primaryKeyName = idx.Name
		}
	}

	// attaching primary
	if _, has := table.IndexMap[primaryKeyName]; has {
		idx := table.IndexMap[primaryKeyName]
		table.Primary = &dbal.Primary{
			Name:      idx.Name,
			TableName: idx.TableName,
			DBName:    idx.DBName,
			Table:     idx.Table,
			Columns:   idx.Columns,
		}
		delete(table.IndexMap, idx.Name)
		for _, column := range table.Primary.Columns {
			column.Primary = true
			column.Indexes = []*dbal.Index{}
		}
	}

	return table, nil
}

// ExecSQL execute sql then update table structure
func (grammarSQL Dameng) ExecSQL(table *dbal.Table, sql string) error {
	_, err := grammarSQL.DB.Exec(sql)
	if err != nil {
		return err
	}
	// update table structure
	new, err := grammarSQL.GetTable(table.TableName)
	if err != nil {
		return err
	}

	*table = *new
	return nil
}

// AlterTable alter a table on the schema
func (grammarSQL Dameng) AlterTable(table *dbal.Table) error {

	sql := fmt.Sprintf("ALTER TABLE %s ", grammarSQL.ID(table.TableName))
	stmts := []string{}
	errs := []error{}

	for _, command := range table.Commands {
		switch command.Name {
		case "AddColumn":
			grammarSQL.alterTableAddColumn(table, command, sql, &stmts, &errs)
			break
		case "ChangeColumn":
			grammarSQL.alterTableChangeColumn(table, command, sql, &stmts, &errs)
			break
		case "RenameColumn":
			grammarSQL.alterTableRenameColumn(table, command, sql, &stmts, &errs)
			break
		case "DropColumn":
			grammarSQL.alterTableDropColumn(table, command, sql, &stmts, &errs)
			break
		case "CreateIndex":
			grammarSQL.alterTableCreateIndex(table, command, sql, &stmts, &errs)
			break
		case "RenameIndex":
			grammarSQL.alterTableRenameIndex(table, command, sql, &stmts, &errs)
			break
		case "DropIndex":
			grammarSQL.alterTableDropIndex(table, command, sql, &stmts, &errs)
			break
		case "CreatePrimary":
			grammarSQL.alterTableCreatePrimary(table, command, sql, &stmts, &errs)
			break
		case "DropPrimary":
			grammarSQL.alterTableDropPrimary(table, command, sql, &stmts, &errs)
			break
		}
	}

	defer log.Debug(strings.Join(stmts, "\n"))

	// Return Errors
	if len(errs) > 0 {
		message := ""
		for _, err := range errs {
			message = message + err.Error() + "\n"
		}
		return errors.New(message)
	}

	return nil
}

func (grammarSQL Dameng) alterTableAddColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	column := command.Params[0].(*dbal.Column)
	stmt := fmt.Sprintf("ADD %s", grammarSQL.SQLAddColumn(column))
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("AddColumn: %s", err))
	}

	// Add comment
	commentStmt := grammarSQL.SQLAddComment(column)
	if commentStmt != "" {
		err := grammarSQL.ExecSQL(table, commentStmt)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("AddComment: %s", err))
		}
	}

	command.Callback(err)
}

func (grammarSQL Dameng) alterTableChangeColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	column := command.Params[0].(*dbal.Column)
	// 达梦数据库使用MODIFY而不是ALTER COLUMN
	stmt := fmt.Sprintf("MODIFY %s", grammarSQL.SQLAddColumn(column))
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("ChangeColumn %s: %s", column.Name, err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableRenameColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	old := command.Params[0].(string)
	new := command.Params[1].(string)
	stmt := fmt.Sprintf("RENAME COLUMN %s TO %s", grammarSQL.ID(old), grammarSQL.ID(new))
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("RenameColumn: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableDropColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	name := command.Params[0].(string)
	stmt := fmt.Sprintf("DROP COLUMN %s", grammarSQL.ID(name))
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("DropColumn: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableCreateIndex(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	index := command.Params[0].(*dbal.Index)
	stmt := grammarSQL.SQLAddIndex(index)
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("CreateIndex: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableRenameIndex(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	old := command.Params[0].(string)
	new := command.Params[1].(string)
	stmt := fmt.Sprintf("ALTER INDEX %s RENAME TO %s", grammarSQL.ID(old), grammarSQL.ID(new))
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("RenameIndex: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableDropIndex(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	name := command.Params[0].(string)
	stmt := fmt.Sprintf("DROP INDEX %s", grammarSQL.ID(name))
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("DropIndex: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableCreatePrimary(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	primary := command.Params[0].(*dbal.Primary)
	columns := []string{}
	for _, column := range primary.Columns {
		columns = append(columns, grammarSQL.ID(column.Name))
	}
	stmt := fmt.Sprintf("ADD PRIMARY KEY (%s)", strings.Join(columns, ","))
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("CreatePrimary: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Dameng) alterTableDropPrimary(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	stmt := "DROP PRIMARY KEY"
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("DropPrimary: %s", err))
	}
	command.Callback(err)
}

// GetColumnListing get a table columns structure
func (grammarSQL Dameng) GetColumnListing(dbName string, tableName string) ([]*dbal.Column, error) {
	// 使用达梦数据库系统表查询列信息，包括注释
	sql := fmt.Sprintf(`
		SELECT 
			c.COLUMN_NAME, 
			c.DATA_TYPE, 
			c.DATA_LENGTH,
			c.DATA_PRECISION,
			c.DATA_SCALE,
			c.NULLABLE,
			c.DATA_DEFAULT,
			c.COLUMN_ID,
			cm.COMMENTS
		FROM ALL_TAB_COLUMNS c
		LEFT JOIN ALL_COL_COMMENTS cm ON c.OWNER = cm.OWNER 
			AND c.TABLE_NAME = cm.TABLE_NAME 
			AND c.COLUMN_NAME = cm.COLUMN_NAME
		WHERE c.OWNER = USER AND c.TABLE_NAME = %s
		ORDER BY c.COLUMN_ID
	`, grammarSQL.VAL(strings.ToUpper(tableName)))

	defer log.Debug(sql)

	rows, err := grammarSQL.DB.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []*dbal.Column{}
	for rows.Next() {
		var column dbal.Column
		var nullable, dataDefault, comment interface{}
		var dataLength, dataPrecision, dataScale interface{}
		var dataType string

		err := rows.Scan(
			&column.Name,
			&dataType,
			&dataLength,
			&dataPrecision,
			&dataScale,
			&nullable,
			&dataDefault,
			&column.Position,
			&comment,
		)
		if err != nil {
			return nil, err
		}

		column.TableName = tableName
		column.DBName = dbName
		column.Type = strings.ToUpper(dataType)
		column.TypeName = dataType
		column.Nullable = nullable == "Y"

		// 处理注释
		if comment != nil {
			commentStr := fmt.Sprintf("%v", comment)
			column.Comment = &commentStr
		}

		// 处理默认值
		if dataDefault != nil {
			defStr := fmt.Sprintf("%v", dataDefault)
			column.Default = &defStr
		}

		// 处理长度
		if dataLength != nil {
			length := int(dataLength.(int64))
			column.Length = &length
		}

		// 处理精度
		if dataPrecision != nil {
			precision := int(dataPrecision.(int64))
			column.Precision = &precision
		}

		// 处理小数位数
		if dataScale != nil {
			scale := int(dataScale.(int64))
			column.Scale = &scale
		}

		columns = append(columns, &column)
	}

	// Cast the database data type to DBAL data type
	for _, column := range columns {
		// 1. 使用FlipTypes映射达梦类型到DBAL类型
		typ, has := grammarSQL.FlipTypes[column.Type]
		if has {
			column.Type = typ
		}

		// 2. 从注释中提取类型信息（用于特殊类型如ipAddress, year等）
		if column.Comment != nil {
			typ = grammarSQL.GetTypeFromComment(column.Comment)
			if typ != "" {
				column.Type = typ
			}
		}

		// 3. 检测自增列（达梦使用IDENTITY）
		// 检查默认值中是否包含IDENTITY或者列定义中包含IDENTITY
		if column.Default != nil {
			// Convert Default to string (since it's an interface{})
			var defaultVal string
			switch v := column.Default.(type) {
			case string:
				defaultVal = strings.ToUpper(v)
			case *string:
				if v != nil {
					defaultVal = strings.ToUpper(*v)
				}
			}
			if strings.Contains(defaultVal, "IDENTITY") {
				column.Extra = utils.StringPtr("AutoIncrement")
			}
		}
	}

	return columns, nil
}

// GetIndexListing get a table indexes structure
func (grammarSQL Dameng) GetIndexListing(dbName string, tableName string) ([]*dbal.Index, error) {
	// 使用达梦数据库系统表查询索引信息，包括主键
	sql := fmt.Sprintf(`
		SELECT 
			i.INDEX_NAME,
			ic.COLUMN_NAME,
			ic.COLUMN_POSITION,
			i.UNIQUENESS,
			CASE 
				WHEN c.CONSTRAINT_TYPE = 'P' THEN 1
				ELSE 0
			END as IS_PRIMARY
		FROM ALL_INDEXES i
		JOIN ALL_IND_COLUMNS ic ON i.INDEX_NAME = ic.INDEX_NAME 
			AND i.TABLE_NAME = ic.TABLE_NAME 
			AND i.TABLE_OWNER = ic.INDEX_OWNER
		LEFT JOIN ALL_CONSTRAINTS c ON i.INDEX_NAME = c.CONSTRAINT_NAME
			AND i.TABLE_NAME = c.TABLE_NAME
			AND i.TABLE_OWNER = c.OWNER
		WHERE i.TABLE_OWNER = USER AND i.TABLE_NAME = %s
		ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION
	`, grammarSQL.VAL(strings.ToUpper(tableName)))

	defer log.Debug(sql)

	rows, err := grammarSQL.DB.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 用于返回的索引数组（每个列作为一个索引记录）
	indexes := []*dbal.Index{}

	for rows.Next() {
		var indexName, columnName, uniqueness string
		var position, isPrimary int

		err := rows.Scan(&indexName, &columnName, &position, &uniqueness, &isPrimary)
		if err != nil {
			return nil, err
		}

		// 创建索引记录（每列一条记录，用于后续关联）
		idx := &dbal.Index{
			Name:       indexName,
			TableName:  tableName,
			DBName:     dbName,
			ColumnName: columnName,
			Type:       "index",
		}

		// 判断索引类型
		if isPrimary == 1 {
			idx.Type = "primary"
			idx.Name = "PRIMARY" // 统一主键名称为PRIMARY
		} else if uniqueness == "UNIQUE" {
			idx.Type = "unique"
			idx.Unique = true
		} else {
			idx.Type = "index"
		}

		indexes = append(indexes, idx)
	}

	return indexes, nil
}
