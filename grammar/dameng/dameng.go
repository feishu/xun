package dameng

import (
	"fmt"

	_ "gitee.com/chunanyong/dm" // Load dameng driver
	"github.com/jmoiron/sqlx"
	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/grammar/sql"
	"github.com/yaoapp/xun/utils"
)

// Dameng the Dameng Grammar
type Dameng struct {
	sql.SQL
}

func init() {
	dbal.Register("dameng", New())
	dbal.Register("dm", New())
}

// setup the method will be executed when db server was connected
func (grammarSQL *Dameng) setup(db *sqlx.DB, config *dbal.Config, option *dbal.Option) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	if config == nil {
		return fmt.Errorf("config is nil")
	}

	grammarSQL.DB = db
	grammarSQL.Config = config
	grammarSQL.Option = option

	// Parse DSN to get database name and schema
	if config.DSN != "" {
		cfg, err := ParseDSN(config.DSN)
		if err == nil {
			// Set database name
			if cfg.DatabaseName != "" {
				grammarSQL.DatabaseName = cfg.DatabaseName
			}
			// Set schema name
			schema := cfg.Schema
			if schema == "" {
				schema = cfg.User
			}
			if schema != "" {
				grammarSQL.SchemaName = schema
			}
		}
	}

	// If DSN parsing failed or no database name, use config.Name
	if grammarSQL.DatabaseName == "" && config.Name != "" {
		grammarSQL.DatabaseName = config.Name
		// Default to using database name as schema if no schema specified
		if grammarSQL.SchemaName == "" {
			grammarSQL.SchemaName = config.Name
		}
	}

	return nil
}

// NewWith Create a new grammar interface, using the given *sqlx.DB, *dbal.Config and *dbal.Option.
func (grammarSQL Dameng) NewWith(db *sqlx.DB, config *dbal.Config, option *dbal.Option) (dbal.Grammar, error) {
	err := grammarSQL.setup(db, config, option)
	if err != nil {
		return nil, err
	}
	grammarSQL.Quoter.Bind(db, option.Prefix)
	return grammarSQL, nil
}

// NewWithRead Create a new grammar interface, using the given *sqlx.DB, *dbal.Config and *dbal.Option.
func (grammarSQL Dameng) NewWithRead(write *sqlx.DB, writeConfig *dbal.Config, read *sqlx.DB, readConfig *dbal.Config, option *dbal.Option) (dbal.Grammar, error) {
	err := grammarSQL.setup(write, writeConfig, option)
	if err != nil {
		return nil, err
	}

	grammarSQL.Read = read
	grammarSQL.ReadConfig = readConfig
	grammarSQL.Quoter.Bind(write, option.Prefix, read)
	return grammarSQL, nil
}

// New Create a new dameng grammar interface
func New(opts ...sql.Option) dbal.Grammar {
	dm := Dameng{
		SQL: sql.NewSQL(&Quoter{}, opts...),
	}
	if dm.Driver == "" || dm.Driver == "sql" {
		dm.Driver = "dameng"
	}
	dm.IndexTypes = map[string]string{
		"unique": "UNIQUE INDEX",
		"index":  "INDEX",
	}

	// 达梦数据库数据类型映射（与GORM保持一致）
	types := dm.SQL.Types
	types["string"] = "VARCHAR" // 与GORM v1/v2保持一致
	types["text"] = "CLOB"
	types["mediumText"] = "CLOB"
	types["longText"] = "CLOB"
	types["binary"] = "BLOB"
	types["dateTime"] = "TIMESTAMP"
	types["dateTimeTz"] = "TIMESTAMP WITH TIME ZONE"
	types["time"] = "TIME"
	types["timeTz"] = "TIME WITH TIME ZONE"
	types["timestamp"] = "TIMESTAMP"
	types["timestampTz"] = "TIMESTAMP WITH TIME ZONE"
	types["boolean"] = "BIT"
	types["decimal"] = "NUMBER"
	types["bigInteger"] = "BIGINT"
	types["tinyInteger"] = "TINYINT"
	types["smallInteger"] = "SMALLINT"
	types["integer"] = "INTEGER"
	types["float"] = "FLOAT"
	types["double"] = "DOUBLE PRECISION"
	types["char"] = "CHAR"
	dm.Types = types

	// set fliptypes
	flipTypes, ok := utils.MapFilp(dm.Types)
	if ok {
		dm.FlipTypes = flipTypes.(map[string]string)
		dm.FlipTypes["CLOB"] = "text"
		dm.FlipTypes["BLOB"] = "binary"
		dm.FlipTypes["NUMBER"] = "decimal"
		dm.FlipTypes["VARCHAR"] = "string"  // 与GORM保持一致
		dm.FlipTypes["VARCHAR2"] = "string" // 兼容旧数据
	}

	return dm
}

// GetOperators get the operators
func (grammarSQL Dameng) GetOperators() []string {
	return []string{
		"=", "<", ">", "<=", ">=", "<>", "!=",
		"like", "not like", "between", "not between",
		"in", "not in", "is", "is not",
	}
}
