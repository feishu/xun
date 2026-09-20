package query

import "database/sql"

// Exec Use the current connection to execute the sql, return the result
func (builder *Builder) Exec(sql string, bindings ...interface{}) (sql.Result, error) {
	return builder.DB().ExecContext(builder.Context(), sql, bindings...)
}

// ExecWrite Use the write connection to execute the sql, return the result
func (builder *Builder) ExecWrite(sql string, bindings ...interface{}) (sql.Result, error) {
	return builder.DB(true).ExecContext(builder.Context(), sql, bindings...)
}
