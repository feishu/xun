package query

import "github.com/jmoiron/sqlx"

// DB Get the sqlx.DB pointer instance
func (builder *Builder) DB(usewrite ...bool) *sqlx.DB {
	if (len(usewrite) == 1 && usewrite[0] == true) || builder.Query.UseWriteConnection {
		return builder.Conn.Write
	}
	return builder.Conn.Read
}

// Executor Get the active execution context (either Tx if in transaction, or sqlx.DB connection)
func (builder *Builder) Executor(usewrite ...bool) sqlx.ExtContext {
	if builder.tx != nil {
		return builder.tx
	}
	return builder.DB(usewrite...)
}

// UseWrite Use the write connection for query.
func (builder *Builder) UseWrite() Query {
	builder.Query.UseWriteConnection = true
	return builder
}

// UseRead Use the read connection for query.
func (builder *Builder) UseRead() Query {
	builder.Query.UseWriteConnection = false
	return builder
}

// IsWrite Determine if the current connection is write.
func (builder *Builder) IsWrite() bool {
	return builder.Query.UseWriteConnection
}

// IsRead Determine if the read connection is read.
func (builder *Builder) IsRead() bool {
	return !builder.Query.UseWriteConnection
}
