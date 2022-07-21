package sq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
)

var bufpool = &sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

// Dialects supported.
const (
	DialectSQLite    = "sqlite"
	DialectPostgres  = "postgres"
	DialectMySQL     = "mysql"
	DialectSQLServer = "sqlserver"
)

// SQLWriter is anything that can be converted to SQL.
type SQLWriter interface {
	// WriteSQL writes the SQL representation of the SQLWriter into the query
	// string (*bytes.Buffer) and args slice (*[]any).
	//
	// The params map is used to hold the mappings between named parameters in
	// the query to the corresponding index in the args slice and is used for
	// rebinding args by their parameter name. The params map may be nil, check
	// first before writing to it.
	WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error
}

// DB is a database/sql abstraction that can query the database. *sql.Conn,
// *sql.DB and *sql.Tx all implement DB.
type DB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// Result is the result of an Exec command.
type Result struct {
	LastInsertId int64
	RowsAffected int64
}

// Query is either SELECT, INSERT, UPDATE or DELETE.
type Query interface {
	SQLWriter
	// SetFetchableFields should return a query with its fetchable fields set
	// to the given fields. If not applicable, it should return false as the
	// second return value.
	SetFetchableFields([]Field) (query Query, ok bool)
	GetDialect() string
}

// Table is anything you can Select from or Join.
type Table interface {
	SQLWriter
	IsTable()
}

// PolicyTable is a table that produces a policy (i.e. a predicate) to be
// enforced whenever it is invoked in a query. This is equivalent to Postgres'
// Row Level Security (RLS) feature but works application-side. Only SELECT,
// UPDATE and DELETE queries are affected.
type PolicyTable interface {
	Table
	Policy(ctx context.Context, dialect string) (Predicate, error)
}

// Window is a window used in SQL window functions.
type Window interface {
	SQLWriter
	IsWindow()
}

// Field is either a table column or some SQL expression.
type Field interface {
	SQLWriter
	IsField()
}

// Predicate is an SQL expression that evaluates to true or false.
type Predicate interface {
	Boolean
}

// Assignment is an SQL assignment 'field = value'.
type Assignment interface {
	SQLWriter
	IsAssignment()
}

// Any is a catch-all interface that covers every field type.
type Any interface {
	Array
	Binary
	Boolean
	Enum
	JSON
	Number
	String
	Time
	UUID
}

// Enumeration represents a Go enum.
type Enumeration interface {
	// Enumerate returns the names of all valid enum values.
	//
	// If the enum is backed by a string, each string in the slice is the
	// corresponding enum's string value.
	//
	// If the enum is backed by an int, each int index in the slice is the
	// corresponding enum's int value and the string is the enum's name. Enums
	// with empty string names are considered invalid, unless it is the very
	// first enum (at index 0).
	Enumerate() []string
}

// Array is a Field of array type.
type Array interface {
	Field
	IsArray()
}

// Binary is a Field of binary type.
type Binary interface {
	Field
	IsBinary()
}

// Boolean is a Field of boolean type.
type Boolean interface {
	Field
	IsBoolean()
}

// Enum is a Field of enum type.
type Enum interface {
	Field
	IsEnum()
}

// JSON is a Field of json type.
type JSON interface {
	Field
	IsJSON()
}

// Number is a Field of numeric type.
type Number interface {
	Field
	IsNumber()
}

// String is a Field of string type.
type String interface {
	Field
	IsString()
}

// Time is a Field of time type.
type Time interface {
	Field
	IsTime()
}

// UUID is a Field of uuid type.
type UUID interface {
	Field
	IsUUID()
}

// DialectValuer is any type that will yield a different driver.Valuer
// depending on the SQL dialect.
type DialectValuer interface {
	DialectValuer(dialect string) (driver.Valuer, error)
}

// TableStruct is meant to be embedded in table structs to make them implement
// the Table interface.
type TableStruct struct {
	schema string
	name   string
	alias  string
}

// ViewStruct is just an alias for TableStruct.
type ViewStruct = TableStruct

var _ Table = (*TableStruct)(nil)

// NewTableStruct creates a new TableStruct.
func NewTableStruct(schema, name, alias string) TableStruct {
	return TableStruct{schema: schema, name: name, alias: alias}
}

// WriteSQL implements the SQLWriter interface.
func (ts TableStruct) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	if ts.schema != "" {
		buf.WriteString(QuoteIdentifier(dialect, ts.schema) + ".")
	}
	buf.WriteString(QuoteIdentifier(dialect, ts.name))
	return nil
}

// GetAlias returns the alias of the TableStruct.
func (ts TableStruct) GetAlias() string { return ts.alias }

// IsTable implements the Table interface.
func (ts TableStruct) IsTable() {}

func withPrefix(w SQLWriter, prefix string) SQLWriter {
	if field, ok := w.(interface {
		SQLWriter
		WithPrefix(string) Field
	}); ok {
		return field.WithPrefix(prefix)
	}
	return w
}

func getAlias(w SQLWriter) string {
	if w, ok := w.(interface{ GetAlias() string }); ok {
		return w.GetAlias()
	}
	return ""
}

func toString(dialect string, w SQLWriter) string {
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	var args []any
	_ = w.WriteSQL(context.Background(), dialect, buf, &args, nil)
	return buf.String()
}

func writeFieldsWithPrefix(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, fields []Field, prefix string, includeAlias bool) error {
	var err error
	var alias string
	for i, field := range fields {
		if field == nil {
			return fmt.Errorf("field #%d is nil", i+1)
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		err = withPrefix(field, prefix).WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("field #%d: %w", i+1, err)
		}
		if includeAlias {
			if alias = getAlias(field); alias != "" {
				buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
			}
		}
	}
	return nil
}

func writeFields(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, fields []Field, includeAlias bool) error {
	var err error
	var alias string
	for i, field := range fields {
		if field == nil {
			return fmt.Errorf("field #%d is nil", i+1)
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		_, isQuery := field.(Query)
		if isQuery {
			buf.WriteString("(")
		}
		err = field.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("field #%d: %w", i+1, err)
		}
		if isQuery {
			buf.WriteString(")")
		}
		if includeAlias {
			if alias = getAlias(field); alias != "" {
				buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
			}
		}
	}
	return nil
}
