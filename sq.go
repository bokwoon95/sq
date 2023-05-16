package sq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/bokwoon95/sq/internal/googleuuid"
	"github.com/bokwoon95/sq/internal/pqarray"
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

// mapperFunctionPanicked recovers from any panics *except* for runtime error
// panics. Runtime error panics like out-of-bounds index accesses or failed
// type assertions are not normal so we don't want to swallow the stack trace
// by recovering from it.
//
// The function is called as such so that it shows up as
// "sq.mapperFunctionPanicked" in panic stack trace, giving the user a
// descriptive clue of what went wrong (i.e. their mapper function panicked).
func mapperFunctionPanicked(err *error) {
	if r := recover(); r != nil {
		switch r := r.(type) {
		case error:
			if runtimeErr, ok := r.(runtime.Error); ok {
				panic(runtimeErr)
			}
			*err = r
		default:
			*err = fmt.Errorf(fmt.Sprint(r))
		}
	}
}

// ArrayValue takes in a []string, []int, []int64, []int32, []float64,
// []float32 or []bool and returns a driver.Valuer for that type. For Postgres,
// it serializes into a Postgres array. Otherwise, it serializes into a JSON
// array.
func ArrayValue(value any) driver.Valuer {
	return &arrayValue{value: value}
}

type arrayValue struct {
	dialect string
	value   any
}

// Value implements the driver.Valuer interface.
func (v *arrayValue) Value() (driver.Value, error) {
	switch v.value.(type) {
	case []string, []int, []int64, []int32, []float64, []float32, []bool:
		break
	default:
		return nil, fmt.Errorf("value %#v is not a []string, []int, []int32, []float64, []float32 or []bool", v.value)
	}
	if v.dialect != DialectPostgres {
		var b strings.Builder
		err := json.NewEncoder(&b).Encode(v.value)
		if err != nil {
			return nil, err
		}
		return strings.TrimSpace(b.String()), nil
	}
	if ints, ok := v.value.([]int); ok {
		bigints := make([]int64, len(ints))
		for i, num := range ints {
			bigints[i] = int64(num)
		}
		v.value = bigints
	}
	return pqarray.Array(v.value).Value()
}

// DialectValuer implements the DialectValuer interface.
func (v *arrayValue) DialectValuer(dialect string) (driver.Valuer, error) {
	v.dialect = dialect
	return v, nil
}

// EnumValue takes in an Enumeration and returns a driver.Valuer which
// serializes the enum into a string and additionally checks if the enum is
// valid.
func EnumValue(value Enumeration) driver.Valuer {
	return &enumValue{value: value}
}

type enumValue struct {
	value Enumeration
}

// Value implements the driver.Valuer interface.
func (v *enumValue) Value() (driver.Value, error) {
	value := reflect.ValueOf(v.value)
	names := v.value.Enumerate()
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := int(value.Int())
		if i < 0 || i >= len(names) {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		name := names[i]
		if name == "" && i != 0 {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		return name, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i := int(value.Uint())
		if i < 0 || i >= len(names) {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		name := names[i]
		if name == "" && i != 0 {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		return name, nil
	case reflect.String:
		typ := value.Type()
		name := value.String()
		if getEnumIndex(name, names, typ) < 0 {
			return nil, fmt.Errorf("%q is not a valid %T", name, v.value)
		}
		return name, nil
	default:
		return nil, fmt.Errorf("underlying type of %[1]v is neither an integer nor string (%[1]T)", v.value)
	}
}

var (
	enumIndexMu sync.RWMutex
	enumIndex   = make(map[reflect.Type]map[string]int)
)

// getEnumIndex returns the index of the enum within the names slice.
func getEnumIndex(name string, names []string, typ reflect.Type) int {
	if len(names) <= 4 {
		for idx := range names {
			if names[idx] == name {
				return idx
			}
		}
		return -1
	}
	var nameIndex map[string]int
	enumIndexMu.RLock()
	nameIndex = enumIndex[typ]
	enumIndexMu.RUnlock()
	if nameIndex != nil {
		idx, ok := nameIndex[name]
		if !ok {
			return -1
		}
		return idx
	}
	idx := -1
	nameIndex = make(map[string]int)
	for i := range names {
		if names[i] == name {
			idx = i
		}
		nameIndex[names[i]] = i
	}
	enumIndexMu.Lock()
	enumIndex[typ] = nameIndex
	enumIndexMu.Unlock()
	return idx
}

// JSONValue takes in an interface{} and returns a driver.Valuer which runs the
// value through json.Marshal before submitting it to the database.
func JSONValue(value any) driver.Valuer {
	return &jsonValue{value: value}
}

type jsonValue struct {
	value any
}

// Value implements the driver.Valuer interface.
func (v *jsonValue) Value() (driver.Value, error) {
	var b strings.Builder
	err := json.NewEncoder(&b).Encode(v.value)
	return strings.TrimSpace(b.String()), err
}

// UUIDValue takes in a type whose underlying type must be a [16]byte and
// returns a driver.Valuer.
func UUIDValue(value any) driver.Valuer {
	return &uuidValue{value: value}
}

type uuidValue struct {
	dialect string
	value   any
}

// Value implements the driver.Valuer interface.
func (v *uuidValue) Value() (driver.Value, error) {
	value := reflect.ValueOf(v.value)
	typ := value.Type()
	if value.Kind() != reflect.Array || value.Len() != 16 || typ.Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("%[1]v %[1]T is not [16]byte", v.value)
	}
	var uuid [16]byte
	for i := 0; i < value.Len(); i++ {
		uuid[i] = value.Index(i).Interface().(byte)
	}
	if v.dialect != DialectPostgres {
		return uuid[:], nil
	}
	var buf [36]byte
	googleuuid.EncodeHex(buf[:], uuid)
	return string(buf[:]), nil
}

// DialectValuer implements the DialectValuer interface.
func (v *uuidValue) DialectValuer(dialect string) (driver.Valuer, error) {
	v.dialect = dialect
	return v, nil
}

func preprocessValue(dialect string, value any) (any, error) {
	if dialectValuer, ok := value.(DialectValuer); ok {
		driverValuer, err := dialectValuer.DialectValuer(dialect)
		if err != nil {
			return nil, fmt.Errorf("calling DialectValuer on %#v: %w", dialectValuer, err)
		}
		value = driverValuer
	}
	switch value := value.(type) {
	case nil:
		return nil, nil
	case Enumeration:
		driverValue, err := (&enumValue{value: value}).Value()
		if err != nil {
			return nil, fmt.Errorf("converting %#v to string: %w", value, err)
		}
		return driverValue, nil
	case [16]byte:
		driverValue, err := (&uuidValue{value: value}).Value()
		if err != nil {
			if dialect == DialectPostgres {
				return nil, fmt.Errorf("converting %#v to string: %w", value, err)
			}
			return nil, fmt.Errorf("converting %#v to bytes: %w", value, err)
		}
		return driverValue, nil
	case driver.Valuer:
		driverValue, err := value.Value()
		if err != nil {
			return nil, fmt.Errorf("calling Value on %#v: %w", value, err)
		}
		return driverValue, nil
	}
	return value, nil
}
