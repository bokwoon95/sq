package sq

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"time"

	"github.com/bokwoon95/sq/internal/googleuuid"
	"github.com/bokwoon95/sq/internal/pqarray"
)

// Row represents the state of a row after a call to rows.Next().
type Row struct {
	dialect    string
	sqlRows    *sql.Rows
	index      int
	fields     []Field
	scanDest   []any
	rawSQLMode bool
}

// ColumnTypes returns the names of the columns returned by the query. This
// method can only be called in a rowmapper if it is paired with a raw SQL
// query e.g. Queryf("SELECT * FROM my_table"). Otherwise, an error will be
// returned.
func (row *Row) Columns() []string {
	if row.sqlRows == nil {
		row.rawSQLMode = true
		return nil
	}
	columns, err := row.sqlRows.Columns()
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"row.Columns(): %w", err))
	}
	return columns
}

// ColumnTypes returns the column types returned by the query. This method can
// only be called in a rowmapper if it is paired with a raw SQL query e.g.
// Queryf("SELECT * FROM my_table"). Otherwise, an error will be returned.
func (row *Row) ColumnTypes() []*sql.ColumnType {
	if row.sqlRows == nil {
		row.rawSQLMode = true
		return nil
	}
	columnTypes, err := row.sqlRows.ColumnTypes()
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"row.ColumnTypes(): %w", err))
	}
	return columnTypes
}

// Values returns the values of the current row. This method can only be called
// in a rowmapper if it is paired with a raw SQL query e.g. Queryf("SELECT *
// FROM my_table"). Otherwise, an error will be returned.
func (row *Row) Values() []any {
	if row.sqlRows == nil {
		row.rawSQLMode = true
		return nil
	}
	if len(row.scanDest) == 0 {
		columns, err := row.sqlRows.Columns()
		if err != nil {
			panic(fmt.Errorf(callsite(1)+"row.Values(): %w", err))
		}
		row.scanDest = make([]any, len(columns))
	}
	values := make([]any, len(row.scanDest))
	for i := range row.scanDest {
		row.scanDest[i] = &values[i]
	}
	err := row.sqlRows.Scan(row.scanDest...)
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"row.Values(): %w", err))
	}
	return values
}

// Value returns the value of the expression. It is intended for use cases
// where you only know the name of the column but not its type to scan into.
// The underlying type of the value is determined by the database driver you
// are using.
func (row *Row) Value(format string, values ...any) any {
	if row.sqlRows == nil {
		var value any
		row.fields = append(row.fields, Expr(format, values...))
		row.scanDest = append(row.scanDest, &value)
		return nil
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*any)
	return *scanDest
}

// Scan scans the expression into destPtr.
func (row *Row) Scan(destPtr any, format string, values ...any) {
	row.scan(destPtr, Expr(format, values...), 1)
}

// ScanField scans the field into destPtr.
func (row *Row) ScanField(destPtr any, field Field) {
	row.scan(destPtr, field, 1)
}

func (row *Row) scan(destPtr any, field Field, skip int) {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		switch destPtr.(type) {
		case *bool, *sql.NullBool:
			row.scanDest = append(row.scanDest, &sql.NullBool{})
		case *float64, *sql.NullFloat64:
			row.scanDest = append(row.scanDest, &sql.NullFloat64{})
		case *int32, *sql.NullInt32:
			row.scanDest = append(row.scanDest, &sql.NullInt32{})
		case *int, *int64, *sql.NullInt64:
			row.scanDest = append(row.scanDest, &sql.NullInt64{})
		case *string, *sql.NullString:
			row.scanDest = append(row.scanDest, &sql.NullString{})
		case *time.Time, *sql.NullTime:
			row.scanDest = append(row.scanDest, &sql.NullTime{})
		default:
			if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
				panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
			}
			row.scanDest = append(row.scanDest, destPtr)
		}
		return
	}
	defer func() {
		row.index++
	}()
	switch destPtr := destPtr.(type) {
	case *bool:
		scanDest := row.scanDest[row.index].(*sql.NullBool)
		*destPtr = scanDest.Bool
	case *sql.NullBool:
		scanDest := row.scanDest[row.index].(*sql.NullBool)
		*destPtr = *scanDest
	case *float64:
		scanDest := row.scanDest[row.index].(*sql.NullFloat64)
		*destPtr = scanDest.Float64
	case *sql.NullFloat64:
		scanDest := row.scanDest[row.index].(*sql.NullFloat64)
		*destPtr = *scanDest
	case *int:
		scanDest := row.scanDest[row.index].(*sql.NullInt64)
		*destPtr = int(scanDest.Int64)
	case *int32:
		scanDest := row.scanDest[row.index].(*sql.NullInt32)
		*destPtr = scanDest.Int32
	case *sql.NullInt32:
		scanDest := row.scanDest[row.index].(*sql.NullInt32)
		*destPtr = *scanDest
	case *int64:
		scanDest := row.scanDest[row.index].(*sql.NullInt64)
		*destPtr = scanDest.Int64
	case *sql.NullInt64:
		scanDest := row.scanDest[row.index].(*sql.NullInt64)
		*destPtr = *scanDest
	case *string:
		scanDest := row.scanDest[row.index].(*sql.NullString)
		*destPtr = scanDest.String
	case *sql.NullString:
		scanDest := row.scanDest[row.index].(*sql.NullString)
		*destPtr = *scanDest
	case *time.Time:
		scanDest := row.scanDest[row.index].(*sql.NullTime)
		*destPtr = scanDest.Time
	case *sql.NullTime:
		scanDest := row.scanDest[row.index].(*sql.NullTime)
		*destPtr = *scanDest
	default:
		destValue := reflect.ValueOf(destPtr).Elem()
		srcValue := reflect.ValueOf(row.scanDest[row.index]).Elem()
		destValue.Set(srcValue)
	}
}

// Array scans the array expression into destPtr. The destPtr must be a pointer
// to a []string, []int, []int64, []int32, []float64, []float32 or []bool.
func (row *Row) Array(destPtr any, format string, values ...any) {
	row.array(destPtr, Expr(format, values...), 1)
}

// ArrayField scans the array field into destPtr. The destPtr must be a pointer
// to a []string, []int, []int64, []int32, []float64, []float32 or []bool.
func (row *Row) ArrayField(destPtr any, field Array) {
	row.array(destPtr, field, 1)
}

func (row *Row) array(destPtr any, field Array, skip int) {
	if row.sqlRows == nil {
		if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		if row.dialect == DialectPostgres {
			switch destPtr.(type) {
			case *[]string, *[]int, *[]int64, *[]int32, *[]float64, *[]float32, *[]bool:
				break
			default:
				panic(fmt.Errorf(callsite(skip+1)+"destptr (%T) must be either a pointer to a []string, []int, []int64, []int32, []float64, []float32 or []bool", destPtr))
			}
		}
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect:     row.dialect,
			displayType: displayTypeString,
		})
		return
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*nullBytes)
	if !scanDest.valid {
		return
	}
	if row.dialect != DialectPostgres {
		err := json.Unmarshal(scanDest.bytes, destPtr)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unmarshaling json %q into %T: %w", string(scanDest.bytes), destPtr, err))
		}
		return
	}
	switch destPtr := destPtr.(type) {
	case *[]string:
		var array pqarray.StringArray
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to string array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]int:
		var array pqarray.Int64Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int64 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = (*destPtr)[:cap(*destPtr)]
		if len(*destPtr) < len(array) {
			*destPtr = make([]int, len(array))
		}
		*destPtr = (*destPtr)[:len(array)]
		for i, num := range array {
			(*destPtr)[i] = int(num)
		}
	case *[]int64:
		var array pqarray.Int64Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int64 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]int32:
		var array pqarray.Int32Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int32 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]float64:
		var array pqarray.Float64Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to float64 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]float32:
		var array pqarray.Float32Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to float32 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]bool:
		var array pqarray.BoolArray
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to bool array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	default:
		panic(fmt.Errorf(callsite(skip+1)+"destptr (%T) must be either a pointer to a []string, []int, []int64, []int32, []float64, []float32 or []bool", destPtr))
	}
}

// Bytes returns the []byte value of the expression.
func (row *Row) Bytes(format string, values ...any) []byte {
	return row.BytesField(Expr(format, values...))
}

// BytesField returns the []byte value of the field.
func (row *Row) BytesField(field Binary) []byte {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect: row.dialect,
		})
		return nil
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*nullBytes)
	var b []byte
	if scanDest.valid {
		b = make([]byte, len(scanDest.bytes))
		copy(b, scanDest.bytes)
	}
	return b
}

// == Bool == //

// Bool returns the bool value of the expression.
func (row *Row) Bool(format string, values ...any) bool {
	return row.NullBoolField(Expr(format, values...)).Bool
}

// BoolField returns the bool value of the field.
func (row *Row) BoolField(field Boolean) bool {
	return row.NullBoolField(field).Bool
}

// NullBool returns the sql.NullBool value of the expression.
func (row *Row) NullBool(format string, values ...any) sql.NullBool {
	return row.NullBoolField(Expr(format, values...))
}

// NullBoolField returns the sql.NullBool value of the field.
func (row *Row) NullBoolField(field Boolean) sql.NullBool {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &sql.NullBool{})
		return sql.NullBool{}
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*sql.NullBool)
	return *scanDest
}

// Enum scans the enum expression into destPtr.
func (row *Row) Enum(destPtr Enumeration, format string, values ...any) {
	row.enum(destPtr, Expr(format, values...), 1)
}

// EnumField scans the enum field into destPtr.
func (row *Row) EnumField(destPtr Enumeration, field Enum) {
	row.enum(destPtr, field, 1)
}

func (row *Row) enum(destPtr Enumeration, field Enum, skip int) {
	if row.sqlRows == nil {
		destType := reflect.TypeOf(destPtr)
		if destType.Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		row.fields = append(row.fields, field)
		switch destType.Elem().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.String:
			row.scanDest = append(row.scanDest, &sql.NullString{})
		default:
			panic(fmt.Errorf(callsite(skip+1)+"underlying type of %[1]v is neither an integer or string (%[1]T)", destPtr))
		}
		return
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*sql.NullString)
	names := destPtr.Enumerate()
	enumIndex := 0
	destValue := reflect.ValueOf(destPtr).Elem()
	if scanDest.Valid {
		enumIndex = getEnumIndex(scanDest.String, names, destValue.Type())
	}
	if enumIndex < 0 {
		panic(fmt.Errorf(callsite(skip+1)+"%q is not a valid %T", scanDest.String, destPtr))
	}
	switch destValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		destValue.SetInt(int64(enumIndex))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		destValue.SetUint(uint64(enumIndex))
	case reflect.String:
		destValue.SetString(scanDest.String)
	}
}

// Float64 returns the float64 value of the expression.
func (row *Row) Float64(format string, values ...any) float64 {
	return row.NullFloat64Field(Expr(format, values...)).Float64
}

// Float64Field returns the float64 value of the field.
func (row *Row) Float64Field(field Number) float64 {
	return row.NullFloat64Field(field).Float64
}

// NullFloat64 returns the sql.NullFloat64 valye of the expression.
func (row *Row) NullFloat64(format string, values ...any) sql.NullFloat64 {
	return row.NullFloat64Field(Expr(format, values...))
}

// NullFloat64Field returns the sql.NullFloat64 value of the field.
func (row *Row) NullFloat64Field(field Number) sql.NullFloat64 {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &sql.NullFloat64{})
		return sql.NullFloat64{}
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*sql.NullFloat64)
	return *scanDest
}

// Int returns the int value of the expression.
func (row *Row) Int(format string, values ...any) int {
	return int(row.NullInt64Field(Expr(format, values...)).Int64)
}

// IntField returns the int value of the field.
func (row *Row) IntField(field Number) int {
	return int(row.NullInt64Field(field).Int64)
}

// Int64 returns the int64 value of the expression.
func (row *Row) Int64(format string, values ...any) int64 {
	return row.NullInt64Field(Expr(format, values...)).Int64
}

// Int64Field returns the int64 value of the field.
func (row *Row) Int64Field(field Number) int64 {
	return row.NullInt64Field(field).Int64
}

// NullInt64 returns the sql.NullInt64 value of the expression.
func (row *Row) NullInt64(format string, values ...any) sql.NullInt64 {
	return row.NullInt64Field(Expr(format, values...))
}

// NullInt64Field returns the sql.NullInt64 value of the field.
func (row *Row) NullInt64Field(field Number) sql.NullInt64 {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &sql.NullInt64{})
		return sql.NullInt64{}
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*sql.NullInt64)
	return *scanDest
}

// JSON scans the JSON expression into destPtr.
func (row *Row) JSON(destPtr any, format string, values ...any) {
	row.json(destPtr, Expr(format, values...), 1)
}

// JSONField scans the JSON field into destPtr.
func (row *Row) JSONField(destPtr any, field JSON) {
	row.json(destPtr, field, 1)
}

func (row *Row) json(destPtr any, field JSON, skip int) {
	if row.sqlRows == nil {
		if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect:     row.dialect,
			displayType: displayTypeString,
		})
		return
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*nullBytes)
	if scanDest.valid {
		err := json.Unmarshal(scanDest.bytes, destPtr)
		if err != nil {
			_, file, line, _ := runtime.Caller(skip + 1)
			panic(fmt.Errorf(callsite(skip+1)+"unmarshaling json %q into %T: %w", file, line, string(scanDest.bytes), destPtr, err))
		}
	}
}

// String returns the string value of the expression.
func (row *Row) String(format string, values ...any) string {
	return row.NullStringField(Expr(format, values...)).String
}

// String returns the string value of the field.
func (row *Row) StringField(field String) string {
	return row.NullStringField(field).String
}

// NullString returns the sql.NullString value of the expression.
func (row *Row) NullString(format string, values ...any) sql.NullString {
	return row.NullStringField(Expr(format, values...))
}

// NullStringField returns the sql.NullString value of the field.
func (row *Row) NullStringField(field String) sql.NullString {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &sql.NullString{})
		return sql.NullString{}
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*sql.NullString)
	return *scanDest
}

// Time returns the time.Time value of the expression.
func (row *Row) Time(format string, values ...any) time.Time {
	return row.NullTimeField(Expr(format, values...)).Time
}

// Time returns the time.Time value of the field.
func (row *Row) TimeField(field Time) time.Time {
	return row.NullTimeField(field).Time
}

// NullTime returns the sql.NullTime value of the expression.
func (row *Row) NullTime(format string, values ...any) sql.NullTime {
	return row.NullTimeField(Expr(format, values...))
}

// NullTimeField returns the sql.NullTime value of the field.
func (row *Row) NullTimeField(field Time) sql.NullTime {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &sql.NullTime{})
		return sql.NullTime{}
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*sql.NullTime)
	return *scanDest
}

// UUID scans the UUID expression into destPtr.
func (row *Row) UUID(destPtr any, format string, values ...any) {
	row.uuid(destPtr, Expr(format, values...), 1)
}

// UUIDField scans the UUID field into destPtr.
func (row *Row) UUIDField(destPtr any, field UUID) {
	row.uuid(destPtr, field, 1)
}

func (row *Row) uuid(destPtr any, field UUID, skip int) {
	if row.sqlRows == nil {
		if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		destValue := reflect.ValueOf(destPtr).Elem()
		if destValue.Kind() != reflect.Array || destValue.Len() != 16 || destValue.Type().Elem().Kind() != reflect.Uint8 {
			panic(fmt.Errorf(callsite(skip+1)+"%T is not a pointer to a [16]byte", destPtr))
		}
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect:     row.dialect,
			displayType: displayTypeUUID,
		})
		return
	}
	defer func() {
		row.index++
	}()
	scanDest := row.scanDest[row.index].(*nullBytes)
	var err error
	var uuid [16]byte
	if len(scanDest.bytes) == 16 {
		copy(uuid[:], scanDest.bytes)
	} else {
		uuid, err = googleuuid.ParseBytes(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"parsing %q as UUID string: %w", string(scanDest.bytes), err))
		}
	}
	if destArrayPtr, ok := destPtr.(*[16]byte); ok {
		copy((*destArrayPtr)[:], uuid[:])
		return
	}
	destValue := reflect.ValueOf(destPtr).Elem()
	for i := 0; i < 16; i++ {
		destValue.Index(i).Set(reflect.ValueOf(uuid[i]))
	}
}

// Column keeps track of what the values mapped to what Field in an
// InsertQuery or SelectQuery.
type Column struct {
	dialect string
	// determines if UPDATE or INSERT
	isUpdate bool
	// UPDATE
	assignments Assignments
	// INSERT
	rowStarted    bool
	rowEnded      bool
	firstField    string
	insertColumns Fields
	rowValues     RowValues
}

// Set maps the value to the Field.
func (col *Column) Set(field Field, value any) {
	if field == nil {
		panic(fmt.Errorf(callsite(1) + "setting a nil field"))
	}
	// UPDATE mode
	if col.isUpdate {
		col.assignments = append(col.assignments, Set(field, value))
		return
	}
	// INSERT mode
	name := toString(col.dialect, field)
	if name == "" {
		panic(fmt.Errorf(callsite(1) + "field name is empty"))
	}
	if !col.rowStarted {
		col.rowStarted = true
		col.firstField = name
		col.insertColumns = append(col.insertColumns, field)
		col.rowValues = append(col.rowValues, RowValue{value})
		return
	}
	if col.rowStarted && name == col.firstField {
		if !col.rowEnded {
			col.rowEnded = true
		}
		// Start a new RowValue
		col.rowValues = append(col.rowValues, RowValue{value})
		return
	}
	if !col.rowEnded {
		col.insertColumns = append(col.insertColumns, field)
	}
	// Append to last RowValue
	last := len(col.rowValues) - 1
	col.rowValues[last] = append(col.rowValues[last], value)
}

// SetBytes maps the []byte value to the field.
func (col *Column) SetBytes(field Binary, value []byte) { col.Set(field, value) }

// SetBool maps the bool value to the field.
func (col *Column) SetBool(field Boolean, value bool) { col.Set(field, value) }

// SetFloat64 maps the float64 value to the field.
func (col *Column) SetFloat64(field Number, value float64) { col.Set(field, value) }

// SetInt maps the int value to the field.
func (col *Column) SetInt(field Number, value int) { col.Set(field, value) }

// SetInt64 maps the int64 value to the field.
func (col *Column) SetInt64(field Number, value int64) { col.Set(field, value) }

// SetString maps the string value to the field.
func (col *Column) SetString(field String, value string) { col.Set(field, value) }

// SetTime maps the time.Time value to the field.
func (col *Column) SetTime(field Time, value time.Time) { col.Set(field, value) }

// SetArray maps the array value to the field. The value should be []string,
// []int, []int64, []int32, []float64, []float32 or []bool.
func (col *Column) SetArray(field Array, value any) { col.Set(field, ArrayValue(value)) }

// SetEnum maps the enum value to the field.
func (col *Column) SetEnum(field Enum, value Enumeration) { col.Set(field, EnumValue(value)) }

// SetJSON maps the JSON value to the field. The value should be able to be
// convertible to JSON using json.Marshal.
func (col *Column) SetJSON(field JSON, value any) { col.Set(field, JSONValue(value)) }

// SetUUID maps the UUID value to the field. The value's type or underlying
// type should be [16]byte.
func (col *Column) SetUUID(field UUID, value any) { col.Set(field, UUIDValue(value)) }

func callsite(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return ""
	}
	return filepath.Base(file) + ":" + strconv.Itoa(line) + ": "
}

type displayType int8

const (
	displayTypeBinary displayType = iota
	displayTypeString
	displayTypeUUID
)

// nullBytes is used in place of scanning into *[]byte. We use *nullBytes
// instead of *[]byte because of the displayType field, which determines how to
// render the value to the user. This is important for logging the query
// results, because UUIDs/JSON/Arrays are all scanned into bytes but we don't
// want to display them as bytes (we need to convert them to UUID/JSON/Array
// strings instead).
type nullBytes struct {
	bytes       []byte
	dialect     string
	displayType displayType
	valid       bool
}

func (n *nullBytes) Scan(value any) error {
	if value == nil {
		n.bytes, n.valid = nil, false
		return nil
	}
	n.valid = true
	switch value := value.(type) {
	case string:
		n.bytes = []byte(value)
	case []byte:
		n.bytes = value
	default:
		return fmt.Errorf("unable to convert %#v to bytes", value)
	}
	return nil
}

func (n *nullBytes) Value() (driver.Value, error) {
	if !n.valid {
		return nil, nil
	}
	switch n.displayType {
	case displayTypeString:
		return string(n.bytes), nil
	case displayTypeUUID:
		if n.dialect != "postgres" {
			return n.bytes, nil
		}
		var uuid [16]byte
		var buf [36]byte
		copy(uuid[:], n.bytes)
		googleuuid.EncodeHex(buf[:], uuid)
		return string(buf[:]), nil
	default:
		return n.bytes, nil
	}
}
