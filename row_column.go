package sq

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/bokwoon95/sq/internal/googleuuid"
	"github.com/bokwoon95/sq/internal/pqarray"
)

// Row represents the state of a row after a call to rows.Next().
type Row struct {
	active       bool
	index        int
	fields       []Field
	dest         []any
	destIsString map[int]struct{}
	destIsUUID   map[int]struct{}
	dialect      string
}

func newRow(dialect string) *Row {
	return &Row{
		dialect:      dialect,
		destIsString: make(map[int]struct{}),
		destIsUUID:   make(map[int]struct{}),
	}
}

// Scan scans the expression into dest.
func (r *Row) Scan(dest any, format string, values ...any) {
	r.scan(dest, Expr(format, values...), 1)
}

// ScanField scans the field into dest.
func (r *Row) ScanField(dest any, field Field) {
	r.scan(dest, field, 1)
}

func (r *Row) scan(dest any, field Field, skip int) {
	if !r.active {
		r.fields = append(r.fields, field)
		switch dest.(type) {
		case *bool, *sql.NullBool:
			r.dest = append(r.dest, &sql.NullBool{})
		case *float64, *sql.NullFloat64:
			r.dest = append(r.dest, &sql.NullFloat64{})
		case *int32, *sql.NullInt32:
			r.dest = append(r.dest, &sql.NullInt32{})
		case *int, *int64, *sql.NullInt64:
			r.dest = append(r.dest, &sql.NullInt64{})
		case *string, *sql.NullString:
			r.dest = append(r.dest, &sql.NullString{})
		case *time.Time, *sql.NullTime:
			r.dest = append(r.dest, &sql.NullTime{})
		default:
			if reflect.TypeOf(dest).Kind() != reflect.Ptr {
				panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
			}
			r.dest = append(r.dest, dest)
		}
		return
	}
	switch ptr := dest.(type) {
	case *bool:
		nullbool := r.dest[r.index].(*sql.NullBool)
		*ptr = nullbool.Bool
	case *sql.NullBool:
		nullbool := r.dest[r.index].(*sql.NullBool)
		*ptr = *nullbool
	case *float64:
		nullfloat64 := r.dest[r.index].(*sql.NullFloat64)
		*ptr = nullfloat64.Float64
	case *sql.NullFloat64:
		nullfloat64 := r.dest[r.index].(*sql.NullFloat64)
		*ptr = *nullfloat64
	case *int:
		nullint64 := r.dest[r.index].(*sql.NullInt64)
		*ptr = int(nullint64.Int64)
	case *int32:
		nullint32 := r.dest[r.index].(*sql.NullInt32)
		*ptr = nullint32.Int32
	case *sql.NullInt32:
		nullint32 := r.dest[r.index].(*sql.NullInt32)
		*ptr = *nullint32
	case *int64:
		nullint64 := r.dest[r.index].(*sql.NullInt64)
		*ptr = nullint64.Int64
	case *sql.NullInt64:
		nullint64 := r.dest[r.index].(*sql.NullInt64)
		*ptr = *nullint64
	case *string:
		nullstring := r.dest[r.index].(*sql.NullString)
		*ptr = nullstring.String
	case *sql.NullString:
		nullstring := r.dest[r.index].(*sql.NullString)
		*ptr = *nullstring
	case *time.Time:
		nulltime := r.dest[r.index].(*sql.NullTime)
		*ptr = nulltime.Time
	case *sql.NullTime:
		nulltime := r.dest[r.index].(*sql.NullTime)
		*ptr = *nulltime
	default:
		destValue := reflect.ValueOf(dest)
		if destValue.Type().Kind() != reflect.Ptr {
			panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
		}
		destValue.Elem().Set(reflect.ValueOf(r.dest[r.index]).Elem())
	}
	r.index++
}

// Array scans the array expression into dest.
func (r *Row) Array(dest any, format string, values ...any) {
	r.array(dest, Expr(format, values...), 1)
}

// ArrayField scans the array field into dest.
func (r *Row) ArrayField(dest any, field Array) {
	r.array(dest, field, 1)
}

func (r *Row) array(dest any, field Array, skip int) {
	if !r.active {
		if reflect.TypeOf(dest).Kind() != reflect.Ptr {
			panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
		}
		r.fields = append(r.fields, field)
		if r.dialect == DialectPostgres {
			r.dest = append(r.dest, pqarray.Array(dest))
		} else {
			r.dest = append(r.dest, &[]byte{})
		}
		r.destIsString[len(r.dest)-1] = struct{}{}
		return
	}
	if r.dialect == DialectPostgres {
		switch pqArray := r.dest[r.index].(type) {
		case *pqarray.BoolArray:
			dest, ok := dest.(*[]bool)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[]bool for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([]bool, len(*pqArray))
			}
			copy(*dest, *pqArray)
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.Int64Array:
			dest, ok := dest.(*[]int64)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[]int64 for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([]int64, len(*pqArray))
			}
			copy(*dest, *pqArray)
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.Int32Array:
			dest, ok := dest.(*[]int32)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[]int32 for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([]int32, len(*pqArray))
			}
			copy(*dest, *pqArray)
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.Float64Array:
			dest, ok := dest.(*[]float64)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[]float64 for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([]float64, len(*pqArray))
			}
			copy(*dest, *pqArray)
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.Float32Array:
			dest, ok := dest.(*[]float32)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[]float32 for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([]float32, len(*pqArray))
			}
			copy(*dest, *pqArray)
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.StringArray:
			dest, ok := dest.(*[]string)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[]string for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([]string, len(*pqArray))
			}
			copy(*dest, *pqArray)
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.ByteaArray:
			dest, ok := dest.(*[][]byte)
			if !ok {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray expected *[][]byte for dest, got %T", file, line, dest))
			}
			if len(*dest) < len(*pqArray) {
				*dest = make([][]byte, len(*pqArray))
			}
			for i, b := range *pqArray {
				if len((*dest)[i]) < len(b) {
					(*dest)[i] = make([]byte, len(b))
				}
				copy((*dest)[i], b)
			}
			*dest = (*dest)[:len(*pqArray)]
		case *pqarray.GenericArray:
			destValue := reflect.ValueOf(dest)
			if destValue.Type().Kind() != reflect.Ptr {
				panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
			}
			destValue.Elem().Set(reflect.ValueOf(r.dest[r.index]).Elem())
		default:
		}
	} else {
		bptr := r.dest[r.index].(*[]byte)
		if len(*bptr) > 0 {
			err := json.Unmarshal(*bptr, dest)
			if err != nil {
				_, file, line, _ := runtime.Caller(skip + 1)
				panic(fmt.Errorf("%s:%d ScanArray unmarshaling json '%s' into %T: %w", file, line, string(*bptr), dest, err))
			}
		}
	}
	r.index++
}

// Bytes returns the []byte value of the expression.
func (r *Row) Bytes(format string, values ...any) []byte {
	return r.BytesField(Expr(format, values...))
}

// BytesField returns the []byte value of the field.
func (r *Row) BytesField(field Binary) []byte {
	if !r.active {
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &[]byte{})
		return nil
	}
	bptr := r.dest[r.index].(*[]byte)
	r.index++
	if len(*bptr) == 0 {
		return nil
	}
	b := make([]byte, len(*bptr))
	copy(b, *bptr)
	return b
}

// == Bool == //

// Bool returns the bool value of the expression.
func (r *Row) Bool(format string, values ...any) bool {
	return r.NullBoolField(Expr(format, values...)).Bool
}

// BoolField returns the bool value of the field.
func (r *Row) BoolField(field Boolean) bool {
	return r.NullBoolField(field).Bool
}

// NullBool returns the sql.NullBool value of the expression.
func (r *Row) NullBool(format string, values ...any) sql.NullBool {
	return r.NullBoolField(Expr(format, values...))
}

// NullBoolField returns the sql.NullBool value of the field.
func (r *Row) NullBoolField(field Boolean) sql.NullBool {
	if !r.active {
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &sql.NullBool{})
		return sql.NullBool{}
	}
	nullbool := r.dest[r.index].(*sql.NullBool)
	r.index++
	return *nullbool
}

// Enum scans the enum expression into dest.
func (r *Row) Enum(dest Enumeration, format string, values ...any) {
	r.enum(dest, Expr(format, values...), 1)
}

// EnumField scans the enum field into dest.
func (r *Row) EnumField(dest Enumeration, field Enum) {
	r.enum(dest, field, 1)
}

func (r *Row) enum(dest Enumeration, field Enum, skip int) {
	if !r.active {
		typ := reflect.TypeOf(dest)
		if typ.Kind() != reflect.Ptr {
			panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
		}
		switch typ.Elem().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.String:
		default:
			panic(fmt.Errorf("underlying type of %[1]v is neither an integer or string (%[1]T)", dest))
		}
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &sql.NullString{})
		return
	}
	nullstring := r.dest[r.index].(*sql.NullString)
	r.index++
	val := reflect.ValueOf(dest).Elem()
	names := dest.Enumerate()
	i := 0
	if nullstring.Valid {
		i = getEnumIndex(nullstring.String, names, val.Type())
	}
	if i < 0 {
		panic(fmt.Errorf("%q is not a valid %T", nullstring.String, dest))
	}
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val.SetInt(int64(i))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val.SetUint(uint64(i))
	case reflect.String:
		val.SetString(nullstring.String)
	}
}

// Float64 returns the float64 value of the expression.
func (r *Row) Float64(format string, values ...any) float64 {
	return r.NullFloat64Field(Expr(format, values...)).Float64
}

// Float64Field returns the float64 value of the field.
func (r *Row) Float64Field(field Number) float64 {
	return r.NullFloat64Field(field).Float64
}

// NullFloat64 returns the sql.NullFloat64 valye of the expression.
func (r *Row) NullFloat64(format string, values ...any) sql.NullFloat64 {
	return r.NullFloat64Field(Expr(format, values...))
}

// NullFloat64Field returns the sql.NullFloat64 value of the field.
func (r *Row) NullFloat64Field(field Number) sql.NullFloat64 {
	if !r.active {
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &sql.NullFloat64{})
		return sql.NullFloat64{}
	}
	nullfloat64 := r.dest[r.index].(*sql.NullFloat64)
	r.index++
	return *nullfloat64
}

// Int returns the int value of the expression.
func (r *Row) Int(format string, values ...any) int {
	return int(r.NullInt64Field(Expr(format, values...)).Int64)
}

// IntField returns the int value of the field.
func (r *Row) IntField(field Number) int {
	return int(r.NullInt64Field(field).Int64)
}

// Int64 returns the int64 value of the expression.
func (r *Row) Int64(format string, values ...any) int64 {
	return r.NullInt64Field(Expr(format, values...)).Int64
}

// Int64Field returns the int64 value of the field.
func (r *Row) Int64Field(field Number) int64 {
	return r.NullInt64Field(field).Int64
}

// NullInt64 returns the sql.NullInt64 value of the expression.
func (r *Row) NullInt64(format string, values ...any) sql.NullInt64 {
	return r.NullInt64Field(Expr(format, values...))
}

// NullInt64Field returns the sql.NullInt64 value of the field.
func (r *Row) NullInt64Field(field Number) sql.NullInt64 {
	if !r.active {
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &sql.NullInt64{})
		return sql.NullInt64{}
	}
	nullint64 := r.dest[r.index].(*sql.NullInt64)
	r.index++
	return *nullint64
}

// JSON scans the JSON expression into dest.
func (r *Row) JSON(dest any, format string, values ...any) {
	r.json(dest, Expr(format, values...), 1)
}

// JSONField scans the JSON field into dest.
func (r *Row) JSONField(dest any, field JSON) {
	r.json(dest, field, 1)
}

func (r *Row) json(dest any, field JSON, skip int) {
	if !r.active {
		if reflect.TypeOf(dest).Kind() != reflect.Ptr {
			panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
		}
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &[]byte{})
		r.destIsString[len(r.dest)-1] = struct{}{}
		return
	}
	bptr := r.dest[r.index].(*[]byte)
	if len(*bptr) > 0 {
		err := json.Unmarshal(*bptr, dest)
		if err != nil {
			_, file, line, _ := runtime.Caller(skip + 1)
			panic(fmt.Errorf("%s:%d ScanJSON unmarshaling json %s into %T: %w", file, line, string(*bptr), dest, err))
		}
	}
	r.index++
}

// String returns the string value of the expression.
func (r *Row) String(format string, values ...any) string {
	return r.NullStringField(Expr(format, values...)).String
}

// String returns the string value of the field.
func (r *Row) StringField(field String) string {
	return r.NullStringField(field).String
}

// NullString returns the sql.NullString value of the expression.
func (r *Row) NullString(format string, values ...any) sql.NullString {
	return r.NullStringField(Expr(format, values...))
}

// NullStringField returns the sql.NullString value of the field.
func (r *Row) NullStringField(field String) sql.NullString {
	if !r.active {
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &sql.NullString{})
		return sql.NullString{}
	}
	nullstring := r.dest[r.index].(*sql.NullString)
	r.index++
	return *nullstring
}

// Time returns the time.Time value of the expression.
func (r *Row) Time(format string, values ...any) time.Time {
	return r.NullTimeField(Expr(format, values...)).Time
}

// Time returns the time.Time value of the field.
func (r *Row) TimeField(field Time) time.Time {
	return r.NullTimeField(field).Time
}

// NullTime returns the sql.NullTime value of the expression.
func (r *Row) NullTime(format string, values ...any) sql.NullTime {
	return r.NullTimeField(Expr(format, values...))
}

// NullTimeField returns the sql.NullTime value of the field.
func (r *Row) NullTimeField(field Time) sql.NullTime {
	if !r.active {
		r.fields = append(r.fields, field)
		r.dest = append(r.dest, &sql.NullTime{})
		return sql.NullTime{}
	}
	nulltime := r.dest[r.index].(*sql.NullTime)
	r.index++
	return *nulltime
}

// UUID scans the UUID expression into dest.
func (r *Row) UUID(dest any, format string, values ...any) {
	r.uuid(dest, Expr(format, values...), 1)
}

// UUIDField scans the UUID field into dest.
func (r *Row) UUIDField(dest any, field UUID) {
	r.uuid(dest, field, 1)
}

func (r *Row) uuid(dest any, field UUID, skip int) {
	if !r.active {
		r.fields = append(r.fields, field)
		if reflect.TypeOf(dest).Kind() != reflect.Ptr {
			panic(fmt.Errorf("cannot pass in non pointer value (%#v) as dest", dest))
		}
		val := reflect.Indirect(reflect.ValueOf(dest))
		typ := val.Type()
		if val.Kind() != reflect.Array || val.Len() != 16 || typ.Elem().Kind() != reflect.Uint8 {
			panic(fmt.Errorf("dest %T is not a pointer to a [16]byte", dest))
		}
		r.dest = append(r.dest, &[]byte{})
		r.destIsUUID[len(r.dest)-1] = struct{}{}
		return
	}
	b := r.dest[r.index].(*[]byte)
	var err error
	var uuid [16]byte
	if len(*b) == 16 {
		copy(uuid[:], *b)
	} else {
		uuid, err = googleuuid.ParseBytes(*b)
		if err != nil {
			panic(err)
		}
	}
	val := reflect.Indirect(reflect.ValueOf(dest))
	for i := 0; i < 16; i++ {
		val.Index(i).Set(reflect.ValueOf(uuid[i]))
	}
	r.index++
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
		file, line, _ := caller(1)
		panic(fmt.Errorf("%s:%d: setting a nil field", filepath.Base(file), line))
	}
	// UPDATE mode
	if col.isUpdate {
		col.assignments = append(col.assignments, Set(field, value))
		return
	}
	// INSERT mode
	name := toString(col.dialect, field)
	if name == "" {
		file, line, _ := caller(1)
		panic(fmt.Errorf("%s:%d: field name is empty", filepath.Base(file), line))
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

// SetUUID maps the UUID value to the field, wrapped with the UUIDValue()
// constructor.
func (col *Column) SetUUID(field UUID, value any) { col.Set(field, UUIDValue(value)) }

// SetJSON maps the JSON value to the field, wrapped with the JSONValue()
// constructor.
func (col *Column) SetJSON(field JSON, value any) { col.Set(field, JSONValue(value)) }

// SetArray maps the array value to the field, wrapped with the ArrayValue()
// constructor.
func (col *Column) SetArray(field Array, value any) { col.Set(field, ArrayValue(value)) }

// SetEnum maps the enum value to the field, wrapped with the EnumValue()
// constructor.
func (col *Column) SetEnum(field Enum, value Enumeration) { col.Set(field, EnumValue(value)) }

func caller(skip int) (file string, line int, function string) {
	/* https://talks.godoc.org/github.com/davecheney/go-1.9-release-party/presentation.slide#20
	 * "Code that queries a single caller at a specific depth should use Caller
	 * rather than passing a slice of length 1 to Callers."
	 */
	// Skip two extra frames to account for this function and runtime.Caller
	// itself.
	// pc, file, line, _ := runtime.Caller(skip + 2) // don't know why this is borked
	pc, file, line, _ := runtime.Caller(skip + 1)
	fn := runtime.FuncForPC(pc)
	function = fn.Name()
	return file, line, function
}
