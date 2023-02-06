package sq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bokwoon95/sq/internal/googleuuid"
	"github.com/bokwoon95/sq/internal/pqarray"
)

// Identifier represents an SQL identifier. If necessary, it will be quoted
// according to the dialect.
type Identifier string

var _ Field = (*Identifier)(nil)

// WriteSQL implements the SQLWriter interface.
func (id Identifier) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString(QuoteIdentifier(dialect, string(id)))
	return nil
}

// IsField implements the Field interface.
func (id Identifier) IsField() {}

// AnyField is a catch-all field type that satisfies the Any interface.
type AnyField struct {
	table      TableStruct
	name       string
	alias      string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	Any
	WithPrefix(string) Field
} = (*AnyField)(nil)

// NewAnyField returns a new AnyField.
func NewAnyField(name string, tbl TableStruct) AnyField {
	return AnyField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f AnyField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new AnyField with the given alias.
func (f AnyField) As(alias string) AnyField {
	f.alias = alias
	return f
}

// Asc returns a new AnyField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (f AnyField) Asc() AnyField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new AnyField indicating that it should be ordered in descending
// order i.e. 'ORDER BY field DESC'.
func (f AnyField) Desc() AnyField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new NumberField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f AnyField) NullsLast() AnyField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new NumberField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f AnyField) NullsFirst() AnyField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f AnyField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f AnyField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f AnyField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// In returns a 'field IN (val)' Predicate.
func (f AnyField) In(val any) Predicate { return In(f, val) }

// Eq returns a 'field = val' Predicate.
func (f AnyField) Eq(val any) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate.
func (f AnyField) Ne(val any) Predicate { return Ne(f, val) }

// Lt returns a 'field < val' Predicate.
func (f AnyField) Lt(val any) Predicate { return Lt(f, val) }

// Le returns a 'field <= val' Predicate.
func (f AnyField) Le(val any) Predicate { return Le(f, val) }

// Gt returns a 'field > val' Predicate.
func (f AnyField) Gt(val any) Predicate { return Gt(f, val) }

// Ge returns a 'field >= val' Predicate.
func (f AnyField) Ge(val any) Predicate { return Ge(f, val) }

// Expr returns an expression where the field is prepended to the front of the
// expression.
func (f AnyField) Expr(format string, values ...any) Expression {
	values = append(values, f)
	ordinal := len(values)
	return Expr("{"+strconv.Itoa(ordinal)+"} "+format, values...)
}

// Set returns an Assignment assigning the val to the field.
func (f AnyField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f AnyField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// GetAlias returns the alias of the AnyField.
func (f AnyField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f AnyField) IsField() {}

// IsArray implements the Array interface.
func (f AnyField) IsArray() {}

// IsBinary implements the Binary interface.
func (f AnyField) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (f AnyField) IsBoolean() {}

// IsEnum implements the Enum interface.
func (f AnyField) IsEnum() {}

// IsJSON implements the JSONValue interface.
func (f AnyField) IsJSON() {}

// IsNumber implements the Number interface.
func (f AnyField) IsNumber() {}

// IsString implements the String interface.
func (f AnyField) IsString() {}

// IsTime implements the Time interface.
func (f AnyField) IsTime() {}

// IsUUIDType implements the UUID interface.
func (f AnyField) IsUUID() {}

// ArrayField represents an SQL array field.
type ArrayField struct {
	table TableStruct
	name  string
	alias string
}

var _ interface {
	Field
	Array
	WithPrefix(string) Field
} = (*ArrayField)(nil)

// NewArrayField returns a new ArrayField.
func NewArrayField(fieldName string, tableName TableStruct) ArrayField {
	return ArrayField{table: tableName, name: fieldName}
}

// WriteSQL implements the SQLWriter interface.
func (f ArrayField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	return nil
}

// As returns a new ArrayField with the given alias.
func (f ArrayField) As(alias string) ArrayField {
	f.alias = alias
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f ArrayField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f ArrayField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNull returns a 'field IS NOT NULL' Predicate.
func (f ArrayField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// Set returns an Assignment assigning the val to the field.
func (f ArrayField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f ArrayField) Setf(format string, values ...any) Assignment {
	return Set(f, Expr(format, values...))
}

// SetArray is like Set but it wraps val with ArrayValue().
func (f ArrayField) SetArray(val any) Assignment { return Set(f, ArrayValue(val)) }

// GetAlias returns the alias of the ArrayField.
func (f ArrayField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f ArrayField) IsField() {}

// IsArray implements the Array interface.
func (f ArrayField) IsArray() {}

type arrayValue struct {
	dialect string
	value   any
}

// ArrayValue takes in a []string, []int64, []int32, []float64, []float32 or
// []bool and returns a driver.Valuer for that type. For Postgres, it
// serializes into a Postgres array. Otherwise, it serializes into a JSON
// array.
func ArrayValue(value any) driver.Valuer { return &arrayValue{value: value} }

// Value implements the driver.Valuer interface.
func (v *arrayValue) Value() (driver.Value, error) {
	if v.dialect == DialectPostgres {
		return pqarray.Array(v.value).Value()
	}
	var b strings.Builder
	err := json.NewEncoder(&b).Encode(v.value)
	return b.String(), err
}

// DialectValuer implements the DialectValuer interface.
func (v *arrayValue) DialectValuer(dialect string) (driver.Valuer, error) {
	v.dialect = dialect
	return v, nil
}

// BinaryField represents an SQL binary field.
type BinaryField struct {
	table      TableStruct
	name       string
	alias      string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	Binary
	WithPrefix(string) Field
} = (*BinaryField)(nil)

// NewBinaryField returns a new BinaryField.
func NewBinaryField(fieldName string, tableName TableStruct) BinaryField {
	return BinaryField{table: tableName, name: fieldName}
}

// WriteSQL implements the SQLWriter interface.
func (f BinaryField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new BinaryField with the given alias.
func (f BinaryField) As(alias string) BinaryField {
	f.alias = alias
	return f
}

// Asc returns a new BinaryField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (f BinaryField) Asc() BinaryField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new BinaryField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (f BinaryField) Desc() BinaryField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new BinaryField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f BinaryField) NullsLast() BinaryField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new BinaryField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f BinaryField) NullsFirst() BinaryField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f BinaryField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f BinaryField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f BinaryField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// In returns a 'field IN (val)' Predicate.
func (f BinaryField) In(val any) Predicate { return In(f, val) }

// Eq returns a 'field = val' Predicate.
func (f BinaryField) Eq(val Binary) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate.
func (f BinaryField) Ne(val Binary) Predicate { return Ne(f, val) }

// EqBytes returns a 'field = b' Predicate.
func (f BinaryField) EqBytes(b []byte) Predicate { return Eq(f, b) }

// NeBytes returns a 'field <> b' Predicate.
func (f BinaryField) NeBytes(b []byte) Predicate { return Ne(f, b) }

// Set returns an Assignment assigning the val to the field.
func (f BinaryField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f BinaryField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetBytes returns an Assignment assigning a []byte to the field.
func (f BinaryField) SetBytes(b []byte) Assignment { return Set(f, b) }

// GetAlias returns the alias of the BinaryField.
func (f BinaryField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f BinaryField) IsField() {}

// IsBinary implements the Binary interface.
func (f BinaryField) IsBinary() {}

// BooleanField represents an SQL boolean field.
type BooleanField struct {
	table      TableStruct
	name       string
	alias      string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	Boolean
	Predicate
	WithPrefix(string) Field
} = (*BooleanField)(nil)

// NewBooleanField returns a new BooleanField.
func NewBooleanField(fieldName string, tableName TableStruct) BooleanField {
	return BooleanField{table: tableName, name: fieldName}
}

// WriteSQL implements the SQLWriter interface.
func (f BooleanField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new BooleanField with the given alias.
func (f BooleanField) As(alias string) BooleanField {
	f.alias = alias
	return f
}

// Asc returns a new BooleanField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (f BooleanField) Asc() BooleanField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new BooleanField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f BooleanField) Desc() BooleanField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new BooleanField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f BooleanField) NullsLast() BooleanField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new BooleanField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f BooleanField) NullsFirst() BooleanField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f BooleanField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f BooleanField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f BooleanField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// Eq returns a 'field = val' Predicate.
func (f BooleanField) Eq(val Boolean) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate.
func (f BooleanField) Ne(val Boolean) Predicate { return Ne(f, val) }

// EqBool returns a 'field = b' Predicate.
func (f BooleanField) EqBool(b bool) Predicate { return Eq(f, b) }

// NeBool returns a 'field <> b' Predicate.
func (f BooleanField) NeBool(b bool) Predicate { return Ne(f, b) }

// Set returns an Assignment assigning the val to the field.
func (f BooleanField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f BooleanField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetBool returns an Assignment assigning a bool to the field i.e. 'field =
// b'.
func (f BooleanField) SetBool(b bool) Assignment { return Set(f, b) }

// GetAlias returns the alias of the BooleanField.
func (f BooleanField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f BooleanField) IsField() {}

// IsBoolean implements the Boolean interface.
func (f BooleanField) IsBoolean() {}

// EnumField represents an SQL enum field.
type EnumField struct {
	table TableStruct
	name  string
	alias string
}

var _ interface {
	Field
	Enum
	WithPrefix(string) Field
} = (*EnumField)(nil)

// NewEnumField returns a new EnumField.
func NewEnumField(name string, tbl TableStruct) EnumField {
	return EnumField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f EnumField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	return nil
}

// As returns a new EnumField with the given alias.
func (f EnumField) As(alias string) EnumField {
	f.alias = alias
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f EnumField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f EnumField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f EnumField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// Eq returns a 'field = val' Predicate. The value is passed as-is to the
// database. If the value is an Enumeration type, you should be using EqEnum
// instead.
func (f EnumField) Eq(val any) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate. The value is passed as-is to the
// database. If the value is an Enumeration type, you should be using NeEnum
// instead.
func (f EnumField) Ne(val any) Predicate { return Ne(f, val) }

// EqEnum is like Eq but it wraps val with EnumValue().
func (f EnumField) EqEnum(val Enumeration) Predicate { return Eq(f, EnumValue(val)) }

// NeEnum  is like Ne but it wraps val with EnumValue().
func (f EnumField) NeEnum(val Enumeration) Predicate { return Ne(f, EnumValue(val)) }

// Set returns an Assignment assigning the val to the field.
func (f EnumField) Set(val any) Assignment { return Set(f, val) }

// SetEnum is like Set but wraps val with EnumValue().
func (f EnumField) SetEnum(val Enumeration) Assignment { return Set(f, EnumValue(val)) }

// Setf returns an Assignment assigning an expression to the field.
func (f EnumField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// GetAlias returns the alias of the EnumField.
func (f EnumField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f EnumField) IsField() {}

// IsEnum implements the Enum interface.
func (f EnumField) IsEnum() {}

type enumValue struct {
	value Enumeration
}

// EnumValue takes in an Enumeration and returns a driver.Valuer which
// serializes the enum into a string and additionally checks if the enum is
// valid.
func EnumValue(value Enumeration) driver.Valuer {
	return &enumValue{value: value}
}

// Value implements the driver.Valuer interface.
func (v *enumValue) Value() (driver.Value, error) {
	val := reflect.ValueOf(v.value)
	names := v.value.Enumerate()
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := int(val.Int())
		if i < 0 || i >= len(names) {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		name := names[i]
		if name == "" && i != 0 {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		return name, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i := int(val.Uint())
		if i < 0 || i >= len(names) {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		name := names[i]
		if name == "" && i != 0 {
			return nil, fmt.Errorf("%d is not a valid %T", i, v.value)
		}
		return name, nil
	case reflect.String:
		typ := val.Type()
		name := val.String()
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

// JSONField represents an SQL JSON field.
type JSONField struct {
	table TableStruct
	name  string
	alias string
}

var _ interface {
	Field
	JSON
	WithPrefix(string) Field
} = (*JSONField)(nil)

// NewJSONField returns a new JSONField.
func NewJSONField(name string, tbl TableStruct) JSONField {
	return JSONField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f JSONField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	return nil
}

// As returns a new JSONField with the given alias.
func (f JSONField) As(alias string) JSONField {
	f.alias = alias
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f JSONField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f JSONField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f JSONField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// Set returns an Assignment assigning the val to the field.
func (f JSONField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f JSONField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetJSON is like Set but it wraps val with JSONValue().
func (f JSONField) SetJSON(val any) Assignment { return Set(f, JSONValue(val)) }

// GetAlias returns the alias of the JSONField.
func (f JSONField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f JSONField) IsField() {}

// IsJSON implements the JSON interface.
func (f JSONField) IsJSON() {}

type jsonValue struct{ value any }

// JSONValue takes in an interface{} and returns a driver.Valuer which runs the
// value through json.Marshal before submitting it to the database.
func JSONValue(value any) driver.Valuer { return &jsonValue{value: value} }

// Value implements the driver.Valuer interface.
func (v *jsonValue) Value() (driver.Value, error) {
	var b strings.Builder
	err := json.NewEncoder(&b).Encode(v.value)
	return b.String(), err
}

// NumberField represents an SQL number field.
type NumberField struct {
	table      TableStruct
	name       string
	alias      string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	Number
	WithPrefix(string) Field
} = (*NumberField)(nil)

// NewNumberField returns a new NumberField.
func NewNumberField(name string, tbl TableStruct) NumberField {
	return NumberField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f NumberField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new NumberField with the given alias.
func (f NumberField) As(alias string) NumberField {
	f.alias = alias
	return f
}

// Asc returns a new NumberField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (f NumberField) Asc() NumberField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new NumberField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (f NumberField) Desc() NumberField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new NumberField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f NumberField) NullsLast() NumberField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new NumberField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f NumberField) NullsFirst() NumberField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f NumberField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f NumberField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f NumberField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// In returns a 'field IN (val)' Predicate.
func (f NumberField) In(val any) Predicate { return In(f, val) }

// Eq returns a 'field = val' Predicate.
func (f NumberField) Eq(val Number) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate.
func (f NumberField) Ne(val Number) Predicate { return Ne(f, val) }

// Lt returns a 'field < val' Predicate.
func (f NumberField) Lt(val Number) Predicate { return Lt(f, val) }

// Le returns a 'field <= val' Predicate.
func (f NumberField) Le(val Number) Predicate { return Le(f, val) }

// Gt returns a 'field > val' Predicate.
func (f NumberField) Gt(val Number) Predicate { return Gt(f, val) }

// Ge returns a 'field >= val' Predicate.
func (f NumberField) Ge(val Number) Predicate { return Ge(f, val) }

// EqInt returns a 'field = num' Predicate.
func (f NumberField) EqInt(num int) Predicate { return Eq(f, num) }

// NeInt returns a 'field <> num' Predicate.
func (f NumberField) NeInt(num int) Predicate { return Ne(f, num) }

// LtInt returns a 'field < num' Predicate.
func (f NumberField) LtInt(num int) Predicate { return Lt(f, num) }

// LeInt returns a 'field <= num' Predicate.
func (f NumberField) LeInt(num int) Predicate { return Le(f, num) }

// GtInt returns a 'field > num' Predicate.
func (f NumberField) GtInt(num int) Predicate { return Gt(f, num) }

// GeInt returns a 'field >= num' Predicate.
func (f NumberField) GeInt(num int) Predicate { return Ge(f, num) }

// EqInt64 returns a 'field = num' Predicate.
func (f NumberField) EqInt64(num int64) Predicate { return Eq(f, num) }

// NeInt64 returns a 'field <> num' Predicate.
func (f NumberField) NeInt64(num int64) Predicate { return Ne(f, num) }

// LtInt64 returns a 'field < num' Predicate.
func (f NumberField) LtInt64(num int64) Predicate { return Lt(f, num) }

// LeInt64 returns a 'field <= num' Predicate.
func (f NumberField) LeInt64(num int64) Predicate { return Le(f, num) }

// GtInt64 returns a 'field > num' Predicate.
func (f NumberField) GtInt64(num int64) Predicate { return Gt(f, num) }

// GeInt64 returns a 'field >= num' Predicate.
func (f NumberField) GeInt64(num int64) Predicate { return Ge(f, num) }

// EqFloat64 returns a 'field = num' Predicate.
func (f NumberField) EqFloat64(num float64) Predicate { return Eq(f, num) }

// NeFloat64 returns a 'field <> num' Predicate.
func (f NumberField) NeFloat64(num float64) Predicate { return Ne(f, num) }

// LtFloat64 returns a 'field < num' Predicate.
func (f NumberField) LtFloat64(num float64) Predicate { return Lt(f, num) }

// LeFloat64 returns a 'field <= num' Predicate.
func (f NumberField) LeFloat64(num float64) Predicate { return Le(f, num) }

// GtFloat64 returns a 'field > num' Predicate.
func (f NumberField) GtFloat64(num float64) Predicate { return Gt(f, num) }

// GeFloat64 returns a 'field >= num' Predicate.
func (f NumberField) GeFloat64(num float64) Predicate { return Ge(f, num) }

// Set returns an Assignment assigning the val to the field.
func (f NumberField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f NumberField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetBytes returns an Assignment assigning an int to the field.
func (f NumberField) SetInt(num int) Assignment { return Set(f, num) }

// SetBytes returns an Assignment assigning an int64 to the field.
func (f NumberField) SetInt64(num int64) Assignment { return Set(f, num) }

// SetBytes returns an Assignment assigning an float64 to the field.
func (f NumberField) SetFloat64(num float64) Assignment { return Set(f, num) }

// GetAlias returns the alias of the NumberField.
func (f NumberField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f NumberField) IsField() {}

// IsNumber implements the Number interface.
func (f NumberField) IsNumber() {}

// StringField represents an SQL string field.
type StringField struct {
	table      TableStruct
	name       string
	alias      string
	collation  string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	String
	WithPrefix(string) Field
} = (*StringField)(nil)

// NewStringField returns a new StringField.
func NewStringField(name string, tbl TableStruct) StringField {
	return StringField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f StringField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	if f.collation != "" {
		buf.WriteString(" COLLATE ")
		if dialect == DialectPostgres {
			buf.WriteString(`"` + EscapeQuote(f.collation, '"') + `"`)
		} else {
			buf.WriteString(QuoteIdentifier(dialect, f.collation))
		}
	}
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new StringField with the given alias.
func (f StringField) As(alias string) StringField {
	f.alias = alias
	return f
}

// Collate returns a new StringField using the given collation.
func (f StringField) Collate(collation string) StringField {
	f.collation = collation
	return f
}

// Asc returns a new StringField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (f StringField) Asc() StringField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new StringField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (f StringField) Desc() StringField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new StringField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f StringField) NullsLast() StringField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new StringField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f StringField) NullsFirst() StringField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f StringField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f StringField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f StringField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// In returns a 'field IN (val)' Predicate.
func (f StringField) In(val any) Predicate { return In(f, val) }

// Eq returns a 'field = val' Predicate.
func (f StringField) Eq(val String) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate.
func (f StringField) Ne(val String) Predicate { return Ne(f, val) }

// Lt returns a 'field < val' Predicate.
func (f StringField) Lt(val String) Predicate { return Lt(f, val) }

// Le returns a 'field <= val' Predicate.
func (f StringField) Le(val String) Predicate { return Le(f, val) }

// Gt returns a 'field > val' Predicate.
func (f StringField) Gt(val String) Predicate { return Gt(f, val) }

// Ge returns a 'field >= val' Predicate.
func (f StringField) Ge(val String) Predicate { return Ge(f, val) }

// EqString returns a 'field = s' Predicate.
func (f StringField) EqString(s string) Predicate { return Eq(f, s) }

// NeString returns a 'field <> s' Predicate.
func (f StringField) NeString(s string) Predicate { return Ne(f, s) }

// LtString returns a 'field < s' Predicate.
func (f StringField) LtString(s string) Predicate { return Lt(f, s) }

// LeString returns a 'field <= s' Predicate.
func (f StringField) LeString(s string) Predicate { return Le(f, s) }

// GtString returns a 'field > s' Predicate.
func (f StringField) GtString(s string) Predicate { return Gt(f, s) }

// GeString returns a 'field >= s' Predicate.
func (f StringField) GeString(s string) Predicate { return Ge(f, s) }

// LikeString returns a 'field LIKE s' Predicate.
func (f StringField) LikeString(s string) Predicate { return Expr("{} LIKE {}", f, s) }

// LikeString returns a 'field ILIKE s' Predicate.
func (f StringField) ILikeString(s string) Predicate { return Expr("{} ILIKE {}", f, s) }

// Set returns an Assignment assigning the val to the field.
func (f StringField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f StringField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetBytes returns an Assignment assigning a string to the field.
func (f StringField) SetString(s string) Assignment { return Set(f, s) }

// GetAlias returns the alias of the StringField.
func (f StringField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f StringField) IsField() {}

// IsString implements the String interface.
func (f StringField) IsString() {}

// TimeField represents an SQL time field.
type TimeField struct {
	table      TableStruct
	name       string
	alias      string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	Time
	WithPrefix(string) Field
} = (*TimeField)(nil)

// NewTimeField returns a new TimeField.
func NewTimeField(name string, tbl TableStruct) TimeField {
	return TimeField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f TimeField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new TimeField with the given alias.
func (f TimeField) As(alias string) TimeField {
	f.alias = alias
	return f
}

// Asc returns a new TimeField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (f TimeField) Asc() TimeField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new TimeField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (f TimeField) Desc() TimeField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new TimeField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f TimeField) NullsLast() TimeField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new TimeField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f TimeField) NullsFirst() TimeField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f TimeField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f TimeField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f TimeField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// In returns a 'field IN (val)' Predicate.
func (f TimeField) In(val any) Predicate { return In(f, val) }

// Eq returns a 'field = val' Predicate.
func (f TimeField) Eq(val Time) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate.
func (f TimeField) Ne(val Time) Predicate { return Ne(f, val) }

// Lt returns a 'field < val' Predicate.
func (f TimeField) Lt(val Time) Predicate { return Lt(f, val) }

// Le returns a 'field <= val' Predicate.
func (f TimeField) Le(val Time) Predicate { return Le(f, val) }

// Gt returns a 'field > val' Predicate.
func (f TimeField) Gt(val Time) Predicate { return Gt(f, val) }

// Ge returns a 'field >= val' Predicate.
func (f TimeField) Ge(val Time) Predicate { return Ge(f, val) }

// EqTime returns a 'field = t' Predicate.
func (f TimeField) EqTime(t time.Time) Predicate { return Eq(f, t) }

// NeTime returns a 'field <> t' Predicate.
func (f TimeField) NeTime(t time.Time) Predicate { return Ne(f, t) }

// LtTime returns a 'field < t' Predicate.
func (f TimeField) LtTime(t time.Time) Predicate { return Lt(f, t) }

// LeTime returns a 'field <= t' Predicate.
func (f TimeField) LeTime(t time.Time) Predicate { return Le(f, t) }

// GtTime returns a 'field > t' Predicate.
func (f TimeField) GtTime(t time.Time) Predicate { return Gt(f, t) }

// GeTime returns a 'field >= t' Predicate.
func (f TimeField) GeTime(t time.Time) Predicate { return Ge(f, t) }

// Set returns an Assignment assigning the val to the field.
func (f TimeField) Set(val any) Assignment { return Set(f, val) }

// Setf returns an Assignment assigning an expression to the field.
func (f TimeField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetTime returns an Assignment assigning a time.Time to the field.
func (f TimeField) SetTime(t time.Time) Assignment { return Set(f, t) }

// SetTimestamp returns an Assignment assigning a Timestamp to the field.
func (f TimeField) SetTimestamp(t Timestamp) Assignment { return Set(f, t) }

// GetAlias returns the alias of the TimeField.
func (f TimeField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f TimeField) IsField() {}

// IsTime implements the Time interface.
func (f TimeField) IsTime() {}

// Timestamp is like sql.NullTime but implements the DialectValuer interface.
// When the dialect is SQLite, Timestamp will render itself an an int64 unix
// time. Otherwise, it behaves similarly to an sql.NullTime.
type Timestamp struct {
	time.Time
	Valid   bool
	dialect string
}

// NewTimestamp creates a new Timestamp from a time.Time.
func NewTimestamp(t time.Time) Timestamp {
	return Timestamp{Time: t, Valid: true}
}

// copied from https://pkg.go.dev/github.com/mattn/go-sqlite3#pkg-variables
var sqliteTimestampFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// Scan implements the sql.Scanner interface.
func (ts *Timestamp) Scan(value any) error {
	if value == nil {
		ts.Time, ts.Valid = time.Time{}, false
		return nil
	}
	// int64 and string handling copied from
	// https://github.com/mattn/go-sqlite3/issues/748#issuecomment-538643131
	switch value := value.(type) {
	case int64:
		// Assume a millisecond unix timestamp if it's 13 digits -- too
		// large to be a reasonable timestamp in seconds.
		if value > 1e12 || value < -1e12 {
			value *= int64(time.Millisecond) // convert ms to nsec
			ts.Time = time.Unix(0, value)
		} else {
			ts.Time = time.Unix(value, 0)
		}
		ts.Valid = true
		return nil
	case string:
		if len(value) == 0 {
			ts.Time, ts.Valid = time.Time{}, false
			return nil
		}
		var err error
		var timeVal time.Time
		value = strings.TrimSuffix(value, "Z")
		for _, format := range sqliteTimestampFormats {
			if timeVal, err = time.ParseInLocation(format, value, time.UTC); err == nil {
				ts.Time, ts.Valid = timeVal, true
				return nil
			}
		}
		return fmt.Errorf("could not convert %q into time", value)
	case []byte:
		if len(value) == 0 {
			ts.Time, ts.Valid = time.Time{}, false
			return nil
		}
		var err error
		var timeVal time.Time
		value = bytes.TrimSuffix(value, []byte("Z"))
		for _, format := range sqliteTimestampFormats {
			if timeVal, err = time.ParseInLocation(format, string(value), time.UTC); err == nil {
				ts.Time, ts.Valid = timeVal, true
				return nil
			}
		}
		return fmt.Errorf("could not convert %q into time", value)
	default:
		var nulltime sql.NullTime
		err := nulltime.Scan(value)
		if err != nil {
			return err
		}
		ts.Time, ts.Valid = nulltime.Time, nulltime.Valid
		return nil
	}
}

// Value implements the driver.Valuer interface.
func (ts Timestamp) Value() (driver.Value, error) {
	if !ts.Valid {
		return nil, nil
	}
	if ts.dialect == DialectSQLite {
		return ts.Time.UTC().Unix(), nil
	}
	return ts.Time, nil
}

// DialectValuer implements the DialectValuer interface.
func (ts Timestamp) DialectValuer(dialect string) (driver.Valuer, error) {
	ts.dialect = dialect
	return ts, nil
}

// UUIDField represents an SQL UUID field.
type UUIDField struct {
	table      TableStruct
	name       string
	alias      string
	desc       sql.NullBool
	nullsfirst sql.NullBool
}

var _ interface {
	Field
	UUID
	WithPrefix(string) Field
} = (*UUIDField)(nil)

// NewUUIDField returns a new UUIDField.
func NewUUIDField(name string, tbl TableStruct) UUIDField {
	return UUIDField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (f UUIDField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, f.table, f.name)
	writeFieldOrder(ctx, dialect, buf, args, params, f.desc, f.nullsfirst)
	return nil
}

// As returns a new UUIDField with the given alias.
func (f UUIDField) As(alias string) UUIDField {
	f.alias = alias
	return f
}

// Asc returns a new UUIDField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (f UUIDField) Asc() UUIDField {
	f.desc.Valid = true
	f.desc.Bool = false
	return f
}

// Desc returns a new UUIDField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (f UUIDField) Desc() UUIDField {
	f.desc.Valid = true
	f.desc.Bool = true
	return f
}

// NullsLast returns a new UUIDField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (f UUIDField) NullsLast() UUIDField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = false
	return f
}

// NullsFirst returns a new UUIDField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (f UUIDField) NullsFirst() UUIDField {
	f.nullsfirst.Valid = true
	f.nullsfirst.Bool = true
	return f
}

// WithPrefix returns a new Field that with the given prefix.
func (f UUIDField) WithPrefix(prefix string) Field {
	f.table.alias = ""
	f.table.name = prefix
	return f
}

// IsNull returns a 'field IS NULL' Predicate.
func (f UUIDField) IsNull() Predicate { return Expr("{} IS NULL", f) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (f UUIDField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", f) }

// In returns a 'field IN (val)' Predicate.
func (f UUIDField) In(val any) Predicate { return In(f, val) }

// Eq returns a 'field = val' Predicate. The value is passed as-is to the
// database.
func (f UUIDField) Eq(val any) Predicate { return Eq(f, val) }

// Ne returns a 'field <> val' Predicate. The value is passed as-is to the
// database.
func (f UUIDField) Ne(val any) Predicate { return Ne(f, val) }

// EqUUID is like Eq but it wraps val in UUIDValue().
func (f UUIDField) EqUUID(val any) Predicate { return Eq(f, UUIDValue(val)) }

// NeUUID is like Ne but it wraps val in UUIDValue().
func (f UUIDField) NeUUID(val any) Predicate { return Ne(f, UUIDValue(val)) }

// Set returns an Assignment assigning the val to the field.
func (f UUIDField) Set(val any) Assignment { return Set(f, val) }

// Set returns an Assignment assigning the val to the field.
func (f UUIDField) Setf(format string, values ...any) Assignment {
	return Setf(f, format, values...)
}

// SetUUID is like Set but it wraps val with the UUIDValue() constructor.
func (f UUIDField) SetUUID(val any) Assignment { return Set(f, UUIDValue(val)) }

// GetAlias returns the alias of the UUIDField.
func (f UUIDField) GetAlias() string { return f.alias }

// IsField implements the Field interface.
func (f UUIDField) IsField() {}

// IsUUID implements the UUID interface.
func (f UUIDField) IsUUID() {}

type uuidValue struct {
	dialect string
	value   any
}

// UUIDValue takes in a type whose underlying type must be a [16]byte and
// returns a driver.Valuer.
func UUIDValue(value any) driver.Valuer { return &uuidValue{value: value} }

// Value implements the driver.Valuer interface.
func (v *uuidValue) Value() (driver.Value, error) {
	val := reflect.ValueOf(v.value)
	typ := val.Type()
	if val.Kind() != reflect.Array || val.Len() != 16 || typ.Elem().Kind() != reflect.Uint8 {
		return nil, fmt.Errorf("%[1]v %[1]T is not [16]byte", v.value)
	}
	var uuid [16]byte
	for i := 0; i < val.Len(); i++ {
		uuid[i] = val.Index(i).Interface().(byte)
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

// New instantiates a new table struct with the given alias. Passing in an
// empty string is equivalent to giving no alias to the table.
func New[T Table](alias string) T {
	var tbl T
	ptrvalue := reflect.ValueOf(&tbl)
	value := reflect.Indirect(ptrvalue)
	typ := value.Type()
	if typ.Kind() != reflect.Struct {
		return tbl
	}
	if value.NumField() == 0 {
		return tbl
	}
	firstfield := value.Field(0)
	firstfieldType := typ.Field(0)
	if !firstfield.CanInterface() {
		return tbl
	}
	_, ok := firstfield.Interface().(TableStruct)
	if !ok {
		return tbl
	}
	if !firstfield.CanSet() {
		return tbl
	}
	var tableSchema, tableName string
	tag := firstfieldType.Tag.Get("sq")
	if i := strings.IndexByte(tag, '.'); i >= 0 {
		tableSchema = tag[:i]
		tableName = tag[i+1:]
	} else {
		tableName = tag
	}
	if tableName == "" {
		tableName = strings.ToLower(typ.Name())
	}
	tableStruct := NewTableStruct(tableSchema, tableName, alias)
	firstfield.Set(reflect.ValueOf(tableStruct))
	for i := 1; i < value.NumField(); i++ {
		v := value.Field(i)
		if !v.CanInterface() {
			continue
		}
		if !v.CanSet() {
			continue
		}
		fieldType := typ.Field(i)
		name := fieldType.Tag.Get("sq")
		if name == "" {
			name = strings.ToLower(fieldType.Name)
		}
		switch v.Interface().(type) {
		case AnyField:
			v.Set(reflect.ValueOf(NewAnyField(name, tableStruct)))
		case ArrayField:
			v.Set(reflect.ValueOf(NewArrayField(name, tableStruct)))
		case BinaryField:
			v.Set(reflect.ValueOf(NewBinaryField(name, tableStruct)))
		case BooleanField:
			v.Set(reflect.ValueOf(NewBooleanField(name, tableStruct)))
		case EnumField:
			v.Set(reflect.ValueOf(NewEnumField(name, tableStruct)))
		case JSONField:
			v.Set(reflect.ValueOf(NewJSONField(name, tableStruct)))
		case NumberField:
			v.Set(reflect.ValueOf(NewNumberField(name, tableStruct)))
		case StringField:
			v.Set(reflect.ValueOf(NewStringField(name, tableStruct)))
		case TimeField:
			v.Set(reflect.ValueOf(NewTimeField(name, tableStruct)))
		case UUIDField:
			v.Set(reflect.ValueOf(NewUUIDField(name, tableStruct)))
		}
	}
	return tbl
}

func writeFieldIdentifier(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, table TableStruct, fieldName string) {
	tableQualifier, _, _ := strings.Cut(table.alias, "(")
	tableQualifier = strings.TrimRight(tableQualifier, " ")
	if tableQualifier == "" {
		tableQualifier = table.name
	}
	if tableQualifier != "" {
		buf.WriteString(QuoteIdentifier(dialect, tableQualifier) + ".")
	}
	buf.WriteString(QuoteIdentifier(dialect, fieldName))
}

func writeFieldOrder(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, desc, nullsfirst sql.NullBool) {
	if desc.Valid {
		if desc.Bool {
			buf.WriteString(" DESC")
		} else {
			buf.WriteString(" ASC")
		}
	}
	if nullsfirst.Valid {
		if nullsfirst.Bool {
			buf.WriteString(" NULLS FIRST")
		} else {
			buf.WriteString(" NULLS LAST")
		}
	}
}
