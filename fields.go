package sq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
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
func (field AnyField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new AnyField with the given alias.
func (field AnyField) As(alias string) AnyField {
	field.alias = alias
	return field
}

// Asc returns a new AnyField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (field AnyField) Asc() AnyField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
}

// Desc returns a new AnyField indicating that it should be ordered in descending
// order i.e. 'ORDER BY field DESC'.
func (field AnyField) Desc() AnyField {
	field.desc.Valid = true
	field.desc.Bool = true
	return field
}

// NullsLast returns a new NumberField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (field AnyField) NullsLast() AnyField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new NumberField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field AnyField) NullsFirst() AnyField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field AnyField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field AnyField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field AnyField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// In returns a 'field IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field IN (x, y, z)'.
func (field AnyField) In(value any) Predicate { return In(field, value) }

// In returns a 'field NOT IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field NOT IN (x, y, z)'.
func (field AnyField) NotIn(value any) Predicate { return NotIn(field, value) }

// Eq returns a 'field = value' Predicate.
func (field AnyField) Eq(value any) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field AnyField) Ne(value any) Predicate { return Ne(field, value) }

// Lt returns a 'field < value' Predicate.
func (field AnyField) Lt(value any) Predicate { return Lt(field, value) }

// Le returns a 'field <= value' Predicate.
func (field AnyField) Le(value any) Predicate { return Le(field, value) }

// Gt returns a 'field > value' Predicate.
func (field AnyField) Gt(value any) Predicate { return Gt(field, value) }

// Ge returns a 'field >= value' Predicate.
func (field AnyField) Ge(value any) Predicate { return Ge(field, value) }

// Expr returns an expression where the field is prepended to the front of the
// expression.
func (field AnyField) Expr(format string, values ...any) Expression {
	values = append(values, field)
	ordinal := len(values)
	return Expr("{"+strconv.Itoa(ordinal)+"} "+format, values...)
}

// Set returns an Assignment assigning the value to the field.
func (field AnyField) Set(value any) Assignment {
	return Set(field, value)
}

// Setf returns an Assignment assigning an expression to the field.
func (field AnyField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// GetAlias returns the alias of the AnyField.
func (field AnyField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field AnyField) IsField() {}

// IsArray implements the Array interface.
func (field AnyField) IsArray() {}

// IsBinary implements the Binary interface.
func (field AnyField) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (field AnyField) IsBoolean() {}

// IsEnum implements the Enum interface.
func (field AnyField) IsEnum() {}

// IsJSON implements the JSONValue interface.
func (field AnyField) IsJSON() {}

// IsNumber implements the Number interface.
func (field AnyField) IsNumber() {}

// IsString implements the String interface.
func (field AnyField) IsString() {}

// IsTime implements the Time interface.
func (field AnyField) IsTime() {}

// IsUUIDType implements the UUID interface.
func (field AnyField) IsUUID() {}

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
func (field ArrayField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	return nil
}

// As returns a new ArrayField with the given alias.
func (field ArrayField) As(alias string) ArrayField {
	field.alias = alias
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field ArrayField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field ArrayField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNull returns a 'field IS NOT NULL' Predicate.
func (field ArrayField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// Set returns an Assignment assigning the value to the field.
func (field ArrayField) Set(value any) Assignment {
	switch value.(type) {
	case SQLWriter:
		return Set(field, value)
	case []string, []int, []int64, []int32, []float64, []float32, []bool:
		return Set(field, ArrayValue(value))
	}
	return Set(field, value)
}

// SetArray returns an Assignment assigning the value to the field. It wraps
// the value with ArrayValue().
func (field ArrayField) SetArray(value any) Assignment {
	return Set(field, ArrayValue(value))
}

// Setf returns an Assignment assigning an expression to the field.
func (field ArrayField) Setf(format string, values ...any) Assignment {
	return Set(field, Expr(format, values...))
}

// GetAlias returns the alias of the ArrayField.
func (field ArrayField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field ArrayField) IsField() {}

// IsArray implements the Array interface.
func (field ArrayField) IsArray() {}

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
func (field BinaryField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new BinaryField with the given alias.
func (field BinaryField) As(alias string) BinaryField {
	field.alias = alias
	return field
}

// Asc returns a new BinaryField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (field BinaryField) Asc() BinaryField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
}

// Desc returns a new BinaryField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (field BinaryField) Desc() BinaryField {
	field.desc.Valid = true
	field.desc.Bool = true
	return field
}

// NullsLast returns a new BinaryField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (field BinaryField) NullsLast() BinaryField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new BinaryField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field BinaryField) NullsFirst() BinaryField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field BinaryField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field BinaryField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field BinaryField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// Eq returns a 'field = value' Predicate.
func (field BinaryField) Eq(value Binary) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field BinaryField) Ne(value Binary) Predicate { return Ne(field, value) }

// EqBytes returns a 'field = b' Predicate.
func (field BinaryField) EqBytes(b []byte) Predicate { return Eq(field, b) }

// NeBytes returns a 'field <> b' Predicate.
func (field BinaryField) NeBytes(b []byte) Predicate { return Ne(field, b) }

// Set returns an Assignment assigning the value to the field.
func (field BinaryField) Set(value any) Assignment {
	return Set(field, value)
}

// Setf returns an Assignment assigning an expression to the field.
func (field BinaryField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// SetBytes returns an Assignment assigning a []byte to the field.
func (field BinaryField) SetBytes(b []byte) Assignment { return Set(field, b) }

// GetAlias returns the alias of the BinaryField.
func (field BinaryField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field BinaryField) IsField() {}

// IsBinary implements the Binary interface.
func (field BinaryField) IsBinary() {}

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
func (field BooleanField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new BooleanField with the given alias.
func (field BooleanField) As(alias string) BooleanField {
	field.alias = alias
	return field
}

// Asc returns a new BooleanField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (field BooleanField) Asc() BooleanField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
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
func (field BooleanField) NullsLast() BooleanField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new BooleanField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field BooleanField) NullsFirst() BooleanField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field BooleanField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field BooleanField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field BooleanField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// Eq returns a 'field = value' Predicate.
func (field BooleanField) Eq(value Boolean) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field BooleanField) Ne(value Boolean) Predicate { return Ne(field, value) }

// EqBool returns a 'field = b' Predicate.
func (field BooleanField) EqBool(b bool) Predicate { return Eq(field, b) }

// NeBool returns a 'field <> b' Predicate.
func (field BooleanField) NeBool(b bool) Predicate { return Ne(field, b) }

// Set returns an Assignment assigning the value to the field.
func (field BooleanField) Set(value any) Assignment {
	return Set(field, value)
}

// Setf returns an Assignment assigning an expression to the field.
func (field BooleanField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// SetBool returns an Assignment assigning a bool to the field i.e. 'field =
// b'.
func (field BooleanField) SetBool(b bool) Assignment { return Set(field, b) }

// GetAlias returns the alias of the BooleanField.
func (field BooleanField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field BooleanField) IsField() {}

// IsBoolean implements the Boolean interface.
func (field BooleanField) IsBoolean() {}

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
func (field EnumField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	return nil
}

// As returns a new EnumField with the given alias.
func (field EnumField) As(alias string) EnumField {
	field.alias = alias
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field EnumField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field EnumField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field EnumField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// In returns a 'field IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field IN (x, y, z)'.
func (field EnumField) In(value any) Predicate { return In(field, value) }

// NotIn returns a 'field NOT IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field NOT IN (x, y, z)'.
func (field EnumField) NotIn(value any) Predicate { return NotIn(field, value) }

// Eq returns a 'field = value' Predicate.
func (field EnumField) Eq(value any) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field EnumField) Ne(value any) Predicate { return Ne(field, value) }

// EqEnum returns a 'field = value' Predicate. It wraps the value with
// EnumValue().
func (field EnumField) EqEnum(value Enumeration) Predicate { return Eq(field, EnumValue(value)) }

// NeEnum returns a 'field <> value' Predicate. it wraps the value with
// EnumValue().
func (field EnumField) NeEnum(value Enumeration) Predicate { return Ne(field, EnumValue(value)) }

// Set returns an Assignment assigning the value to the field.
func (field EnumField) Set(value any) Assignment {
	return Set(field, value)
}

// SetEnum returns an Assignment assigning the value to the field. It wraps the
// value with EnumValue().
func (field EnumField) SetEnum(value Enumeration) Assignment {
	return Set(field, EnumValue(value))
}

// Setf returns an Assignment assigning an expression to the field.
func (field EnumField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// GetAlias returns the alias of the EnumField.
func (field EnumField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field EnumField) IsField() {}

// IsEnum implements the Enum interface.
func (field EnumField) IsEnum() {}

// JSONField represents an SQL JSON field.
type JSONField struct {
	table TableStruct
	name  string
	alias string
}

var _ interface {
	Field
	Binary
	JSON
	String
	WithPrefix(string) Field
} = (*JSONField)(nil)

// NewJSONField returns a new JSONField.
func NewJSONField(name string, tbl TableStruct) JSONField {
	return JSONField{table: tbl, name: name}
}

// WriteSQL implements the SQLWriter interface.
func (field JSONField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	return nil
}

// As returns a new JSONField with the given alias.
func (field JSONField) As(alias string) JSONField {
	field.alias = alias
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field JSONField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field JSONField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field JSONField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// Set returns an Assignment assigning the value to the field.
func (field JSONField) Set(value any) Assignment {
	switch value.(type) {
	case []byte, driver.Valuer, SQLWriter:
		return Set(field, value)
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Map, reflect.Struct, reflect.Slice, reflect.Array:
		return Set(field, JSONValue(value))
	}
	return Set(field, value)
}

// SetJSON returns an Assignment assigning the value to the field. It wraps the
// value in JSONValue().
func (field JSONField) SetJSON(value any) Assignment {
	return Set(field, JSONValue(value))
}

// Setf returns an Assignment assigning an expression to the field.
func (field JSONField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// GetAlias returns the alias of the JSONField.
func (field JSONField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field JSONField) IsField() {}

// IsBinary implements the Binary interface.
func (field JSONField) IsBinary() {}

// IsJSON implements the JSON interface.
func (field JSONField) IsJSON() {}

// IsString implements the String interface.
func (field JSONField) IsString() {}

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
func (field NumberField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new NumberField with the given alias.
func (field NumberField) As(alias string) NumberField {
	field.alias = alias
	return field
}

// Asc returns a new NumberField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (field NumberField) Asc() NumberField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
}

// Desc returns a new NumberField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (field NumberField) Desc() NumberField {
	field.desc.Valid = true
	field.desc.Bool = true
	return field
}

// NullsLast returns a new NumberField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (field NumberField) NullsLast() NumberField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new NumberField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field NumberField) NullsFirst() NumberField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field NumberField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field NumberField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field NumberField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// In returns a 'field IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field IN (x, y, z)'.
func (field NumberField) In(value any) Predicate { return In(field, value) }

// NotIn returns a 'field NOT IN (value)' Predicate. The value can be a slice,
// which corresponds to the expression 'field IN (x, y, z)'.
func (field NumberField) NotIn(value any) Predicate { return NotIn(field, value) }

// Eq returns a 'field = value' Predicate.
func (field NumberField) Eq(value Number) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field NumberField) Ne(value Number) Predicate { return Ne(field, value) }

// Lt returns a 'field < value' Predicate.
func (field NumberField) Lt(value Number) Predicate { return Lt(field, value) }

// Le returns a 'field <= value' Predicate.
func (field NumberField) Le(value Number) Predicate { return Le(field, value) }

// Gt returns a 'field > value' Predicate.
func (field NumberField) Gt(value Number) Predicate { return Gt(field, value) }

// Ge returns a 'field >= value' Predicate.
func (field NumberField) Ge(value Number) Predicate { return Ge(field, value) }

// EqInt returns a 'field = num' Predicate.
func (field NumberField) EqInt(num int) Predicate { return Eq(field, num) }

// NeInt returns a 'field <> num' Predicate.
func (field NumberField) NeInt(num int) Predicate { return Ne(field, num) }

// LtInt returns a 'field < num' Predicate.
func (field NumberField) LtInt(num int) Predicate { return Lt(field, num) }

// LeInt returns a 'field <= num' Predicate.
func (field NumberField) LeInt(num int) Predicate { return Le(field, num) }

// GtInt returns a 'field > num' Predicate.
func (field NumberField) GtInt(num int) Predicate { return Gt(field, num) }

// GeInt returns a 'field >= num' Predicate.
func (field NumberField) GeInt(num int) Predicate { return Ge(field, num) }

// EqInt64 returns a 'field = num' Predicate.
func (field NumberField) EqInt64(num int64) Predicate { return Eq(field, num) }

// NeInt64 returns a 'field <> num' Predicate.
func (field NumberField) NeInt64(num int64) Predicate { return Ne(field, num) }

// LtInt64 returns a 'field < num' Predicate.
func (field NumberField) LtInt64(num int64) Predicate { return Lt(field, num) }

// LeInt64 returns a 'field <= num' Predicate.
func (field NumberField) LeInt64(num int64) Predicate { return Le(field, num) }

// GtInt64 returns a 'field > num' Predicate.
func (field NumberField) GtInt64(num int64) Predicate { return Gt(field, num) }

// GeInt64 returns a 'field >= num' Predicate.
func (field NumberField) GeInt64(num int64) Predicate { return Ge(field, num) }

// EqFloat64 returns a 'field = num' Predicate.
func (field NumberField) EqFloat64(num float64) Predicate { return Eq(field, num) }

// NeFloat64 returns a 'field <> num' Predicate.
func (field NumberField) NeFloat64(num float64) Predicate { return Ne(field, num) }

// LtFloat64 returns a 'field < num' Predicate.
func (field NumberField) LtFloat64(num float64) Predicate { return Lt(field, num) }

// LeFloat64 returns a 'field <= num' Predicate.
func (field NumberField) LeFloat64(num float64) Predicate { return Le(field, num) }

// GtFloat64 returns a 'field > num' Predicate.
func (field NumberField) GtFloat64(num float64) Predicate { return Gt(field, num) }

// GeFloat64 returns a 'field >= num' Predicate.
func (field NumberField) GeFloat64(num float64) Predicate { return Ge(field, num) }

// Set returns an Assignment assigning the value to the field.
func (field NumberField) Set(value any) Assignment {
	return Set(field, value)
}

// Setf returns an Assignment assigning an expression to the field.
func (field NumberField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// SetBytes returns an Assignment assigning an int to the field.
func (field NumberField) SetInt(num int) Assignment { return Set(field, num) }

// SetBytes returns an Assignment assigning an int64 to the field.
func (field NumberField) SetInt64(num int64) Assignment { return Set(field, num) }

// SetBytes returns an Assignment assigning an float64 to the field.
func (field NumberField) SetFloat64(num float64) Assignment { return Set(field, num) }

// GetAlias returns the alias of the NumberField.
func (field NumberField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field NumberField) IsField() {}

// IsNumber implements the Number interface.
func (field NumberField) IsNumber() {}

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
func (field StringField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	if field.collation != "" {
		buf.WriteString(" COLLATE ")
		if dialect == DialectPostgres {
			buf.WriteString(`"` + EscapeQuote(field.collation, '"') + `"`)
		} else {
			buf.WriteString(QuoteIdentifier(dialect, field.collation))
		}
	}
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new StringField with the given alias.
func (field StringField) As(alias string) StringField {
	field.alias = alias
	return field
}

// Collate returns a new StringField using the given collation.
func (field StringField) Collate(collation string) StringField {
	field.collation = collation
	return field
}

// Asc returns a new StringField indicating that it should be ordered in
// ascending order i.e. 'ORDER BY field ASC'.
func (field StringField) Asc() StringField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
}

// Desc returns a new StringField indicating that it should be ordered in
// descending order i.e. 'ORDER BY field DESC'.
func (field StringField) Desc() StringField {
	field.desc.Valid = true
	field.desc.Bool = true
	return field
}

// NullsLast returns a new StringField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (field StringField) NullsLast() StringField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new StringField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field StringField) NullsFirst() StringField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field StringField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field StringField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field StringField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// In returns a 'field IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field IN (x, y, z)'.
func (field StringField) In(value any) Predicate { return In(field, value) }

// In returns a 'field NOT IN (value)' Predicate. The value can be a slice,
// which corresponds to the expression 'field NOT IN (x, y, z)'.
func (field StringField) NotIn(value any) Predicate { return NotIn(field, value) }

// Eq returns a 'field = value' Predicate.
func (field StringField) Eq(value String) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field StringField) Ne(value String) Predicate { return Ne(field, value) }

// Lt returns a 'field < value' Predicate.
func (field StringField) Lt(value String) Predicate { return Lt(field, value) }

// Le returns a 'field <= value' Predicate.
func (field StringField) Le(value String) Predicate { return Le(field, value) }

// Gt returns a 'field > value' Predicate.
func (field StringField) Gt(value String) Predicate { return Gt(field, value) }

// Ge returns a 'field >= value' Predicate.
func (field StringField) Ge(value String) Predicate { return Ge(field, value) }

// EqString returns a 'field = str' Predicate.
func (field StringField) EqString(str string) Predicate { return Eq(field, str) }

// NeString returns a 'field <> str' Predicate.
func (field StringField) NeString(str string) Predicate { return Ne(field, str) }

// LtString returns a 'field < str' Predicate.
func (field StringField) LtString(str string) Predicate { return Lt(field, str) }

// LeString returns a 'field <= str' Predicate.
func (field StringField) LeString(str string) Predicate { return Le(field, str) }

// GtString returns a 'field > str' Predicate.
func (field StringField) GtString(str string) Predicate { return Gt(field, str) }

// GeString returns a 'field >= str' Predicate.
func (field StringField) GeString(str string) Predicate { return Ge(field, str) }

// LikeString returns a 'field LIKE str' Predicate.
func (field StringField) LikeString(str string) Predicate {
	return Expr("{} LIKE {}", field, str)
}

// NotLikeString returns a 'field NOT LIKE str' Predicate.
func (field StringField) NotLikeString(str string) Predicate {
	return Expr("{} NOT LIKE {}", field, str)
}

// ILikeString returns a 'field ILIKE str' Predicate.
func (field StringField) ILikeString(str string) Predicate {
	return Expr("{} ILIKE {}", field, str)
}

// NotILikeString returns a 'field NOT ILIKE str' Predicate.
func (field StringField) NotILikeString(str string) Predicate {
	return Expr("{} NOT ILIKE {}", field, str)
}

// Set returns an Assignment assigning the value to the field.
func (field StringField) Set(value any) Assignment {
	return Set(field, value)
}

// Setf returns an Assignment assigning an expression to the field.
func (field StringField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// SetString returns an Assignment assigning a string to the field.
func (field StringField) SetString(str string) Assignment { return Set(field, str) }

// GetAlias returns the alias of the StringField.
func (field StringField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field StringField) IsField() {}

// IsString implements the String interface.
func (field StringField) IsString() {}

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
func (field TimeField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new TimeField with the given alias.
func (field TimeField) As(alias string) TimeField {
	field.alias = alias
	return field
}

// Asc returns a new TimeField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (field TimeField) Asc() TimeField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
}

// Desc returns a new TimeField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (field TimeField) Desc() TimeField {
	field.desc.Valid = true
	field.desc.Bool = true
	return field
}

// NullsLast returns a new TimeField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (field TimeField) NullsLast() TimeField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new TimeField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field TimeField) NullsFirst() TimeField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field TimeField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field TimeField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field TimeField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// In returns a 'field IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field IN (x, y, z)'.
func (field TimeField) In(value any) Predicate { return In(field, value) }

// NotIn returns a 'field NOT IN (value)' Predicate. The value can be a slice,
// which corresponds to the expression 'field NOT IN (x, y, z)'.
func (field TimeField) NotIn(value any) Predicate { return NotIn(field, value) }

// Eq returns a 'field = value' Predicate.
func (field TimeField) Eq(value Time) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field TimeField) Ne(value Time) Predicate { return Ne(field, value) }

// Lt returns a 'field < value' Predicate.
func (field TimeField) Lt(value Time) Predicate { return Lt(field, value) }

// Le returns a 'field <= value' Predicate.
func (field TimeField) Le(value Time) Predicate { return Le(field, value) }

// Gt returns a 'field > value' Predicate.
func (field TimeField) Gt(value Time) Predicate { return Gt(field, value) }

// Ge returns a 'field >= value' Predicate.
func (field TimeField) Ge(value Time) Predicate { return Ge(field, value) }

// EqTime returns a 'field = t' Predicate.
func (field TimeField) EqTime(t time.Time) Predicate { return Eq(field, t) }

// NeTime returns a 'field <> t' Predicate.
func (field TimeField) NeTime(t time.Time) Predicate { return Ne(field, t) }

// LtTime returns a 'field < t' Predicate.
func (field TimeField) LtTime(t time.Time) Predicate { return Lt(field, t) }

// LeTime returns a 'field <= t' Predicate.
func (field TimeField) LeTime(t time.Time) Predicate { return Le(field, t) }

// GtTime returns a 'field > t' Predicate.
func (field TimeField) GtTime(t time.Time) Predicate { return Gt(field, t) }

// GeTime returns a 'field >= t' Predicate.
func (field TimeField) GeTime(t time.Time) Predicate { return Ge(field, t) }

// Set returns an Assignment assigning the value to the field.
func (field TimeField) Set(value any) Assignment {
	return Set(field, value)
}

// Setf returns an Assignment assigning an expression to the field.
func (field TimeField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// SetTime returns an Assignment assigning a time.Time to the field.
func (field TimeField) SetTime(t time.Time) Assignment { return Set(field, t) }

// SetTimestamp returns an Assignment assigning a Timestamp to the field.
func (field TimeField) SetTimestamp(t Timestamp) Assignment { return Set(field, t) }

// GetAlias returns the alias of the TimeField.
func (field TimeField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field TimeField) IsField() {}

// IsTime implements the Time interface.
func (field TimeField) IsTime() {}

// Timestamp is as a replacement for sql.NullTime but with the following
// enhancements:
//
// 1. Timestamp.Value() returns an int64 unix timestamp if the dialect is
// SQLite, otherwise it returns a time.Time (similar to sql.NullTime).
//
// 2. Timestamp.Scan() additionally supports scanning from int64 and text
// (string/[]byte) values on top of what sql.NullTime already supports. The
// following text timestamp formats are supported:
//
//	var timestampFormats = []string{
//		"2006-01-02 15:04:05.999999999-07:00",
//		"2006-01-02T15:04:05.999999999-07:00",
//		"2006-01-02 15:04:05.999999999",
//		"2006-01-02T15:04:05.999999999",
//		"2006-01-02 15:04:05",
//		"2006-01-02T15:04:05",
//		"2006-01-02 15:04",
//		"2006-01-02T15:04",
//		"2006-01-02",
//	}
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
var timestampFormats = []string{
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

// Scan implements the sql.Scanner interface. It additionally supports scanning
// from int64 and text (string/[]byte) values on top of what sql.NullTime
// already supports. The following text timestamp formats are supported:
//
//	var timestampFormats = []string{
//		"2006-01-02 15:04:05.999999999-07:00",
//		"2006-01-02T15:04:05.999999999-07:00",
//		"2006-01-02 15:04:05.999999999",
//		"2006-01-02T15:04:05.999999999",
//		"2006-01-02 15:04:05",
//		"2006-01-02T15:04:05",
//		"2006-01-02 15:04",
//		"2006-01-02T15:04",
//		"2006-01-02",
//	}
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
		for _, format := range timestampFormats {
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
		for _, format := range timestampFormats {
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

// Value implements the driver.Valuer interface. It returns an int64 unix
// timestamp if the dialect is SQLite, otherwise it returns a time.Time
// (similar to sql.NullTime).
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
func (field UUIDField) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	writeFieldIdentifier(ctx, dialect, buf, args, params, field.table, field.name)
	writeFieldOrder(ctx, dialect, buf, args, params, field.desc, field.nullsfirst)
	return nil
}

// As returns a new UUIDField with the given alias.
func (field UUIDField) As(alias string) UUIDField {
	field.alias = alias
	return field
}

// Asc returns a new UUIDField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field ASC'.
func (field UUIDField) Asc() UUIDField {
	field.desc.Valid = true
	field.desc.Bool = false
	return field
}

// Desc returns a new UUIDField indicating that it should be ordered in ascending
// order i.e. 'ORDER BY field DESC'.
func (field UUIDField) Desc() UUIDField {
	field.desc.Valid = true
	field.desc.Bool = true
	return field
}

// NullsLast returns a new UUIDField indicating that it should be ordered
// with nulls last i.e. 'ORDER BY field NULLS LAST'.
func (field UUIDField) NullsLast() UUIDField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = false
	return field
}

// NullsFirst returns a new UUIDField indicating that it should be ordered
// with nulls first i.e. 'ORDER BY field NULLS FIRST'.
func (field UUIDField) NullsFirst() UUIDField {
	field.nullsfirst.Valid = true
	field.nullsfirst.Bool = true
	return field
}

// WithPrefix returns a new Field that with the given prefix.
func (field UUIDField) WithPrefix(prefix string) Field {
	field.table.alias = ""
	field.table.name = prefix
	return field
}

// IsNull returns a 'field IS NULL' Predicate.
func (field UUIDField) IsNull() Predicate { return Expr("{} IS NULL", field) }

// IsNotNull returns a 'field IS NOT NULL' Predicate.
func (field UUIDField) IsNotNull() Predicate { return Expr("{} IS NOT NULL", field) }

// In returns a 'field IN (value)' Predicate. The value can be a slice, which
// corresponds to the expression 'field IN (x, y, z)'.
func (field UUIDField) In(value any) Predicate { return In(field, value) }

// NotIn returns a 'field NOT IN (value)' Predicate. The value can be a slice,
// which corresponds to the expression 'field NOT IN (x, y, z)'.
func (field UUIDField) NotIn(value any) Predicate { return NotIn(field, value) }

// Eq returns a 'field = value' Predicate.
func (field UUIDField) Eq(value any) Predicate { return Eq(field, value) }

// Ne returns a 'field <> value' Predicate.
func (field UUIDField) Ne(value any) Predicate { return Ne(field, value) }

// EqUUID returns a 'field = value' Predicate. The value is wrapped in
// UUIDValue().
func (field UUIDField) EqUUID(value any) Predicate { return Eq(field, UUIDValue(value)) }

// NeUUID returns a 'field <> value' Predicate. The value is wrapped in
// UUIDValue().
func (field UUIDField) NeUUID(value any) Predicate { return Ne(field, UUIDValue(value)) }

// Set returns an Assignment assigning the value to the field.
func (field UUIDField) Set(value any) Assignment {
	return Set(field, value)
}

// SetUUID returns an Assignment assigning the value to the field. It wraps the
// value in UUIDValue().
func (field UUIDField) SetUUID(value any) Assignment {
	return Set(field, UUIDValue(value))
}

// Set returns an Assignment assigning the value to the field.
func (field UUIDField) Setf(format string, values ...any) Assignment {
	return Setf(field, format, values...)
}

// GetAlias returns the alias of the UUIDField.
func (field UUIDField) GetAlias() string { return field.alias }

// IsField implements the Field interface.
func (field UUIDField) IsField() {}

// IsUUID implements the UUID interface.
func (field UUIDField) IsUUID() {}

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
	tag := firstfieldType.Tag.Get("sq")
	tableSchema, tableName, ok := strings.Cut(tag, ".")
	if !ok {
		tableSchema, tableName = "", tableSchema
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
