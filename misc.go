package sq

import (
	"bytes"
	"context"
	"fmt"
	"strings"
)

// ValueExpression represents an SQL value that is passed in as an argument to
// a prepared query.
type ValueExpression struct {
	value any
	alias string
}

var _ interface {
	Field
	Predicate
	Any
} = (*ValueExpression)(nil)

// Value returns a new ValueExpression.
func Value(value any) ValueExpression { return ValueExpression{value: value} }

// WriteSQL implements the SQLWriter interface.
func (e ValueExpression) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return WriteValue(ctx, dialect, buf, args, params, e.value)
}

// As returns a new ValueExpression with the given alias.
func (e ValueExpression) As(alias string) ValueExpression {
	e.alias = alias
	return e
}

// In returns a 'expr IN (val)' Predicate.
func (e ValueExpression) In(val any) Predicate { return In(e.value, val) }

// Eq returns a 'expr = val' Predicate.
func (e ValueExpression) Eq(val any) Predicate { return Eq(e.value, val) }

// Ne returns a 'expr <> val' Predicate.
func (e ValueExpression) Ne(val any) Predicate { return Ne(e.value, val) }

// Lt returns a 'expr < val' Predicate.
func (e ValueExpression) Lt(val any) Predicate { return Lt(e.value, val) }

// Le returns a 'expr <= val' Predicate.
func (e ValueExpression) Le(val any) Predicate { return Le(e.value, val) }

// Gt returns a 'expr > val' Predicate.
func (e ValueExpression) Gt(val any) Predicate { return Gt(e.value, val) }

// Ge returns a 'expr >= val' Predicate.
func (e ValueExpression) Ge(val any) Predicate { return Ge(e.value, val) }

// GetAlias returns the alias of the ValueExpression.
func (e ValueExpression) GetAlias() string { return e.alias }

// IsField implements the Field interface.
func (e ValueExpression) IsField() {}

// IsArray implements the Array interface.
func (e ValueExpression) IsArray() {}

// IsBinary implements the Binary interface.
func (e ValueExpression) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (e ValueExpression) IsBoolean() {}

// IsEnum implements the Enum interface.
func (e ValueExpression) IsEnum() {}

// IsJSON implements the JSON interface.
func (e ValueExpression) IsJSON() {}

// IsNumber implements the Number interface.
func (e ValueExpression) IsNumber() {}

// IsString implements the String interface.
func (e ValueExpression) IsString() {}

// IsTime implements the Time interfaces.
func (e ValueExpression) IsTime() {}

// IsUUID implements the UUID interface.
func (e ValueExpression) IsUUID() {}

// LiteralValue represents an SQL value literally interpolated into the query.
// Doing so potentially exposes the query to SQL injection so only do this for
// values that you trust e.g. literals and constants.
type LiteralValue struct {
	value any
	alias string
}

var _ interface {
	Field
	Predicate
	Binary
	Boolean
	Number
	String
	Time
} = (*LiteralValue)(nil)

// Literal returns a new LiteralValue.
func Literal(value any) LiteralValue { return LiteralValue{value: value} }

// WriteSQL implements the SQLWriter interface.
func (v LiteralValue) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	s, err := Sprint(dialect, v.value)
	if err != nil {
		return err
	}
	buf.WriteString(s)
	return nil
}

// As returns a new LiteralValue with the given alias.
func (v LiteralValue) As(alias string) LiteralValue {
	v.alias = alias
	return v
}

// In returns a 'literal IN (val)' Predicate.
func (v LiteralValue) In(val any) Predicate { return In(v, val) }

// Eq returns a 'literal = val' Predicate.
func (v LiteralValue) Eq(val any) Predicate { return Eq(v, val) }

// Ne returns a 'literal <> val' Predicate.
func (v LiteralValue) Ne(val any) Predicate { return Ne(v, val) }

// Lt returns a 'literal < val' Predicate.
func (v LiteralValue) Lt(val any) Predicate { return Lt(v, val) }

// Le returns a 'literal <= val' Predicate.
func (v LiteralValue) Le(val any) Predicate { return Le(v, val) }

// Gt returns a 'literal > val' Predicate.
func (v LiteralValue) Gt(val any) Predicate { return Gt(v, val) }

// Ge returns a 'literal >= val' Predicate.
func (v LiteralValue) Ge(val any) Predicate { return Ge(v, val) }

// GetAlias returns the alias of the LiteralValue.
func (v LiteralValue) GetAlias() string { return v.alias }

// IsField implements the Field interface.
func (v LiteralValue) IsField() {}

// IsBinary implements the Binary interface.
func (v LiteralValue) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (v LiteralValue) IsBoolean() {}

// IsNumber implements the Number interface.
func (v LiteralValue) IsNumber() {}

// IsString implements the String interface.
func (v LiteralValue) IsString() {}

// IsTime implements the Time interfaces.
func (v LiteralValue) IsTime() {}

// DialectExpression represents an SQL expression that renders differently
// depending on the dialect.
type DialectExpression struct {
	Default any
	Cases   DialectCases
}

// DialectCases is a slice of DialectCases.
type DialectCases = []DialectCase

// DialectCase holds the result to be used for a given dialect in a
// DialectExpression.
type DialectCase struct {
	Dialect string
	Result  any
}

var _ interface {
	Table
	Field
	Predicate
	Any
} = (*DialectExpression)(nil)

// DialectValue returns a new DialectExpression. The value passed in is used as
// the default.
func DialectValue(value any) DialectExpression {
	return DialectExpression{Default: value}
}

// DialectExpr returns a new DialectExpression. The expression passed in is
// used as the default.
func DialectExpr(format string, values ...any) DialectExpression {
	return DialectExpression{Default: Expr(format, values...)}
}

// WriteSQL implements the SQLWriter interface.
func (e DialectExpression) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	for _, Case := range e.Cases {
		if dialect == Case.Dialect {
			return WriteValue(ctx, dialect, buf, args, params, Case.Result)
		}
	}
	return WriteValue(ctx, dialect, buf, args, params, e.Default)
}

// DialectValue adds a new dialect-value pair to the DialectExpression.
func (e DialectExpression) DialectValue(dialect string, value any) DialectExpression {
	e.Cases = append(e.Cases, DialectCase{Dialect: dialect, Result: value})
	return e
}

// DialectExpr adds a new dialect-expression pair to the DialectExpression.
func (e DialectExpression) DialectExpr(dialect string, format string, values ...any) DialectExpression {
	e.Cases = append(e.Cases, DialectCase{Dialect: dialect, Result: Expr(format, values...)})
	return e
}

// IsTable implements the Table interface.
func (e DialectExpression) IsTable() {}

// IsField implements the Field interface.
func (e DialectExpression) IsField() {}

// IsArray implements the Array interface.
func (e DialectExpression) IsArray() {}

// IsBinary implements the Binary interface.
func (e DialectExpression) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (e DialectExpression) IsBoolean() {}

// IsEnum implements the Enum interface.
func (e DialectExpression) IsEnum() {}

// IsJSON implements the JSON interface.
func (e DialectExpression) IsJSON() {}

// IsNumber implements the Number interface.
func (e DialectExpression) IsNumber() {}

// IsString implements the String interface.
func (e DialectExpression) IsString() {}

// IsTime implements the Time interface.
func (e DialectExpression) IsTime() {}

// IsUUID implements the UUID interface.
func (e DialectExpression) IsUUID() {}

// CaseExpression represents an SQL CASE expression.
type CaseExpression struct {
	alias   string
	Cases   PredicateCases
	Default any
}

// PredicateCases is a slice of PredicateCases.
type PredicateCases = []PredicateCase

// PredicateCase holds the result to be used for a given predicate in a
// CaseExpression.
type PredicateCase struct {
	Predicate Predicate
	Result    any
}

var _ interface {
	Field
	Any
} = (*CaseExpression)(nil)

// CaseWhen returns a new CaseExpression.
func CaseWhen(predicate Predicate, result any) CaseExpression {
	return CaseExpression{
		Cases: PredicateCases{{Predicate: predicate, Result: result}},
	}
}

// WriteSQL implements the SQLWriter interface.
func (e CaseExpression) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString("CASE")
	if len(e.Cases) == 0 {
		return fmt.Errorf("CaseExpression empty")
	}
	var err error
	for i, Case := range e.Cases {
		buf.WriteString(" WHEN ")
		err = WriteValue(ctx, dialect, buf, args, params, Case.Predicate)
		if err != nil {
			return fmt.Errorf("CASE #%d WHEN: %w", i+1, err)
		}
		buf.WriteString(" THEN ")
		err = WriteValue(ctx, dialect, buf, args, params, Case.Result)
		if err != nil {
			return fmt.Errorf("CASE #%d THEN: %w", i+1, err)
		}
	}
	if e.Default != nil {
		buf.WriteString(" ELSE ")
		err = WriteValue(ctx, dialect, buf, args, params, e.Default)
		if err != nil {
			return fmt.Errorf("CASE ELSE: %w", err)
		}
	}
	buf.WriteString(" END")
	return nil
}

// When adds a new predicate-result pair to the CaseExpression.
func (e CaseExpression) When(predicate Predicate, result any) CaseExpression {
	e.Cases = append(e.Cases, PredicateCase{Predicate: predicate, Result: result})
	return e
}

// Else sets the fallback result of the CaseExpression.
func (e CaseExpression) Else(fallback any) CaseExpression {
	e.Default = fallback
	return e
}

// As returns a new CaseExpression with the given alias.
func (e CaseExpression) As(alias string) CaseExpression {
	e.alias = alias
	return e
}

// GetAlias returns the alias of the CaseExpression.
func (e CaseExpression) GetAlias() string { return e.alias }

// IsField implements the Field interface.
func (e CaseExpression) IsField() {}

// IsArray implements the Array interface.
func (e CaseExpression) IsArray() {}

// IsBinary implements the Binary interface.
func (e CaseExpression) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (e CaseExpression) IsBoolean() {}

// IsEnum implements the Enum interface.
func (e CaseExpression) IsEnum() {}

// IsJSON implements the JSON interface.
func (e CaseExpression) IsJSON() {}

// IsNumber implements the Number interface.
func (e CaseExpression) IsNumber() {}

// IsString implements the String interface.
func (e CaseExpression) IsString() {}

// IsTime implements the Time interface.
func (e CaseExpression) IsTime() {}

// IsUUID implements the UUID interface.
func (e CaseExpression) IsUUID() {}

// SimpleCaseExpression represents an SQL simple CASE expression.
type SimpleCaseExpression struct {
	alias      string
	Expression any
	Cases      SimpleCases
	Default    any
}

// SimpleCases is a slice of SimpleCases.
type SimpleCases = []SimpleCase

// SimpleCase holds the result to be used for a given value in a
// SimpleCaseExpression.
type SimpleCase struct {
	Value  any
	Result any
}

var _ interface {
	Field
	Any
} = (*SimpleCaseExpression)(nil)

// Case returns a new SimpleCaseExpression.
func Case(expression any) SimpleCaseExpression {
	return SimpleCaseExpression{Expression: expression}
}

// WriteSQL implements the SQLWriter interface.
func (e SimpleCaseExpression) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString("CASE ")
	if len(e.Cases) == 0 {
		return fmt.Errorf("SimpleCaseExpression empty")
	}
	var err error
	err = WriteValue(ctx, dialect, buf, args, params, e.Expression)
	if err != nil {
		return fmt.Errorf("CASE: %w", err)
	}
	for i, Case := range e.Cases {
		buf.WriteString(" WHEN ")
		err = WriteValue(ctx, dialect, buf, args, params, Case.Value)
		if err != nil {
			return fmt.Errorf("CASE #%d WHEN: %w", i+1, err)
		}
		buf.WriteString(" THEN ")
		err = WriteValue(ctx, dialect, buf, args, params, Case.Result)
		if err != nil {
			return fmt.Errorf("CASE #%d THEN: %w", i+1, err)
		}
	}
	if e.Default != nil {
		buf.WriteString(" ELSE ")
		err = WriteValue(ctx, dialect, buf, args, params, e.Default)
		if err != nil {
			return fmt.Errorf("CASE ELSE: %w", err)
		}
	}
	buf.WriteString(" END")
	return nil
}

// When adds a new value-result pair to the SimpleCaseExpression.
func (e SimpleCaseExpression) When(value any, result any) SimpleCaseExpression {
	e.Cases = append(e.Cases, SimpleCase{Value: value, Result: result})
	return e
}

// Else sets the fallback result of the SimpleCaseExpression.
func (e SimpleCaseExpression) Else(fallback any) SimpleCaseExpression {
	e.Default = fallback
	return e
}

// As returns a new SimpleCaseExpression with the given alias.
func (e SimpleCaseExpression) As(alias string) SimpleCaseExpression {
	e.alias = alias
	return e
}

// GetAlias returns the alias of the SimpleCaseExpression.
func (e SimpleCaseExpression) GetAlias() string { return e.alias }

// IsField implements the Field interface.
func (e SimpleCaseExpression) IsField() {}

// IsArray implements the Array interface.
func (e SimpleCaseExpression) IsArray() {}

// IsBinary implements the Binary interface.
func (e SimpleCaseExpression) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (e SimpleCaseExpression) IsBoolean() {}

// IsEnum implements the Enum interface.
func (e SimpleCaseExpression) IsEnum() {}

// IsJSON implements the JSON interface.
func (e SimpleCaseExpression) IsJSON() {}

// IsNumber implements the Number interface.
func (e SimpleCaseExpression) IsNumber() {}

// IsString implements the String interface.
func (e SimpleCaseExpression) IsString() {}

// IsTime implements the Time interface.
func (e SimpleCaseExpression) IsTime() {}

// IsUUID implements the UUID interface.
func (e SimpleCaseExpression) IsUUID() {}

// Count represents an SQL COUNT(<field>) expression.
func Count(field Field) Expression { return Expr("COUNT({})", field) }

// CountStar represents an SQL COUNT(*) expression.
func CountStar() Expression { return Expr("COUNT(*)") }

// Sum represents an SQL SUM(<num>) expression.
func Sum(num Number) Expression { return Expr("SUM({})", num) }

// Avg represents an SQL AVG(<num>) expression.
func Avg(num Number) Expression { return Expr("AVG({})", num) }

// Min represent an SQL MIN(<field>) expression.
func Min(field Field) Expression { return Expr("MIN({})", field) }

// Max represents an SQL MAX(<field>) expression.
func Max(field Field) Expression { return Expr("MAX({})", field) }

// SelectValues represents a table literal comprised of SELECT statements
// UNION-ed together e.g.
//
//   (SELECT 1 AS a, 2 AS b, 3 AS c
//   UNION ALL
//   SELECT 4, 5, 6
//   UNION ALL
//   SELECT 7, 8, 9) AS tbl
type SelectValues struct {
	Alias     string
	Columns   []string
	RowValues [][]any
}

var _ interface {
	Query
	Table
} = (*SelectValues)(nil)

// WriteSQL implements the SQLWriter interface.
func (vs SelectValues) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	for i, rowvalue := range vs.RowValues {
		if i > 0 {
			buf.WriteString(" UNION ALL ")
		}
		if len(vs.Columns) > 0 && len(rowvalue) != len(vs.Columns) {
			return fmt.Errorf("rowvalue #%d: got %d values, want %d values (%s)", i+1, len(rowvalue), len(vs.Columns), strings.Join(vs.Columns, ", "))
		}
		buf.WriteString("SELECT ")
		for j, value := range rowvalue {
			if j > 0 {
				buf.WriteString(", ")
			}
			err = WriteValue(ctx, dialect, buf, args, params, value)
			if err != nil {
				return fmt.Errorf("rowvalue #%d value #%d: %w", i+1, j+1, err)
			}
			if i == 0 && j < len(vs.Columns) {
				buf.WriteString(" AS " + QuoteIdentifier(dialect, vs.Columns[j]))
			}
		}
	}
	return nil
}

// Field returns a new field qualified by the SelectValues' alias.
func (vs SelectValues) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: vs.Alias})
}

// SetFetchableFields implements the Query interface. It always returns false
// as the second result.
func (vs SelectValues) SetFetchableFields([]Field) (query Query, ok bool) { return vs, false }

// GetFetchableFields returns the fetchable fields of the SelectValues.
func (vs SelectValues) GetFetchableFields() []Field {
	fields := make([]Field, len(vs.Columns))
	for i, column := range vs.Columns {
		fields[i] = NewAnyField(column, NewTableStruct("", "", vs.Alias))
	}
	return fields
}

// GetDialect implements the Query interface. It always returns an empty
// string.
func (vs SelectValues) GetDialect() string { return "" }

// GetAlias returns the alias of the SelectValues.
func (vs SelectValues) GetAlias() string { return vs.Alias }

// IsTable implements the Table interface.
func (vs SelectValues) IsTable() {}

// TableValues represents a table literal created by the VALUES clause e.g.
//
//   (VALUES
//     (1, 2, 3),
//     (4, 5, 6),
//     (7, 8, 9)) AS tbl (a, b, c)
type TableValues struct {
	Alias     string
	Columns   []string
	RowValues [][]any
}

var _ interface {
	Query
	Table
} = (*TableValues)(nil)

// WriteSQL implements the SQLWriter interface.
func (vs TableValues) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	buf.WriteString("VALUES ")
	for i, rowvalue := range vs.RowValues {
		if len(vs.Columns) > 0 && len(vs.Columns) != len(rowvalue) {
			return fmt.Errorf("rowvalue #%d: got %d values, want %d values (%s)", i+1, len(rowvalue), len(vs.Columns), strings.Join(vs.Columns, ", "))
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		if dialect == DialectMySQL {
			buf.WriteString("ROW(")
		} else {
			buf.WriteString("(")
		}
		for j, value := range rowvalue {
			if j > 0 {
				buf.WriteString(", ")
			}
			err = WriteValue(ctx, dialect, buf, args, params, value)
			if err != nil {
				return fmt.Errorf("rowvalue #%d value #%d: %w", i+1, j+1, err)
			}
		}
		buf.WriteString(")")
	}
	return nil
}

// Field returns a new field qualified by the TableValues' alias.
func (vs TableValues) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: vs.Alias})
}

// SetFetchableFields implements the Query interface. It always returns false
// as the second result.
func (vs TableValues) SetFetchableFields([]Field) (query Query, ok bool) { return vs, false }

// GetFetchableFields returns the fetchable fields of the TableValues.
func (vs TableValues) GetFetchableFields() []Field {
	fields := make([]Field, len(vs.Columns))
	for i, column := range vs.Columns {
		fields[i] = NewAnyField(column, NewTableStruct("", "", vs.Alias))
	}
	return fields
}

// GetDialect implements the Query interface. It always returns an empty
// string.
func (vs TableValues) GetDialect() string { return "" }

// GetAlias returns the alias of the TableValues.
func (vs TableValues) GetAlias() string { return vs.Alias }

// GetColumns returns the names of the columns in the TableValues.
func (vs TableValues) GetColumns() []string { return vs.Columns }

// IsTable implements the Table interface.
func (vs TableValues) IsTable() {}
