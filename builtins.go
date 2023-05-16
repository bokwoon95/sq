package sq

import (
	"bytes"
	"context"
	"fmt"
	"strings"
)

// Expression is an SQL expression that satisfies the Table, Field, Predicate,
// Binary, Boolean, Number, String and Time interfaces.
type Expression struct {
	format string
	values []any
	alias  string
}

var _ interface {
	Table
	Field
	Predicate
	Any
	Assignment
} = (*Expression)(nil)

// Expr creates a new Expression using Writef syntax.
func Expr(format string, values ...any) Expression {
	return Expression{format: format, values: values}
}

// WriteSQL implements the SQLWriter interface.
func (expr Expression) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	err := Writef(ctx, dialect, buf, args, params, expr.format, expr.values)
	if err != nil {
		return err
	}
	return nil
}

// As returns a new Expression with the given alias.
func (expr Expression) As(alias string) Expression {
	expr.alias = alias
	return expr
}

// In returns an 'expr IN (value)' Predicate.
func (expr Expression) In(value any) Predicate { return In(expr, value) }

// In returns an 'expr NOT IN (value)' Predicate.
func (expr Expression) NotIn(value any) Predicate { return NotIn(expr, value) }

// Eq returns an 'expr = value' Predicate.
func (expr Expression) Eq(value any) Predicate { return cmp("=", expr, value) }

// Ne returns an 'expr <> value' Predicate.
func (expr Expression) Ne(value any) Predicate { return cmp("<>", expr, value) }

// Lt returns an 'expr < value' Predicate.
func (expr Expression) Lt(value any) Predicate { return cmp("<", expr, value) }

// Le returns an 'expr <= value' Predicate.
func (expr Expression) Le(value any) Predicate { return cmp("<=", expr, value) }

// Gt returns an 'expr > value' Predicate.
func (expr Expression) Gt(value any) Predicate { return cmp(">", expr, value) }

// Ge returns an 'expr >= value' Predicate.
func (expr Expression) Ge(value any) Predicate { return cmp(">=", expr, value) }

// GetAlias returns the alias of the Expression.
func (expr Expression) GetAlias() string { return expr.alias }

// IsTable implements the Table interface.
func (expr Expression) IsTable() {}

// IsField implements the Field interface.
func (expr Expression) IsField() {}

// IsArray implements the Array interface.
func (expr Expression) IsArray() {}

// IsBinary implements the Binary interface.
func (expr Expression) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (expr Expression) IsBoolean() {}

// IsEnum implements the Enum interface.
func (expr Expression) IsEnum() {}

// IsJSON implements the JSON interface.
func (expr Expression) IsJSON() {}

// IsNumber implements the Number interface.
func (expr Expression) IsNumber() {}

// IsString implements the String interface.
func (expr Expression) IsString() {}

// IsTime implements the Time interface.
func (expr Expression) IsTime() {}

// IsUUID implements the UUID interface.
func (expr Expression) IsUUID() {}

func (e Expression) IsAssignment() {}

// CustomQuery represents a user-defined query.
type CustomQuery struct {
	dialect string
	format  string
	values  []any
	fields  []Field
	alias   string
}

var _ Query = (*CustomQuery)(nil)

// Queryf creates a new query using Writef syntax.
func Queryf(format string, values ...any) CustomQuery {
	return CustomQuery{format: format, values: values}
}

// Queryf creates a new SQLite query using Writef syntax.
func (b sqliteQueryBuilder) Queryf(format string, values ...any) CustomQuery {
	return CustomQuery{dialect: DialectSQLite, format: format, values: values}
}

// Queryf creates a new Postgres query using Writef syntax.
func (b postgresQueryBuilder) Queryf(format string, values ...any) CustomQuery {
	return CustomQuery{dialect: DialectPostgres, format: format, values: values}
}

// Queryf creates a new MySQL query using Writef syntax.
func (b mysqlQueryBuilder) Queryf(format string, values ...any) CustomQuery {
	return CustomQuery{dialect: DialectMySQL, format: format, values: values}
}

// Queryf creates a new SQL Server query using Writef syntax.
func (b sqlserverQueryBuilder) Queryf(format string, values ...any) CustomQuery {
	return CustomQuery{dialect: DialectSQLServer, format: format, values: values}
}

// Append returns a new CustomQuery with the format string and values slice
// appended to the current CustomQuery.
func (q CustomQuery) Append(format string, values ...any) CustomQuery {
	q.format += " " + format
	q.values = append(q.values, values...)
	return q
}

// WriteSQL implements the SQLWriter interface.
func (q CustomQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	format := q.format
	splitAt := -1
	for i := strings.IndexByte(format, '{'); i >= 0; i = strings.IndexByte(format, '{') {
		if i+2 <= len(format) && format[i:i+2] == "{{" {
			format = format[i+2:]
			continue
		}
		if i+3 <= len(format) && format[i:i+3] == "{*}" {
			splitAt = len(q.format) - len(format[i:])
			break
		}
		format = format[i+1:]
	}
	if splitAt < 0 {
		return Writef(ctx, dialect, buf, args, params, q.format, q.values)
	}
	runningValuesIndex := 0
	ordinalIndices := make(map[int]int)
	err = writef(ctx, dialect, buf, args, params, q.format[:splitAt], q.values, &runningValuesIndex, ordinalIndices)
	if err != nil {
		return err
	}
	err = writeFields(ctx, dialect, buf, args, params, q.fields, true)
	if err != nil {
		return err
	}
	err = writef(ctx, dialect, buf, args, params, q.format[splitAt+3:], q.values, &runningValuesIndex, ordinalIndices)
	if err != nil {
		return err
	}
	return nil
}

// SetFetchableFields sets the fetchable fields of the query.
func (q CustomQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	format := q.format
	for i := strings.IndexByte(format, '{'); i >= 0; i = strings.IndexByte(format, '{') {
		if i+2 <= len(format) && format[i:i+2] == "{{" {
			format = format[i+2:]
			continue
		}
		if i+3 <= len(format) && format[i:i+3] == "{*}" {
			q.fields = fields
			return q, true
		}
		format = format[i+1:]
	}
	return q, false
}

// GetFetchableFields gets the fetchable fields of the query.
func (q CustomQuery) GetFetchableFields() []Field {
	return q.fields
}

// GetDialect gets the dialect of the query.
func (q CustomQuery) GetDialect() string { return q.dialect }

// SetDialect sets the dialect of the query.
func (q CustomQuery) SetDialect(dialect string) CustomQuery {
	q.dialect = dialect
	return q
}

// VariadicPredicate represents the 'x AND y AND z...' or 'x OR Y OR z...' SQL
// construct.
type VariadicPredicate struct {
	// Toplevel indicates if the VariadicPredicate can skip writing the
	// (surrounding brackets).
	Toplevel bool
	alias    string
	// If IsDisjunction is true, the Predicates are joined using OR. If false,
	// the Predicates are joined using AND. The default is AND.
	IsDisjunction bool
	// Predicates holds the predicates inside the VariadicPredicate
	Predicates []Predicate
}

var _ Predicate = (*VariadicPredicate)(nil)

// And joins the predicates together with the AND operator.
func And(predicates ...Predicate) VariadicPredicate {
	return VariadicPredicate{IsDisjunction: false, Predicates: predicates}
}

// Or joins the predicates together with the OR operator.
func Or(predicates ...Predicate) VariadicPredicate {
	return VariadicPredicate{IsDisjunction: true, Predicates: predicates}
}

// WriteSQL implements the SQLWriter interface.
func (p VariadicPredicate) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	if len(p.Predicates) == 0 {
		return fmt.Errorf("VariadicPredicate empty")
	}

	if len(p.Predicates) == 1 {
		switch p1 := p.Predicates[0].(type) {
		case nil:
			return fmt.Errorf("predicate #1 is nil")
		case VariadicPredicate:
			p1.Toplevel = p.Toplevel
			err = p1.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return err
			}
		default:
			err = p.Predicates[0].WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if !p.Toplevel {
		buf.WriteString("(")
	}
	for i, predicate := range p.Predicates {
		if i > 0 {
			if p.IsDisjunction {
				buf.WriteString(" OR ")
			} else {
				buf.WriteString(" AND ")
			}
		}
		switch predicate := predicate.(type) {
		case nil:
			return fmt.Errorf("predicate #%d is nil", i+1)
		case VariadicPredicate:
			predicate.Toplevel = false
			err = predicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("predicate #%d: %w", i+1, err)
			}
		default:
			err = predicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("predicate #%d: %w", i+1, err)
			}
		}
	}
	if !p.Toplevel {
		buf.WriteString(")")
	}
	return nil
}

// As returns a new VariadicPredicate with the given alias.
func (p VariadicPredicate) As(alias string) VariadicPredicate {
	p.alias = alias
	return p
}

// GetAlias returns the alias of the VariadicPredicate.
func (p VariadicPredicate) GetAlias() string { return p.alias }

// IsField implements the Field interface.
func (p VariadicPredicate) IsField() {}

// IsBooleanType implements the Predicate interface.
func (p VariadicPredicate) IsBoolean() {}

// assignment represents assigning a value to a Field.
type assignment struct {
	field Field
	value any
}

var _ Assignment = (*assignment)(nil)

// Set creates a new Assignment assigning the value to a field.
func Set(field Field, value any) Assignment {
	return assignment{field: field, value: value}
}

// Setf creates a new Assignment assigning a custom expression to a Field.
func Setf(field Field, format string, values ...any) Assignment {
	return assignment{field: field, value: Expr(format, values...)}
}

// WriteSQL implements the SQLWriter interface.
func (a assignment) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	if a.field == nil {
		return fmt.Errorf("field is nil")
	}
	var err error
	if dialect == DialectMySQL {
		err = a.field.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return err
		}
	} else {
		err = withPrefix(a.field, "").WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return err
		}
	}
	buf.WriteString(" = ")
	_, isQuery := a.value.(Query)
	if isQuery {
		buf.WriteString("(")
	}
	err = WriteValue(ctx, dialect, buf, args, params, a.value)
	if err != nil {
		return err
	}
	if isQuery {
		buf.WriteString(")")
	}
	return nil
}

// IsAssignment implements the Assignment interface.
func (a assignment) IsAssignment() {}

// Assignments represents a list of Assignments e.g. x = 1, y = 2, z = 3.
type Assignments []Assignment

// WriteSQL implements the SQLWriter interface.
func (as Assignments) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	for i, assignment := range as {
		if assignment == nil {
			return fmt.Errorf("assignment #%d is nil", i+1)
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		err = assignment.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("assignment #%d: %w", i+1, err)
		}
	}
	return nil
}

// RowValue represents an SQL row value expression e.g. (x, y, z).
type RowValue []any

// WriteSQL implements the SQLWriter interface.
func (r RowValue) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString("(")
	var err error
	for i, value := range r {
		if i > 0 {
			buf.WriteString(", ")
		}
		_, isQuery := value.(Query)
		if isQuery {
			buf.WriteString("(")
		}
		err = WriteValue(ctx, dialect, buf, args, params, value)
		if err != nil {
			return fmt.Errorf("rowvalue #%d: %w", i+1, err)
		}
		if isQuery {
			buf.WriteString(")")
		}
	}
	buf.WriteString(")")
	return nil
}

// In returns an 'rowvalue IN (value)' Predicate.
func (r RowValue) In(v any) Predicate { return In(r, v) }

// NotIn returns an 'rowvalue NOT IN (value)' Predicate.
func (r RowValue) NotIn(v any) Predicate { return NotIn(r, v) }

// Eq returns an 'rowvalue = value' Predicate.
func (r RowValue) Eq(v any) Predicate { return cmp("=", r, v) }

// RowValues represents a list of RowValues e.g. (x, y, z), (a, b, c).
type RowValues []RowValue

// WriteSQL implements the SQLWriter interface.
func (rs RowValues) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	for i, r := range rs {
		if i > 0 {
			buf.WriteString(", ")
		}
		err = r.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("rowvalues #%d: %w", i+1, err)
		}
	}
	return nil
}

// Fields represents a list of Fields e.g. tbl.field1, tbl.field2, tbl.field3.
type Fields []Field

// WriteSQL implements the SQLWriter interface.
func (fs Fields) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	for i, field := range fs {
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
	}
	return nil
}

type (
	sqliteQueryBuilder    struct{ ctes []CTE }
	postgresQueryBuilder  struct{ ctes []CTE }
	mysqlQueryBuilder     struct{ ctes []CTE }
	sqlserverQueryBuilder struct{ ctes []CTE }
)

// Dialect-specific query builder variables.
var (
	SQLite    sqliteQueryBuilder
	Postgres  postgresQueryBuilder
	MySQL     mysqlQueryBuilder
	SQLServer sqlserverQueryBuilder
)

// With sets the CTEs in the SQLiteQueryBuilder.
func (b sqliteQueryBuilder) With(ctes ...CTE) sqliteQueryBuilder {
	b.ctes = ctes
	return b
}

// With sets the CTEs in the PostgresQueryBuilder.
func (b postgresQueryBuilder) With(ctes ...CTE) postgresQueryBuilder {
	b.ctes = ctes
	return b
}

// With sets the CTEs in the MySQLQueryBuilder.
func (b mysqlQueryBuilder) With(ctes ...CTE) mysqlQueryBuilder {
	b.ctes = ctes
	return b
}

// With sets the CTEs in the SQLServerQueryBuilder.
func (b sqlserverQueryBuilder) With(ctes ...CTE) sqlserverQueryBuilder {
	b.ctes = ctes
	return b
}

// ToSQL converts an SQLWriter into a query string and args slice.
//
// The params map is used to hold the mappings between named parameters in the
// query to the corresponding index in the args slice and is used for rebinding
// args by their parameter name. If you don't need to track this, you can pass
// in a nil map.
func ToSQL(dialect string, w SQLWriter, params map[string][]int) (query string, args []any, err error) {
	return ToSQLContext(context.Background(), dialect, w, params)
}

// ToSQLContext is like ToSQL but additionally requires a context.Context.
func ToSQLContext(ctx context.Context, dialect string, w SQLWriter, params map[string][]int) (query string, args []any, err error) {
	if w == nil {
		return "", nil, fmt.Errorf("SQLWriter is nil")
	}
	if dialect == "" {
		if q, ok := w.(Query); ok {
			dialect = q.GetDialect()
		}
	}
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	err = w.WriteSQL(ctx, dialect, buf, &args, params)
	query = buf.String()
	if err != nil {
		return query, args, err
	}
	return query, args, nil
}

// Eq returns an 'x = y' Predicate.
func Eq(x, y any) Predicate { return cmp("=", x, y) }

// Ne returns an 'x <> y' Predicate.
func Ne(x, y any) Predicate { return cmp("<>", x, y) }

// Lt returns an 'x < y' Predicate.
func Lt(x, y any) Predicate { return cmp("<", x, y) }

// Le returns an 'x <= y' Predicate.
func Le(x, y any) Predicate { return cmp("<=", x, y) }

// Gt returns an 'x > y' Predicate.
func Gt(x, y any) Predicate { return cmp(">", x, y) }

// Ge returns an 'x >= y' Predicate.
func Ge(x, y any) Predicate { return cmp(">=", x, y) }

// Exists returns an 'EXISTS (query)' Predicate.
func Exists(query Query) Predicate { return Expr("EXISTS ({})", query) }

// NotExists returns a 'NOT EXISTS (query)' Predicate.
func NotExists(query Query) Predicate { return Expr("NOT EXISTS ({})", query) }

// In returns an 'x IN (y)' Predicate.
func In(x, y any) Predicate {
	_, isQueryA := x.(Query)
	_, isRowValueB := y.(RowValue)
	if !isQueryA && !isRowValueB {
		return Expr("{} IN ({})", x, y)
	} else if !isQueryA && isRowValueB {
		return Expr("{} IN {}", x, y)
	} else if isQueryA && !isRowValueB {
		return Expr("({}) IN ({})", x, y)
	} else {
		return Expr("({}) IN {}", x, y)
	}
}

// NotIn returns an 'x NOT IN (y)' Predicate.
func NotIn(x, y any) Predicate {
	_, isQueryA := x.(Query)
	_, isRowValueB := y.(RowValue)
	if !isQueryA && !isRowValueB {
		return Expr("{} NOT IN ({})", x, y)
	} else if !isQueryA && isRowValueB {
		return Expr("{} NOT IN {}", x, y)
	} else if isQueryA && !isRowValueB {
		return Expr("({}) NOT IN ({})", x, y)
	} else {
		return Expr("({}) NOT IN {}", x, y)
	}
}

// cmp returns an 'x <operator> y' Predicate.
func cmp(operator string, x, y any) Expression {
	_, isQueryA := x.(Query)
	_, isQueryB := y.(Query)
	if !isQueryA && !isQueryB {
		return Expr("{} "+operator+" {}", x, y)
	} else if !isQueryA && isQueryB {
		return Expr("{} "+operator+" ({})", x, y)
	} else if isQueryA && !isQueryB {
		return Expr("({}) "+operator+" {}", x, y)
	} else {
		return Expr("({}) "+operator+" ({})", x, y)
	}
}

// appendPolicy will append a policy from a Table (if it implements
// PolicyTable) to a slice of policies. The resultant slice is returned.
func appendPolicy(ctx context.Context, dialect string, policies []Predicate, table Table) ([]Predicate, error) {
	policyTable, ok := table.(PolicyTable)
	if !ok {
		return policies, nil
	}
	policy, err := policyTable.Policy(ctx, dialect)
	if err != nil {
		return nil, err
	}
	if policy != nil {
		policies = append(policies, policy)
	}
	return policies, nil
}

// appendPredicates will append a slices of predicates into a predicate.
func appendPredicates(predicate Predicate, predicates []Predicate) VariadicPredicate {
	if predicate == nil {
		return And(predicates...)
	}
	if p1, ok := predicate.(VariadicPredicate); ok && !p1.IsDisjunction {
		p1.Predicates = append(p1.Predicates, predicates...)
		return p1
	}
	p2 := VariadicPredicate{Predicates: make([]Predicate, 1+len(predicates))}
	p2.Predicates[0] = predicate
	copy(p2.Predicates[1:], predicates)
	return p2
}

func writeTop(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, topLimit, topPercentLimit any, withTies bool) error {
	var err error
	if topLimit != nil {
		buf.WriteString("TOP (")
		err = WriteValue(ctx, dialect, buf, args, params, topLimit)
		if err != nil {
			return fmt.Errorf("TOP: %w", err)
		}
		buf.WriteString(") ")
	} else if topPercentLimit != nil {
		buf.WriteString("TOP (")
		err = WriteValue(ctx, dialect, buf, args, params, topPercentLimit)
		if err != nil {
			return fmt.Errorf("TOP PERCENT: %w", err)
		}
		buf.WriteString(") PERCENT ")
	}
	if (topLimit != nil || topPercentLimit != nil) && withTies {
		buf.WriteString("WITH TIES ")
	}
	return nil
}
