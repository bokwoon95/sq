package sq

import (
	"bytes"
	"context"
	"fmt"
)

// SelectQuery represents an SQL SELECT query.
type SelectQuery struct {
	Dialect string
	// WITH
	CTEs []CTE
	// SELECT
	Distinct         bool
	SelectFields     []Field
	DistinctOnFields []Field
	// TOP
	LimitTop        any
	LimitTopPercent any
	// FROM
	FromTable Table
	// JOIN
	JoinTables []JoinTable
	// WHERE
	WherePredicate Predicate
	// GROUP BY
	GroupByFields []Field
	// HAVING
	HavingPredicate Predicate
	// WINDOW
	NamedWindows []NamedWindow
	// ORDER BY
	OrderByFields []Field
	// LIMIT
	LimitRows any
	// OFFSET
	OffsetRows any
	// FETCH NEXT
	FetchNextRows any
	FetchWithTies bool
	// FOR UPDATE | FOR SHARE
	LockClause string
	LockValues []any
	// AS
	Alias   string
	Columns []string
}

var _ interface {
	Query
	Table
	Field
	Any
} = (*SelectQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SelectQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	if len(q.SelectFields) == 0 {
		return fmt.Errorf("SELECT: no fields provided")
	}
	// Table Policies
	var policies []Predicate
	policies, err = appendPolicy(ctx, dialect, policies, q.FromTable)
	if err != nil {
		return fmt.Errorf("FROM %s Policy: %w", toString(q.Dialect, q.FromTable), err)
	}
	for _, joinTable := range q.JoinTables {
		policies, err = appendPolicy(ctx, dialect, policies, joinTable.Table)
		if err != nil {
			return fmt.Errorf("%s %s Policy: %w", joinTable.JoinOperator, joinTable.Table, err)
		}
	}
	if len(policies) > 0 {
		if q.WherePredicate != nil {
			policies = append(policies, q.WherePredicate)
		}
		q.WherePredicate = And(policies...)
	}
	// WITH
	if len(q.CTEs) > 0 {
		err = writeCTEs(ctx, dialect, buf, args, params, q.CTEs)
		if err != nil {
			return fmt.Errorf("WITH: %w", err)
		}
	}
	// SELECT
	buf.WriteString("SELECT ")
	if q.LimitTop != nil || q.LimitTopPercent != nil { // TOP
		if dialect != DialectSQLServer {
			return fmt.Errorf("%s does not support SELECT TOP n", dialect)
		}
		if len(q.OrderByFields) == 0 {
			return fmt.Errorf("sqlserver does not support TOP without ORDER BY")
		}
		err = writeTop(ctx, dialect, buf, args, params, q.LimitTop, q.LimitTopPercent, q.FetchWithTies)
		if err != nil {
			return err
		}
	}
	if len(q.DistinctOnFields) > 0 {
		if dialect != DialectPostgres {
			return fmt.Errorf("%s does not support SELECT DISTINCT ON", dialect)
		}
		if q.Distinct {
			return fmt.Errorf("postgres SELECT cannot be DISTINCT and DISTINCT ON at the same time")
		}
		buf.WriteString("DISTINCT ON (")
		err = writeFields(ctx, dialect, buf, args, params, q.DistinctOnFields, false)
		if err != nil {
			return fmt.Errorf("DISTINCT ON: %w", err)
		}
		buf.WriteString(") ")
	} else if q.Distinct {
		buf.WriteString("DISTINCT ")
	}
	err = writeFields(ctx, dialect, buf, args, params, q.SelectFields, true)
	if err != nil {
		return fmt.Errorf("SELECT: %w", err)
	}
	// FROM
	if q.FromTable != nil {
		buf.WriteString(" FROM ")
		_, isQuery := q.FromTable.(Query)
		if isQuery {
			buf.WriteString("(")
		}
		err = q.FromTable.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("FROM: %w", err)
		}
		if isQuery {
			buf.WriteString(")")
		}
		if alias := getAlias(q.FromTable); alias != "" {
			buf.WriteString(" AS " + QuoteIdentifier(dialect, alias) + quoteTableColumns(dialect, q.FromTable))
		} else if isQuery && dialect != DialectSQLite {
			return fmt.Errorf("%s FROM subquery must have alias", dialect)
		}
	}
	// JOIN
	if len(q.JoinTables) > 0 {
		if q.FromTable == nil {
			return fmt.Errorf("can't JOIN without a FROM table")
		}
		buf.WriteString(" ")
		err = writeJoinTables(ctx, dialect, buf, args, params, q.JoinTables)
		if err != nil {
			return fmt.Errorf("JOIN: %w", err)
		}
	}
	// WHERE
	if q.WherePredicate != nil {
		buf.WriteString(" WHERE ")
		switch predicate := q.WherePredicate.(type) {
		case VariadicPredicate:
			predicate.Toplevel = true
			err = predicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("WHERE: %w", err)
			}
		default:
			err = q.WherePredicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("WHERE: %w", err)
			}
		}
	}
	// GROUP BY
	if len(q.GroupByFields) > 0 {
		buf.WriteString(" GROUP BY ")
		err = writeFields(ctx, dialect, buf, args, params, q.GroupByFields, false)
		if err != nil {
			return fmt.Errorf("GROUP BY: %w", err)
		}
	}
	// HAVING
	if q.HavingPredicate != nil {
		buf.WriteString(" HAVING ")
		switch predicate := q.HavingPredicate.(type) {
		case VariadicPredicate:
			predicate.Toplevel = true
			err = predicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("HAVING: %w", err)
			}
		default:
			err = q.HavingPredicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("HAVING: %w", err)
			}
		}
	}
	// WINDOW
	if len(q.NamedWindows) > 0 {
		buf.WriteString(" WINDOW ")
		err = NamedWindows(q.NamedWindows).WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("WINDOW: %w", err)
		}
	}
	// ORDER BY
	if len(q.OrderByFields) > 0 {
		buf.WriteString(" ORDER BY ")
		err = writeFields(ctx, dialect, buf, args, params, q.OrderByFields, false)
		if err != nil {
			return fmt.Errorf("ORDER BY: %w", err)
		}
	}
	// LIMIT
	if q.LimitRows != nil {
		if dialect == DialectSQLServer {
			return fmt.Errorf("sqlserver does not support LIMIT")
		}
		buf.WriteString(" LIMIT ")
		err = WriteValue(ctx, dialect, buf, args, params, q.LimitRows)
		if err != nil {
			return fmt.Errorf("LIMIT: %w", err)
		}
	}
	// OFFSET
	if q.OffsetRows != nil {
		if dialect == DialectSQLServer {
			if len(q.OrderByFields) == 0 {
				return fmt.Errorf("sqlserver does not support OFFSET without ORDER BY")
			}
			if q.LimitTop != nil || q.LimitTopPercent != nil {
				return fmt.Errorf("sqlserver does not support OFFSET with TOP")
			}
		}
		buf.WriteString(" OFFSET ")
		err = WriteValue(ctx, dialect, buf, args, params, q.OffsetRows)
		if err != nil {
			return fmt.Errorf("OFFSET: %w", err)
		}
		if dialect == DialectSQLServer {
			buf.WriteString(" ROWS")
		}
	}
	// FETCH NEXT
	if q.FetchNextRows != nil {
		switch dialect {
		case DialectPostgres:
			if q.LimitRows != nil {
				return fmt.Errorf("postgres does not allow FETCH NEXT with LIMIT")
			}
		case DialectSQLServer:
			if q.LimitTop != nil || q.LimitTopPercent != nil {
				return fmt.Errorf("sqlserver does not allow FETCH NEXT with TOP")
			}
		default:
			return fmt.Errorf("%s does not support FETCH NEXT", dialect)
		}
		buf.WriteString(" FETCH NEXT ")
		err = WriteValue(ctx, dialect, buf, args, params, q.FetchNextRows)
		if err != nil {
			return fmt.Errorf("FETCH NEXT: %w", err)
		}
		buf.WriteString(" ROWS ")
		if q.FetchWithTies {
			if dialect == DialectSQLServer {
				return fmt.Errorf("sqlserver WITH TIES only works with TOP")
			}
			if len(q.OrderByFields) == 0 {
				return fmt.Errorf("%s WITH TIES cannot be used without ORDER BY", dialect)
			}
			buf.WriteString("WITH TIES")
		} else {
			buf.WriteString("ONLY")
		}
	}
	// FOR UPDATE | FOR SHARE
	if q.LockClause != "" {
		buf.WriteString(" ")
		err = Writef(ctx, dialect, buf, args, params, q.LockClause, q.LockValues)
		if err != nil {
			return err
		}
	}
	return nil
}

// Select creates a new SelectQuery.
func Select(fields ...Field) SelectQuery {
	return SelectQuery{SelectFields: fields}
}

// SelectDistinct creates a new SelectQuery.
func SelectDistinct(fields ...Field) SelectQuery {
	return SelectQuery{
		SelectFields: fields,
		Distinct:     true,
	}
}

// SelectOne creates a new SelectQuery.
func SelectOne() SelectQuery {
	return SelectQuery{SelectFields: Fields{Expr("1")}}
}

// From creates a new SelectQuery.
func From(table Table) SelectQuery {
	return SelectQuery{FromTable: table}
}

// Select appends to the SelectFields in the SelectQuery.
func (q SelectQuery) Select(fields ...Field) SelectQuery {
	q.SelectFields = append(q.SelectFields, fields...)
	return q
}

// SelectDistinct sets the SelectFields in the SelectQuery.
func (q SelectQuery) SelectDistinct(fields ...Field) SelectQuery {
	q.SelectFields = fields
	q.Distinct = true
	return q
}

// SelectOne sets the SelectQuery to SELECT 1.
func (q SelectQuery) SelectOne(fields ...Field) SelectQuery {
	q.SelectFields = Fields{Expr("1")}
	return q
}

// From sets the FromTable field in the SelectQuery.
func (q SelectQuery) From(table Table) SelectQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the SelectQuery.
func (q SelectQuery) Join(table Table, predicates ...Predicate) SelectQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the SelectQuery.
func (q SelectQuery) LeftJoin(table Table, predicates ...Predicate) SelectQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the SelectQuery.
func (q SelectQuery) CrossJoin(table Table) SelectQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the SelectQuery with a custom join operator.
func (q SelectQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) SelectQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the SelectQuery with the USING operator.
func (q SelectQuery) JoinUsing(table Table, fields ...Field) SelectQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field in the SelectQuery.
func (q SelectQuery) Where(predicates ...Predicate) SelectQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// GroupBy appends to the GroupByFields field in the SelectQuery.
func (q SelectQuery) GroupBy(fields ...Field) SelectQuery {
	q.GroupByFields = append(q.GroupByFields, fields...)
	return q
}

// Having appends to the HavingPredicate field in the SelectQuery.
func (q SelectQuery) Having(predicates ...Predicate) SelectQuery {
	q.HavingPredicate = appendPredicates(q.HavingPredicate, predicates)
	return q
}

// OrderBy appends to the OrderByFields field in the SelectQuery.
func (q SelectQuery) OrderBy(fields ...Field) SelectQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Limit sets the LimitRows field in the SelectQuery.
func (q SelectQuery) Limit(limit any) SelectQuery {
	q.LimitRows = limit
	return q
}

// Offset sets the OffsetRows field in the SelectQuery.
func (q SelectQuery) Offset(offset any) SelectQuery {
	q.OffsetRows = offset
	return q
}

// As returns a new SelectQuery with the table alias (and optionally column
// aliases).
func (q SelectQuery) As(alias string, columns ...string) SelectQuery {
	q.Alias = alias
	q.Columns = columns
	return q
}

// Field returns a new field qualified by the SelectQuery's alias.
func (q SelectQuery) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: q.Alias})
}

// SetFetchableFields implements the Query interface.
func (q SelectQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	if len(q.SelectFields) == 0 {
		q.SelectFields = fields
		return q, true
	}
	return q, false
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SelectQuery) GetFetchableFields() []Field {
	return q.SelectFields
}

// GetDialect implements the Query interface.
func (q SelectQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q SelectQuery) SetDialect(dialect string) SelectQuery {
	q.Dialect = dialect
	return q
}

// GetAlias returns the alias of the SelectQuery.
func (q SelectQuery) GetAlias() string { return q.Alias }

// GetColumns returns the column aliases of the SelectQuery.
func (q SelectQuery) GetColumns() []string { return q.Columns }

// IsTable implements the Table interface.
func (q SelectQuery) IsTable() {}

// IsField implements the Field interface.
func (q SelectQuery) IsField() {}

// IsArray implements the Array interface.
func (q SelectQuery) IsArray() {}

// IsBinary implements the Binary interface.
func (q SelectQuery) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (q SelectQuery) IsBoolean() {}

// IsEnum implements the Enum interface.
func (q SelectQuery) IsEnum() {}

// IsJSON implements the JSON interface.
func (q SelectQuery) IsJSON() {}

// IsNumber implements the Number interface.
func (q SelectQuery) IsNumber() {}

// IsString implements the String interface.
func (q SelectQuery) IsString() {}

// IsTime implements the Time interface.
func (q SelectQuery) IsTime() {}

// IsUUID implements the UUID interface.
func (q SelectQuery) IsUUID() {}

// SQLiteSelectQuery represents an SQLite SELECT query.
type SQLiteSelectQuery SelectQuery

var _ interface {
	Query
	Table
	Field
	Any
} = (*SQLiteSelectQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLiteSelectQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return SelectQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Select creates a new SQLiteSelectQuery.
func (b sqliteQueryBuilder) Select(fields ...Field) SQLiteSelectQuery {
	return SQLiteSelectQuery{
		Dialect:      DialectSQLite,
		CTEs:         b.ctes,
		SelectFields: fields,
	}
}

// SelectDistinct creates a new SQLiteSelectQuery.
func (b sqliteQueryBuilder) SelectDistinct(fields ...Field) SQLiteSelectQuery {
	return SQLiteSelectQuery{
		Dialect:      DialectSQLite,
		CTEs:         b.ctes,
		SelectFields: fields,
		Distinct:     true,
	}
}

// SelectOne creates a new SQLiteSelectQuery.
func (b sqliteQueryBuilder) SelectOne() SQLiteSelectQuery {
	return SQLiteSelectQuery{
		Dialect:      DialectSQLite,
		CTEs:         b.ctes,
		SelectFields: Fields{Expr("1")},
	}
}

// From creates a new SQLiteSelectQuery.
func (b sqliteQueryBuilder) From(table Table) SQLiteSelectQuery {
	return SQLiteSelectQuery{
		Dialect:   DialectSQLite,
		CTEs:      b.ctes,
		FromTable: table,
	}
}

// Select appends to the SelectFields in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) Select(fields ...Field) SQLiteSelectQuery {
	q.SelectFields = append(q.SelectFields, fields...)
	return q
}

// SelectDistinct sets the SelectFields in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) SelectDistinct(fields ...Field) SQLiteSelectQuery {
	q.SelectFields = fields
	q.Distinct = true
	return q
}

// SelectOne sets the SQLiteSelectQuery to SELECT 1.
func (q SQLiteSelectQuery) SelectOne(fields ...Field) SQLiteSelectQuery {
	q.SelectFields = Fields{Expr("1")}
	return q
}

// From sets the FromTable field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) From(table Table) SQLiteSelectQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the SQLiteSelectQuery.
func (q SQLiteSelectQuery) Join(table Table, predicates ...Predicate) SQLiteSelectQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the SQLiteSelectQuery.
func (q SQLiteSelectQuery) LeftJoin(table Table, predicates ...Predicate) SQLiteSelectQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the SQLiteSelectQuery.
func (q SQLiteSelectQuery) CrossJoin(table Table) SQLiteSelectQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the SQLiteSelectQuery with a custom join
// operator.
func (q SQLiteSelectQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) SQLiteSelectQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the SQLiteSelectQuery with the USING operator.
func (q SQLiteSelectQuery) JoinUsing(table Table, fields ...Field) SQLiteSelectQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) Where(predicates ...Predicate) SQLiteSelectQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// GroupBy appends to the GroupByFields field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) GroupBy(fields ...Field) SQLiteSelectQuery {
	q.GroupByFields = append(q.GroupByFields, fields...)
	return q
}

// Having appends to the HavingPredicate field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) Having(predicates ...Predicate) SQLiteSelectQuery {
	q.HavingPredicate = appendPredicates(q.HavingPredicate, predicates)
	return q
}

// OrderBy appends to the OrderByFields field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) OrderBy(fields ...Field) SQLiteSelectQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Limit sets the LimitRows field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) Limit(limit any) SQLiteSelectQuery {
	q.LimitRows = limit
	return q
}

// Offset sets the OffsetRows field in the SQLiteSelectQuery.
func (q SQLiteSelectQuery) Offset(offset any) SQLiteSelectQuery {
	q.OffsetRows = offset
	return q
}

// As returns a new SQLiteSelectQuery with the table alias (and optionally
// column aliases).
func (q SQLiteSelectQuery) As(alias string, columns ...string) SQLiteSelectQuery {
	q.Alias = alias
	q.Columns = columns
	return q
}

// Field returns a new field qualified by the SQLiteSelectQuery's alias.
func (q SQLiteSelectQuery) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: q.Alias})
}

// SetFetchableFields implements the Query interface.
func (q SQLiteSelectQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	if len(q.SelectFields) == 0 {
		q.SelectFields = fields
		return q, true
	}
	return q, false
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SQLiteSelectQuery) GetFetchableFields() []Field {
	return q.SelectFields
}

// GetDialect implements the Query interface.
func (q SQLiteSelectQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q SQLiteSelectQuery) SetDialect(dialect string) SQLiteSelectQuery {
	q.Dialect = dialect
	return q
}

// GetAlias returns the alias of the SQLiteSelectQuery.
func (q SQLiteSelectQuery) GetAlias() string { return q.Alias }

// IsTable implements the Table interface.
func (q SQLiteSelectQuery) IsTable() {}

// IsField implements the Field interface.
func (q SQLiteSelectQuery) IsField() {}

// IsArray implements the Array interface.
func (q SQLiteSelectQuery) IsArray() {}

// IsBinary implements the Binary interface.
func (q SQLiteSelectQuery) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (q SQLiteSelectQuery) IsBoolean() {}

// IsEnum implements the Enum interface.
func (q SQLiteSelectQuery) IsEnum() {}

// IsJSON implements the JSON interface.
func (q SQLiteSelectQuery) IsJSON() {}

// IsNumber implements the Number interface.
func (q SQLiteSelectQuery) IsNumber() {}

// IsString implements the String interface.
func (q SQLiteSelectQuery) IsString() {}

// IsTime implements the Time interface.
func (q SQLiteSelectQuery) IsTime() {}

// IsUUID implements the UUID interface.
func (q SQLiteSelectQuery) IsUUID() {}

// PostgresSelectQuery represents a Postgres SELECT query.
type PostgresSelectQuery SelectQuery

var _ interface {
	Query
	Table
	Field
	Any
} = (*PostgresSelectQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q PostgresSelectQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return SelectQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Select creates a new PostgresSelectQuery.
func (b postgresQueryBuilder) Select(fields ...Field) PostgresSelectQuery {
	q := PostgresSelectQuery{
		CTEs:         b.ctes,
		SelectFields: fields,
	}
	if q.Dialect == "" {
		q.Dialect = DialectPostgres
	}
	return q
}

// SelectDistinct creates a new PostgresSelectQuery.
func (b postgresQueryBuilder) SelectDistinct(fields ...Field) PostgresSelectQuery {
	q := PostgresSelectQuery{
		CTEs:         b.ctes,
		SelectFields: fields,
		Distinct:     true,
	}
	if q.Dialect == "" {
		q.Dialect = DialectPostgres
	}
	return q
}

// SelectOne creates a new PostgresSelectQuery.
func (b postgresQueryBuilder) SelectOne() PostgresSelectQuery {
	q := PostgresSelectQuery{
		CTEs:         b.ctes,
		SelectFields: Fields{Expr("1")},
	}
	if q.Dialect == "" {
		q.Dialect = DialectPostgres
	}
	return q
}

// From creates a new PostgresSelectQuery.
func (b postgresQueryBuilder) From(table Table) PostgresSelectQuery {
	q := PostgresSelectQuery{
		CTEs:      b.ctes,
		FromTable: table,
	}
	if q.Dialect == "" {
		q.Dialect = DialectPostgres
	}
	return q
}

// Select appends to the SelectFields in the PostgresSelectQuery.
func (q PostgresSelectQuery) Select(fields ...Field) PostgresSelectQuery {
	q.SelectFields = append(q.SelectFields, fields...)
	return q
}

// SelectDistinct sets the SelectFields in the PostgresSelectQuery.
func (q PostgresSelectQuery) SelectDistinct(fields ...Field) PostgresSelectQuery {
	q.SelectFields = fields
	q.Distinct = true
	return q
}

// DistinctOn sets the DistinctOnFields in the PostgresSelectQuery.
func (q PostgresSelectQuery) DistinctOn(fields ...Field) PostgresSelectQuery {
	q.DistinctOnFields = fields
	return q
}

// SelectOne sets the PostgresSelectQuery to SELECT 1.
func (q PostgresSelectQuery) SelectOne(fields ...Field) PostgresSelectQuery {
	q.SelectFields = Fields{Expr("1")}
	return q
}

// From sets the FromTable field in the PostgresSelectQuery.
func (q PostgresSelectQuery) From(table Table) PostgresSelectQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the PostgresSelectQuery.
func (q PostgresSelectQuery) Join(table Table, predicates ...Predicate) PostgresSelectQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the PostgresSelectQuery.
func (q PostgresSelectQuery) LeftJoin(table Table, predicates ...Predicate) PostgresSelectQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the PostgresSelectQuery.
func (q PostgresSelectQuery) FullJoin(table Table, predicates ...Predicate) PostgresSelectQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the PostgresSelectQuery.
func (q PostgresSelectQuery) CrossJoin(table Table) PostgresSelectQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the PostgresSelectQuery with a custom join
// operator.
func (q PostgresSelectQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) PostgresSelectQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the PostgresSelectQuery with the USING operator.
func (q PostgresSelectQuery) JoinUsing(table Table, fields ...Field) PostgresSelectQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field in the PostgresSelectQuery.
func (q PostgresSelectQuery) Where(predicates ...Predicate) PostgresSelectQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// GroupBy appends to the GroupByFields field in the PostgresSelectQuery.
func (q PostgresSelectQuery) GroupBy(fields ...Field) PostgresSelectQuery {
	q.GroupByFields = append(q.GroupByFields, fields...)
	return q
}

// Having appends to the HavingPredicate field in the PostgresSelectQuery.
func (q PostgresSelectQuery) Having(predicates ...Predicate) PostgresSelectQuery {
	q.HavingPredicate = appendPredicates(q.HavingPredicate, predicates)
	return q
}

// OrderBy appends to the OrderByFields field in the PostgresSelectQuery.
func (q PostgresSelectQuery) OrderBy(fields ...Field) PostgresSelectQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Limit sets the LimitRows field in the PostgresSelectQuery.
func (q PostgresSelectQuery) Limit(limit any) PostgresSelectQuery {
	q.LimitRows = limit
	return q
}

// Offset sets the OffsetRows field in the PostgresSelectQuery.
func (q PostgresSelectQuery) Offset(offset any) PostgresSelectQuery {
	q.OffsetRows = offset
	return q
}

// FetchNext sets the FetchNextRows field in the PostgresSelectQuery.
func (q PostgresSelectQuery) FetchNext(n any) PostgresSelectQuery {
	q.FetchNextRows = n
	return q
}

// WithTies enables the FetchWithTies field in the PostgresSelectQuery.
func (q PostgresSelectQuery) WithTies() PostgresSelectQuery {
	q.FetchWithTies = true
	return q
}

// LockRows sets the lock clause of the PostgresSelectQuery.
func (q PostgresSelectQuery) LockRows(lockClause string, lockValues ...any) PostgresSelectQuery {
	q.LockClause = lockClause
	q.LockValues = lockValues
	return q
}

// As returns a new PostgresSelectQuery with the table alias (and optionally
// column aliases).
func (q PostgresSelectQuery) As(alias string, columns ...string) PostgresSelectQuery {
	q.Alias = alias
	q.Columns = columns
	return q
}

// Field returns a new field qualified by the PostgresSelectQuery's alias.
func (q PostgresSelectQuery) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: q.Alias})
}

// SetFetchableFields implements the Query interface.
func (q PostgresSelectQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	if len(q.SelectFields) == 0 {
		q.SelectFields = fields
		return q, true
	}
	return q, false
}

// GetFetchableFields returns the fetchable fields of the query.
func (q PostgresSelectQuery) GetFetchableFields() []Field {
	return q.SelectFields
}

// GetDialect implements the Query interface.
func (q PostgresSelectQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q PostgresSelectQuery) SetDialect(dialect string) PostgresSelectQuery {
	q.Dialect = dialect
	return q
}

// GetAlias returns the alias of the PostgresSelectQuery.
func (q PostgresSelectQuery) GetAlias() string { return q.Alias }

// IsTable implements the Table interface.
func (q PostgresSelectQuery) IsTable() {}

// IsField implements the Field interface.
func (q PostgresSelectQuery) IsField() {}

// IsArray implements the Array interface.
func (q PostgresSelectQuery) IsArray() {}

// IsBinary implements the Binary interface.
func (q PostgresSelectQuery) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (q PostgresSelectQuery) IsBoolean() {}

// IsEnum implements the Enum interface.
func (q PostgresSelectQuery) IsEnum() {}

// IsJSON implements the JSON interface.
func (q PostgresSelectQuery) IsJSON() {}

// IsNumber implements the Number interface.
func (q PostgresSelectQuery) IsNumber() {}

// IsString implements the String interface.
func (q PostgresSelectQuery) IsString() {}

// IsTime implements the Time interface.
func (q PostgresSelectQuery) IsTime() {}

// IsUUID implements the UUID interface.
func (q PostgresSelectQuery) IsUUID() {}

// MySQLSelectQuery represents a MySQL SELECT query.
type MySQLSelectQuery SelectQuery

var _ interface {
	Query
	Table
	Field
	Any
} = (*MySQLSelectQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q MySQLSelectQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return SelectQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Select creates a new MySQLSelectQuery.
func (b mysqlQueryBuilder) Select(fields ...Field) MySQLSelectQuery {
	q := MySQLSelectQuery{
		CTEs:         b.ctes,
		SelectFields: fields,
	}
	if q.Dialect == "" {
		q.Dialect = DialectMySQL
	}
	return q
}

// SelectDistinct creates a new MySQLSelectQuery.
func (b mysqlQueryBuilder) SelectDistinct(fields ...Field) MySQLSelectQuery {
	q := MySQLSelectQuery{
		CTEs:         b.ctes,
		SelectFields: fields,
		Distinct:     true,
	}
	if q.Dialect == "" {
		q.Dialect = DialectMySQL
	}
	return q
}

// SelectOne creates a new MySQLSelectQuery.
func (b mysqlQueryBuilder) SelectOne() MySQLSelectQuery {
	q := MySQLSelectQuery{
		CTEs:         b.ctes,
		SelectFields: Fields{Expr("1")},
	}
	if q.Dialect == "" {
		q.Dialect = DialectMySQL
	}
	return q
}

// From creates a new MySQLSelectQuery.
func (b mysqlQueryBuilder) From(table Table) MySQLSelectQuery {
	q := MySQLSelectQuery{
		CTEs:      b.ctes,
		FromTable: table,
	}
	if q.Dialect == "" {
		q.Dialect = DialectMySQL
	}
	return q
}

// Select appends to the SelectFields in the MySQLSelectQuery.
func (q MySQLSelectQuery) Select(fields ...Field) MySQLSelectQuery {
	q.SelectFields = append(q.SelectFields, fields...)
	return q
}

// SelectDistinct sets the SelectFields in the MySQLSelectQuery.
func (q MySQLSelectQuery) SelectDistinct(fields ...Field) MySQLSelectQuery {
	q.SelectFields = fields
	q.Distinct = true
	return q
}

// SelectOne sets the MySQLSelectQuery to SELECT 1.
func (q MySQLSelectQuery) SelectOne(fields ...Field) MySQLSelectQuery {
	q.SelectFields = Fields{Expr("1")}
	return q
}

// From sets the FromTable field in the MySQLSelectQuery.
func (q MySQLSelectQuery) From(table Table) MySQLSelectQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the MySQLSelectQuery.
func (q MySQLSelectQuery) Join(table Table, predicates ...Predicate) MySQLSelectQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the MySQLSelectQuery.
func (q MySQLSelectQuery) LeftJoin(table Table, predicates ...Predicate) MySQLSelectQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the MySQLSelectQuery.
func (q MySQLSelectQuery) FullJoin(table Table, predicates ...Predicate) MySQLSelectQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the MySQLSelectQuery.
func (q MySQLSelectQuery) CrossJoin(table Table) MySQLSelectQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the MySQLSelectQuery with a custom join
// operator.
func (q MySQLSelectQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) MySQLSelectQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the MySQLSelectQuery with the USING operator.
func (q MySQLSelectQuery) JoinUsing(table Table, fields ...Field) MySQLSelectQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field in the MySQLSelectQuery.
func (q MySQLSelectQuery) Where(predicates ...Predicate) MySQLSelectQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// GroupBy appends to the GroupByFields field in the MySQLSelectQuery.
func (q MySQLSelectQuery) GroupBy(fields ...Field) MySQLSelectQuery {
	q.GroupByFields = append(q.GroupByFields, fields...)
	return q
}

// Having appends to the HavingPredicate field in the MySQLSelectQuery.
func (q MySQLSelectQuery) Having(predicates ...Predicate) MySQLSelectQuery {
	q.HavingPredicate = appendPredicates(q.HavingPredicate, predicates)
	return q
}

// OrderBy appends to the OrderByFields field in the MySQLSelectQuery.
func (q MySQLSelectQuery) OrderBy(fields ...Field) MySQLSelectQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Limit sets the LimitRows field in the MySQLSelectQuery.
func (q MySQLSelectQuery) Limit(limit any) MySQLSelectQuery {
	q.LimitRows = limit
	return q
}

// Offset sets the OffsetRows field in the MySQLSelectQuery.
func (q MySQLSelectQuery) Offset(offset any) MySQLSelectQuery {
	q.OffsetRows = offset
	return q
}

// LockRows sets the lock clause of the MySQLSelectQuery.
func (q MySQLSelectQuery) LockRows(lockClause string, lockValues ...any) MySQLSelectQuery {
	q.LockClause = lockClause
	q.LockValues = lockValues
	return q
}

// As returns a new MySQLSelectQuery with the table alias (and optionally
// column aliases).
func (q MySQLSelectQuery) As(alias string, columns ...string) MySQLSelectQuery {
	q.Alias = alias
	q.Columns = columns
	return q
}

// Field returns a new field qualified by the MySQLSelectQuery's alias.
func (q MySQLSelectQuery) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: q.Alias})
}

// SetFetchableFields implements the Query interface.
func (q MySQLSelectQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	if len(q.SelectFields) == 0 {
		q.SelectFields = fields
		return q, true
	}
	return q, false
}

// GetFetchableFields returns the fetchable fields of the query.
func (q MySQLSelectQuery) GetFetchableFields() []Field {
	return q.SelectFields
}

// GetDialect implements the Query interface.
func (q MySQLSelectQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q MySQLSelectQuery) SetDialect(dialect string) MySQLSelectQuery {
	q.Dialect = dialect
	return q
}

// GetAlias returns the alias of the MySQLSelectQuery.
func (q MySQLSelectQuery) GetAlias() string { return q.Alias }

// IsTable implements the Table interface.
func (q MySQLSelectQuery) IsTable() {}

// IsField implements the Field interface.
func (q MySQLSelectQuery) IsField() {}

// IsArray implements the Array interface.
func (q MySQLSelectQuery) IsArray() {}

// IsBinary implements the Binary interface.
func (q MySQLSelectQuery) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (q MySQLSelectQuery) IsBoolean() {}

// IsEnum implements the Enum interface.
func (q MySQLSelectQuery) IsEnum() {}

// IsJSON implements the JSON interface.
func (q MySQLSelectQuery) IsJSON() {}

// IsNumber implements the Number interface.
func (q MySQLSelectQuery) IsNumber() {}

// IsString implements the String interface.
func (q MySQLSelectQuery) IsString() {}

// IsTime implements the Time interface.
func (q MySQLSelectQuery) IsTime() {}

// IsUUID implements the UUID interface.
func (q MySQLSelectQuery) IsUUID() {}

// SQLServerSelectQuery represents an SQL Server SELECT query.
type SQLServerSelectQuery SelectQuery

var _ interface {
	Query
	Table
	Field
	Any
} = (*SQLServerSelectQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLServerSelectQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return SelectQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Select creates a new SQLServerSelectQuery.
func (b sqlserverQueryBuilder) Select(fields ...Field) SQLServerSelectQuery {
	q := SQLServerSelectQuery{
		CTEs:         b.ctes,
		SelectFields: fields,
	}
	if q.Dialect == "" {
		q.Dialect = DialectSQLServer
	}
	return q
}

// SelectDistinct creates a new SQLServerSelectQuery.
func (b sqlserverQueryBuilder) SelectDistinct(fields ...Field) SQLServerSelectQuery {
	q := SQLServerSelectQuery{
		CTEs:         b.ctes,
		SelectFields: fields,
		Distinct:     true,
	}
	if q.Dialect == "" {
		q.Dialect = DialectSQLServer
	}
	return q
}

// SelectOne creates a new SQLServerSelectQuery.
func (b sqlserverQueryBuilder) SelectOne() SQLServerSelectQuery {
	q := SQLServerSelectQuery{
		CTEs:         b.ctes,
		SelectFields: Fields{Expr("1")},
	}
	if q.Dialect == "" {
		q.Dialect = DialectSQLServer
	}
	return q
}

// From creates a new SQLServerSelectQuery.
func (b sqlserverQueryBuilder) From(table Table) SQLServerSelectQuery {
	q := SQLServerSelectQuery{
		CTEs:      b.ctes,
		FromTable: table,
	}
	if q.Dialect == "" {
		q.Dialect = DialectSQLServer
	}
	return q
}

// Select appends to the SelectFields in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) Select(fields ...Field) SQLServerSelectQuery {
	q.SelectFields = append(q.SelectFields, fields...)
	return q
}

// SelectDistinct sets the SelectFields in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) SelectDistinct(fields ...Field) SQLServerSelectQuery {
	q.SelectFields = fields
	q.Distinct = true
	return q
}

// SelectOne sets the SQLServerSelectQuery to SELECT 1.
func (q SQLServerSelectQuery) SelectOne(fields ...Field) SQLServerSelectQuery {
	q.SelectFields = Fields{Expr("1")}
	return q
}

// Top sets the LimitTop field of the SQLServerSelectQuery.
func (q SQLServerSelectQuery) Top(limit any) SQLServerSelectQuery {
	q.LimitTop = limit
	return q
}

// Top sets the LimitTopPercent field of the SQLServerSelectQuery.
func (q SQLServerSelectQuery) TopPercent(percentLimit any) SQLServerSelectQuery {
	q.LimitTopPercent = percentLimit
	return q
}

// From sets the FromTable field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) From(table Table) SQLServerSelectQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the SQLServerSelectQuery.
func (q SQLServerSelectQuery) Join(table Table, predicates ...Predicate) SQLServerSelectQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the SQLServerSelectQuery.
func (q SQLServerSelectQuery) LeftJoin(table Table, predicates ...Predicate) SQLServerSelectQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the SQLServerSelectQuery.
func (q SQLServerSelectQuery) FullJoin(table Table, predicates ...Predicate) SQLServerSelectQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the SQLServerSelectQuery.
func (q SQLServerSelectQuery) CrossJoin(table Table) SQLServerSelectQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the SQLServerSelectQuery with a custom join
// operator.
func (q SQLServerSelectQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) SQLServerSelectQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// Where appends to the WherePredicate field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) Where(predicates ...Predicate) SQLServerSelectQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// GroupBy appends to the GroupByFields field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) GroupBy(fields ...Field) SQLServerSelectQuery {
	q.GroupByFields = append(q.GroupByFields, fields...)
	return q
}

// Having appends to the HavingPredicate field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) Having(predicates ...Predicate) SQLServerSelectQuery {
	q.HavingPredicate = appendPredicates(q.HavingPredicate, predicates)
	return q
}

// OrderBy appends to the OrderByFields field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) OrderBy(fields ...Field) SQLServerSelectQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Offset sets the OffsetRows field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) Offset(offset any) SQLServerSelectQuery {
	q.OffsetRows = offset
	return q
}

// FetchNext sets the FetchNextRows field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) FetchNext(n any) SQLServerSelectQuery {
	q.FetchNextRows = n
	return q
}

// WithTies enables the FetchWithTies field in the SQLServerSelectQuery.
func (q SQLServerSelectQuery) WithTies() SQLServerSelectQuery {
	q.FetchWithTies = true
	return q
}

// As returns a new SQLServerSelectQuery with the table alias (and optionally
// column aliases).
func (q SQLServerSelectQuery) As(alias string, columns ...string) SQLServerSelectQuery {
	q.Alias = alias
	q.Columns = columns
	return q
}

// Field returns a new field qualified by the SQLServerSelectQuery's alias.
func (q SQLServerSelectQuery) Field(name string) AnyField {
	return NewAnyField(name, TableStruct{alias: q.Alias})
}

// SetFetchableFields implements the Query interface.
func (q SQLServerSelectQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	if len(q.SelectFields) == 0 {
		q.SelectFields = fields
		return q, true
	}
	return q, false
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SQLServerSelectQuery) GetFetchableFields() []Field {
	return q.SelectFields
}

// GetDialect implements the Query interface.
func (q SQLServerSelectQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q SQLServerSelectQuery) SetDialect(dialect string) SQLServerSelectQuery {
	q.Dialect = dialect
	return q
}

// GetAlias returns the alias of the SQLServerSelectQuery.
func (q SQLServerSelectQuery) GetAlias() string { return q.Alias }

// IsTable implements the Table interface.
func (q SQLServerSelectQuery) IsTable() {}

// IsField implements the Field interface.
func (q SQLServerSelectQuery) IsField() {}

// IsArray implements the Array interface.
func (q SQLServerSelectQuery) IsArray() {}

// IsBinary implements the Binary interface.
func (q SQLServerSelectQuery) IsBinary() {}

// IsBoolean implements the Boolean interface.
func (q SQLServerSelectQuery) IsBoolean() {}

// IsEnum implements the Enum interface.
func (q SQLServerSelectQuery) IsEnum() {}

// IsJSON implements the JSON interface.
func (q SQLServerSelectQuery) IsJSON() {}

// IsNumber implements the Number interface.
func (q SQLServerSelectQuery) IsNumber() {}

// IsString implements the String interface.
func (q SQLServerSelectQuery) IsString() {}

// IsTime implements the Time interface.
func (q SQLServerSelectQuery) IsTime() {}

// IsUUID implements the UUID interface.
func (q SQLServerSelectQuery) IsUUID() {}
