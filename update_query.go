package sq

import (
	"bytes"
	"context"
	"fmt"
)

// UpdateQuery represents an SQL UPDATE query.
type UpdateQuery struct {
	Dialect      string
	ColumnMapper func(*Column) error
	// WITH
	CTEs []CTE
	// UPDATE
	UpdateTable Table
	// FROM
	FromTable  Table
	JoinTables []JoinTable
	// SET
	Assignments []Assignment
	// WHERE
	WherePredicate Predicate
	// ORDER BY
	OrderByFields []Field
	// LIMIT
	LimitRows any
	// RETURNING
	ReturningFields []Field
}

var _ Query = (*UpdateQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q UpdateQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	if q.ColumnMapper != nil {
		col := &Column{
			dialect:  q.Dialect,
			isUpdate: true,
		}
		err = q.ColumnMapper(col)
		if err != nil {
			return err
		}
		q.Assignments = col.assignments
	}
	// Table Policies
	var policies []Predicate
	policies, err = appendPolicy(ctx, dialect, policies, q.UpdateTable)
	if err != nil {
		return fmt.Errorf("UPDATE %s Policy: %w", toString(q.Dialect, q.UpdateTable), err)
	}
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
	// UPDATE
	buf.WriteString("UPDATE ")
	if q.UpdateTable == nil {
		return fmt.Errorf("no table provided to UPDATE")
	}
	err = q.UpdateTable.WriteSQL(ctx, dialect, buf, args, params)
	if err != nil {
		return fmt.Errorf("UPDATE: %w", err)
	}
	if dialect != DialectSQLServer {
		if alias := getAlias(q.UpdateTable); alias != "" {
			buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
		}
	}
	if len(q.Assignments) == 0 {
		return fmt.Errorf("no fields to update")
	}
	// SET (not mysql)
	if dialect != DialectMySQL {
		buf.WriteString(" SET ")
		err = Assignments(q.Assignments).WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("SET: %w", err)
		}
	}
	// OUTPUT
	if len(q.ReturningFields) > 0 && dialect == DialectSQLServer {
		buf.WriteString(" OUTPUT ")
		err = writeFieldsWithPrefix(ctx, dialect, buf, args, params, q.ReturningFields, "INSERTED", true)
		if err != nil {
			return err
		}
	}
	// FROM
	if q.FromTable != nil {
		if dialect == DialectMySQL {
			return fmt.Errorf("mysql UPDATE does not support FROM")
		}
		buf.WriteString(" FROM ")
		err = q.FromTable.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("FROM: %w", err)
		}
		if alias := getAlias(q.FromTable); alias != "" {
			buf.WriteString(" AS " + QuoteIdentifier(dialect, alias) + quoteTableColumns(dialect, q.FromTable))
		}
	}
	// JOIN
	if len(q.JoinTables) > 0 {
		if q.FromTable == nil && dialect != DialectMySQL {
			return fmt.Errorf("%s can't JOIN without a FROM table", dialect)
		}
		buf.WriteString(" ")
		err = writeJoinTables(ctx, dialect, buf, args, params, q.JoinTables)
		if err != nil {
			return fmt.Errorf("JOIN: %w", err)
		}
	}
	// SET (mysql)
	if dialect == DialectMySQL {
		buf.WriteString(" SET ")
		err = Assignments(q.Assignments).WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("SET: %w", err)
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
	// ORDER BY
	if len(q.OrderByFields) > 0 {
		if dialect != DialectMySQL {
			return fmt.Errorf("%s UPDATE does not support ORDER BY", dialect)
		}
		buf.WriteString(" ORDER BY ")
		err = writeFields(ctx, dialect, buf, args, params, q.OrderByFields, false)
		if err != nil {
			return fmt.Errorf("ORDER BY: %w", err)
		}
	}
	// LIMIT
	if q.LimitRows != nil {
		if dialect != DialectMySQL {
			return fmt.Errorf("%s UPDATE does not support LIMIT", dialect)
		}
		buf.WriteString(" LIMIT ")
		err = WriteValue(ctx, dialect, buf, args, params, q.LimitRows)
		if err != nil {
			return fmt.Errorf("LIMIT: %w", err)
		}
	}
	// RETURNING
	if len(q.ReturningFields) > 0 && dialect != DialectSQLServer {
		if dialect != DialectPostgres && dialect != DialectSQLite {
			return fmt.Errorf("%s UPDATE does not support RETURNING", dialect)
		}
		buf.WriteString(" RETURNING ")
		err = writeFields(ctx, dialect, buf, args, params, q.ReturningFields, true)
		if err != nil {
			return fmt.Errorf("RETURNING: %w", err)
		}
	}
	return nil
}

// Update returns a new UpdateQuery.
func Update(table Table) UpdateQuery {
	return UpdateQuery{UpdateTable: table}
}

// Set sets the Assignments field of the UpdateQuery.
func (q UpdateQuery) Set(assignments ...Assignment) UpdateQuery {
	q.Assignments = append(q.Assignments, assignments...)
	return q
}

// SetFunc sets the ColumnMapper field of the UpdateQuery.
func (q UpdateQuery) SetFunc(colmapper func(*Column) error) UpdateQuery {
	q.ColumnMapper = colmapper
	return q
}

// Where appends to the WherePredicate field of the UpdateQuery.
func (q UpdateQuery) Where(predicates ...Predicate) UpdateQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// SetFetchableFields implements the Query interface.
func (q UpdateQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	switch q.Dialect {
	case DialectPostgres, DialectSQLite:
		q.ReturningFields = fields
		return q, true
	default:
		return q, false
	}
}

// GetFetchableFields returns the fetchable fields of the query.
func (q UpdateQuery) GetFetchableFields() []Field {
	switch q.Dialect {
	case DialectPostgres, DialectSQLite:
		return q.ReturningFields
	default:
		return nil
	}
}

// GetDialect implements the Query interface.
func (q UpdateQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q UpdateQuery) SetDialect(dialect string) UpdateQuery {
	q.Dialect = dialect
	return q
}

// SQLiteUpdateQuery represents an SQLite UPDATE query.
type SQLiteUpdateQuery UpdateQuery

var _ Query = (*SQLiteUpdateQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLiteUpdateQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return UpdateQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Update returns a new SQLiteUpdateQuery.
func (b sqliteQueryBuilder) Update(table Table) SQLiteUpdateQuery {
	return SQLiteUpdateQuery{
		Dialect:     DialectSQLite,
		CTEs:        b.ctes,
		UpdateTable: table,
	}
}

// Set sets the Assignments field of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) Set(assignments ...Assignment) SQLiteUpdateQuery {
	q.Assignments = append(q.Assignments, assignments...)
	return q
}

// SetFunc sets the ColumnMapper of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) SetFunc(colmapper func(*Column) error) SQLiteUpdateQuery {
	q.ColumnMapper = colmapper
	return q
}

// From sets the FromTable field of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) From(table Table) SQLiteUpdateQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) Join(table Table, predicates ...Predicate) SQLiteUpdateQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) LeftJoin(table Table, predicates ...Predicate) SQLiteUpdateQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) CrossJoin(table Table) SQLiteUpdateQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the SQLiteUpdateQuery with a custom join
// operator.
func (q SQLiteUpdateQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) SQLiteUpdateQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the SQLiteUpdateQuery with the USING operator.
func (q SQLiteUpdateQuery) JoinUsing(table Table, fields ...Field) SQLiteUpdateQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) Where(predicates ...Predicate) SQLiteUpdateQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// Returning sets the ReturningFields field of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) Returning(fields ...Field) SQLiteUpdateQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q SQLiteUpdateQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return UpdateQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) GetFetchableFields() []Field {
	return UpdateQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q SQLiteUpdateQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the SQLiteUpdateQuery.
func (q SQLiteUpdateQuery) SetDialect(dialect string) SQLiteUpdateQuery {
	q.Dialect = dialect
	return q
}

// PostgresUpdateQuery represents a Postgres UPDATE query.
type PostgresUpdateQuery UpdateQuery

var _ Query = (*PostgresUpdateQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q PostgresUpdateQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return UpdateQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Update returns a new PostgresUpdateQuery.
func (b postgresQueryBuilder) Update(table Table) PostgresUpdateQuery {
	return PostgresUpdateQuery{
		Dialect:     DialectPostgres,
		CTEs:        b.ctes,
		UpdateTable: table,
	}
}

// Set sets the Assignments field of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) Set(assignments ...Assignment) PostgresUpdateQuery {
	q.Assignments = append(q.Assignments, assignments...)
	return q
}

// SetFunc sets the ColumnMapper of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) SetFunc(colmapper func(*Column) error) PostgresUpdateQuery {
	q.ColumnMapper = colmapper
	return q
}

// From sets the FromTable field of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) From(table Table) PostgresUpdateQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the PostgresUpdateQuery.
func (q PostgresUpdateQuery) Join(table Table, predicates ...Predicate) PostgresUpdateQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the PostgresUpdateQuery.
func (q PostgresUpdateQuery) LeftJoin(table Table, predicates ...Predicate) PostgresUpdateQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the PostgresUpdateQuery.
func (q PostgresUpdateQuery) FullJoin(table Table, predicates ...Predicate) PostgresUpdateQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the PostgresUpdateQuery.
func (q PostgresUpdateQuery) CrossJoin(table Table) PostgresUpdateQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the PostgresUpdateQuery with a custom join
// operator.
func (q PostgresUpdateQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) PostgresUpdateQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the PostgresUpdateQuery with the USING operator.
func (q PostgresUpdateQuery) JoinUsing(table Table, fields ...Field) PostgresUpdateQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) Where(predicates ...Predicate) PostgresUpdateQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// Returning sets the ReturningFields field of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) Returning(fields ...Field) PostgresUpdateQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q PostgresUpdateQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return UpdateQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) GetFetchableFields() []Field {
	return UpdateQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q PostgresUpdateQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the PostgresUpdateQuery.
func (q PostgresUpdateQuery) SetDialect(dialect string) PostgresUpdateQuery {
	q.Dialect = dialect
	return q
}

// MySQLUpdateQuery represents a MySQL UPDATE query.
type MySQLUpdateQuery UpdateQuery

var _ Query = (*MySQLUpdateQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q MySQLUpdateQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return UpdateQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Update returns a new MySQLUpdateQuery.
func (b mysqlQueryBuilder) Update(table Table) MySQLUpdateQuery {
	q := MySQLUpdateQuery{
		Dialect:     DialectMySQL,
		CTEs:        b.ctes,
		UpdateTable: table,
	}
	return q
}

// Join joins a new Table to the MySQLUpdateQuery.
func (q MySQLUpdateQuery) Join(table Table, predicates ...Predicate) MySQLUpdateQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the MySQLUpdateQuery.
func (q MySQLUpdateQuery) LeftJoin(table Table, predicates ...Predicate) MySQLUpdateQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the MySQLUpdateQuery.
func (q MySQLUpdateQuery) FullJoin(table Table, predicates ...Predicate) MySQLUpdateQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the MySQLUpdateQuery.
func (q MySQLUpdateQuery) CrossJoin(table Table) MySQLUpdateQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the MySQLUpdateQuery with a custom join
// operator.
func (q MySQLUpdateQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) MySQLUpdateQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the MySQLUpdateQuery with the USING operator.
func (q MySQLUpdateQuery) JoinUsing(table Table, fields ...Field) MySQLUpdateQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Set sets the Assignments field of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) Set(assignments ...Assignment) MySQLUpdateQuery {
	q.Assignments = append(q.Assignments, assignments...)
	return q
}

// SetFunc sets the ColumnMapper of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) SetFunc(colmapper func(*Column) error) MySQLUpdateQuery {
	q.ColumnMapper = colmapper
	return q
}

// Where appends to the WherePredicate field of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) Where(predicates ...Predicate) MySQLUpdateQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// OrderBy sets the OrderByFields of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) OrderBy(fields ...Field) MySQLUpdateQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Limit sets the LimitRows field of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) Limit(limit any) MySQLUpdateQuery {
	q.LimitRows = limit
	return q
}

// SetFetchableFields implements the Query interface.
func (q MySQLUpdateQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return UpdateQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) GetFetchableFields() []Field {
	return UpdateQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q MySQLUpdateQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the MySQLUpdateQuery.
func (q MySQLUpdateQuery) SetDialect(dialect string) MySQLUpdateQuery {
	q.Dialect = dialect
	return q
}

// SQLServerUpdateQuery represents an SQL Server UPDATE query.
type SQLServerUpdateQuery UpdateQuery

var _ Query = (*SQLServerUpdateQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLServerUpdateQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return UpdateQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// Update returns a new SQLServerUpdateQuery.
func (b sqlserverQueryBuilder) Update(table Table) SQLServerUpdateQuery {
	return SQLServerUpdateQuery{
		Dialect:     DialectSQLServer,
		CTEs:        b.ctes,
		UpdateTable: table,
	}
}

// Set sets the Assignments field of the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) Set(assignments ...Assignment) SQLServerUpdateQuery {
	q.Assignments = append(q.Assignments, assignments...)
	return q
}

// SetFunc sets the ColumnMapper of the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) SetFunc(colmapper func(*Column) error) SQLServerUpdateQuery {
	q.ColumnMapper = colmapper
	return q
}

// From sets the FromTable field of the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) From(table Table) SQLServerUpdateQuery {
	q.FromTable = table
	return q
}

// Join joins a new Table to the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) Join(table Table, predicates ...Predicate) SQLServerUpdateQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) LeftJoin(table Table, predicates ...Predicate) SQLServerUpdateQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) FullJoin(table Table, predicates ...Predicate) SQLServerUpdateQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) CrossJoin(table Table) SQLServerUpdateQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the SQLServerUpdateQuery with a custom join
// operator.
func (q SQLServerUpdateQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) SQLServerUpdateQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// Where appends to the WherePredicate field of the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) Where(predicates ...Predicate) SQLServerUpdateQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// SetFetchableFields implements the Query interface.
func (q SQLServerUpdateQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return UpdateQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) GetFetchableFields() []Field {
	return UpdateQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q SQLServerUpdateQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the SQLServerUpdateQuery.
func (q SQLServerUpdateQuery) SetDialect(dialect string) SQLServerUpdateQuery {
	q.Dialect = dialect
	return q
}
