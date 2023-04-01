package sq

import (
	"bytes"
	"context"
	"fmt"
)

// DeleteQuery represents an SQL DELETE query.
type DeleteQuery struct {
	Dialect string
	// WITH
	CTEs []CTE
	// DELETE FROM
	DeleteTable  Table
	DeleteTables []Table
	// USING
	UsingTable Table
	JoinTables []JoinTable
	// WHERE
	WherePredicate Predicate
	// ORDER BY
	OrderByFields Fields
	// LIMIT
	LimitRows any
	// OFFSET
	OffsetRows any
	// RETURNING
	ReturningFields []Field
}

var _ Query = (*DeleteQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q DeleteQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	// Table Policies
	var policies []Predicate
	policies, err = appendPolicy(ctx, dialect, policies, q.DeleteTable)
	if err != nil {
		return fmt.Errorf("DELETE FROM %s Policy: %w", toString(q.Dialect, q.DeleteTable), err)
	}
	policies, err = appendPolicy(ctx, dialect, policies, q.UsingTable)
	if err != nil {
		return fmt.Errorf("USING %s Policy: %w", toString(q.Dialect, q.UsingTable), err)
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
	// DELETE FROM
	if (dialect == DialectMySQL || dialect == DialectSQLServer) && len(q.DeleteTables) > 0 {
		buf.WriteString("DELETE ")
		if len(q.DeleteTables) > 1 && dialect != DialectMySQL {
			return fmt.Errorf("dialect %q does not support multi-table DELETE", dialect)
		}
		for i, table := range q.DeleteTables {
			if i > 0 {
				buf.WriteString(", ")
			}
			if alias := getAlias(table); alias != "" {
				buf.WriteString(alias)
			} else {
				err = table.WriteSQL(ctx, dialect, buf, args, params)
				if err != nil {
					return fmt.Errorf("table #%d: %w", i+1, err)
				}
			}
		}
	} else {
		buf.WriteString("DELETE FROM ")
		if q.DeleteTable == nil {
			return fmt.Errorf("no table provided to DELETE FROM")
		}
		err = q.DeleteTable.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("DELETE FROM: %w", err)
		}
		if dialect != DialectSQLServer {
			if alias := getAlias(q.DeleteTable); alias != "" {
				buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
			}
		}
	}
	if q.UsingTable != nil || len(q.JoinTables) > 0 {
		if dialect != DialectPostgres && dialect != DialectMySQL && dialect != DialectSQLServer {
			return fmt.Errorf("%s DELETE does not support JOIN", dialect)
		}
	}
	// OUTPUT
	if len(q.ReturningFields) > 0 && dialect == DialectSQLServer {
		buf.WriteString(" OUTPUT ")
		err = writeFieldsWithPrefix(ctx, dialect, buf, args, params, q.ReturningFields, "DELETED", true)
		if err != nil {
			return err
		}
	}
	// USING/FROM
	if q.UsingTable != nil {
		switch dialect {
		case DialectPostgres:
			buf.WriteString(" USING ")
			err = q.UsingTable.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("USING: %w", err)
			}
		case DialectMySQL, DialectSQLServer:
			buf.WriteString(" FROM ")
			err = q.UsingTable.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("FROM: %w", err)
			}
		}
		if alias := getAlias(q.UsingTable); alias != "" {
			buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
		}
	}
	// JOIN
	if len(q.JoinTables) > 0 {
		if q.UsingTable == nil {
			return fmt.Errorf("%s can't JOIN without a USING/FROM table", dialect)
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
	// ORDER BY
	if len(q.OrderByFields) > 0 {
		if dialect != DialectMySQL {
			return fmt.Errorf("%s UPDATE does not support ORDER BY", dialect)
		}
		buf.WriteString(" ORDER BY ")
		err = q.OrderByFields.WriteSQL(ctx, dialect, buf, args, params)
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
		if dialect != DialectPostgres && dialect != DialectSQLite && dialect != DialectMySQL {
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

// DeleteFrom returns a new DeleteQuery.
func DeleteFrom(table Table) DeleteQuery {
	return DeleteQuery{DeleteTable: table}
}

// Where appends to the WherePredicate field of the DeleteQuery.
func (q DeleteQuery) Where(predicates ...Predicate) DeleteQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// SetFetchableFields implements the Query interface.
func (q DeleteQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	switch q.Dialect {
	case DialectPostgres, DialectSQLite:
		q.ReturningFields = fields
		return q, true
	default:
		return q, false
	}
}

// GetFetchableFields returns the fetchable fields of the query.
func (q DeleteQuery) GetFetchableFields() []Field {
	switch q.Dialect {
	case DialectPostgres, DialectSQLite:
		return q.ReturningFields
	default:
		return nil
	}
}

// GetDialect implements the Query interface.
func (q DeleteQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q DeleteQuery) SetDialect(dialect string) DeleteQuery {
	q.Dialect = dialect
	return q
}

// SQLiteDeleteQuery represents an SQLite DELETE query.
type SQLiteDeleteQuery DeleteQuery

var _ Query = (*SQLiteDeleteQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLiteDeleteQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return DeleteQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// DeleteFrom returns a new SQLiteDeleteQuery.
func (b sqliteQueryBuilder) DeleteFrom(table Table) SQLiteDeleteQuery {
	return SQLiteDeleteQuery{
		Dialect:     DialectSQLite,
		CTEs:        b.ctes,
		DeleteTable: table,
	}
}

// Where appends to the WherePredicate field of the SQLiteDeleteQuery.
func (q SQLiteDeleteQuery) Where(predicates ...Predicate) SQLiteDeleteQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// Returning appends fields to the RETURNING clause of the SQLiteDeleteQuery.
func (q SQLiteDeleteQuery) Returning(fields ...Field) SQLiteDeleteQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q SQLiteDeleteQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return DeleteQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SQLiteDeleteQuery) GetFetchableFields() []Field {
	return DeleteQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q SQLiteDeleteQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q SQLiteDeleteQuery) SetDialect(dialect string) SQLiteDeleteQuery {
	q.Dialect = dialect
	return q
}

// PostgresDeleteQuery represents a Postgres DELETE query.
type PostgresDeleteQuery DeleteQuery

var _ Query = (*PostgresDeleteQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q PostgresDeleteQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return DeleteQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// DeleteFrom returns a new PostgresDeleteQuery.
func (b postgresQueryBuilder) DeleteFrom(table Table) PostgresDeleteQuery {
	return PostgresDeleteQuery{
		Dialect:     DialectPostgres,
		CTEs:        b.ctes,
		DeleteTable: table,
	}
}

// Using sets the UsingTable field of the PostgresDeleteQuery.
func (q PostgresDeleteQuery) Using(table Table) PostgresDeleteQuery {
	q.UsingTable = table
	return q
}

// Join joins a new Table to the PostgresDeleteQuery.
func (q PostgresDeleteQuery) Join(table Table, predicates ...Predicate) PostgresDeleteQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the PostgresDeleteQuery.
func (q PostgresDeleteQuery) LeftJoin(table Table, predicates ...Predicate) PostgresDeleteQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the PostgresDeleteQuery.
func (q PostgresDeleteQuery) FullJoin(table Table, predicates ...Predicate) PostgresDeleteQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the PostgresDeleteQuery.
func (q PostgresDeleteQuery) CrossJoin(table Table) PostgresDeleteQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the PostgresDeleteQuery with a custom join
// operator.
func (q PostgresDeleteQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) PostgresDeleteQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the PostgresDeleteQuery with the USING operator.
func (q PostgresDeleteQuery) JoinUsing(table Table, fields ...Field) PostgresDeleteQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field of the PostgresDeleteQuery.
func (q PostgresDeleteQuery) Where(predicates ...Predicate) PostgresDeleteQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// Returning appends fields to the RETURNING clause of the PostgresDeleteQuery.
func (q PostgresDeleteQuery) Returning(fields ...Field) PostgresDeleteQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q PostgresDeleteQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return DeleteQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q PostgresDeleteQuery) GetFetchableFields() []Field {
	return DeleteQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q PostgresDeleteQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q PostgresDeleteQuery) SetDialect(dialect string) PostgresDeleteQuery {
	q.Dialect = dialect
	return q
}

// MySQLDeleteQuery represents a MySQL DELETE query.
type MySQLDeleteQuery DeleteQuery

var _ Query = (*MySQLDeleteQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q MySQLDeleteQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return DeleteQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// DeleteFrom returns a new MySQLDeleteQuery.
func (b mysqlQueryBuilder) DeleteFrom(table Table) MySQLDeleteQuery {
	return MySQLDeleteQuery{
		Dialect:     DialectMySQL,
		CTEs:        b.ctes,
		DeleteTable: table,
	}
}

// Delete returns a new MySQLDeleteQuery.
func (b mysqlQueryBuilder) Delete(tables ...Table) MySQLDeleteQuery {
	return MySQLDeleteQuery{
		Dialect:      DialectMySQL,
		CTEs:         b.ctes,
		DeleteTables: tables,
	}
}

// From sets the UsingTable of the MySQLDeleteQuery.
func (q MySQLDeleteQuery) From(table Table) MySQLDeleteQuery {
	q.UsingTable = table
	return q
}

// Join joins a new Table to the MySQLDeleteQuery.
func (q MySQLDeleteQuery) Join(table Table, predicates ...Predicate) MySQLDeleteQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the MySQLDeleteQuery.
func (q MySQLDeleteQuery) LeftJoin(table Table, predicates ...Predicate) MySQLDeleteQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the MySQLDeleteQuery.
func (q MySQLDeleteQuery) FullJoin(table Table, predicates ...Predicate) MySQLDeleteQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the MySQLDeleteQuery.
func (q MySQLDeleteQuery) CrossJoin(table Table) MySQLDeleteQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the MySQLDeleteQuery with a custom join
// operator.
func (q MySQLDeleteQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) MySQLDeleteQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// JoinUsing joins a new Table to the MySQLDeleteQuery with the USING operator.
func (q MySQLDeleteQuery) JoinUsing(table Table, fields ...Field) MySQLDeleteQuery {
	q.JoinTables = append(q.JoinTables, JoinUsing(table, fields...))
	return q
}

// Where appends to the WherePredicate field of the MySQLDeleteQuery.
func (q MySQLDeleteQuery) Where(predicates ...Predicate) MySQLDeleteQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// OrderBy sets the OrderByFields field of the MySQLDeleteQuery.
func (q MySQLDeleteQuery) OrderBy(fields ...Field) MySQLDeleteQuery {
	q.OrderByFields = append(q.OrderByFields, fields...)
	return q
}

// Limit sets the LimitRows field of the MySQLDeleteQuery.
func (q MySQLDeleteQuery) Limit(limit any) MySQLDeleteQuery {
	q.LimitRows = limit
	return q
}

// Returning appends fields to the RETURNING clause of the MySQLDeleteQuery.
func (q MySQLDeleteQuery) Returning(fields ...Field) MySQLDeleteQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q MySQLDeleteQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return DeleteQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q MySQLDeleteQuery) GetFetchableFields() []Field {
	return DeleteQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q MySQLDeleteQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q MySQLDeleteQuery) SetDialect(dialect string) MySQLDeleteQuery {
	q.Dialect = dialect
	return q
}

// SQLServerDeleteQuery represents an SQL Server DELETE query.
type SQLServerDeleteQuery DeleteQuery

var _ Query = (*SQLServerDeleteQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLServerDeleteQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return DeleteQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// DeleteFrom returns a new SQLServerDeleteQuery.
func (b sqlserverQueryBuilder) DeleteFrom(table Table) SQLServerDeleteQuery {
	return SQLServerDeleteQuery{
		Dialect:     DialectSQLServer,
		CTEs:        b.ctes,
		DeleteTable: table,
	}
}

// Delete returns a new SQLServerDeleteQuery.
func (b sqlserverQueryBuilder) Delete(table Table) SQLServerDeleteQuery {
	return SQLServerDeleteQuery{
		Dialect:      DialectSQLServer,
		CTEs:         b.ctes,
		DeleteTables: []Table{table},
	}
}

// From sets the UsingTable of the SQLServerDeleteQuery.
func (q SQLServerDeleteQuery) From(table Table) SQLServerDeleteQuery {
	q.UsingTable = table
	return q
}

// Join joins a new Table to the SQLServerDeleteQuery.
func (q SQLServerDeleteQuery) Join(table Table, predicates ...Predicate) SQLServerDeleteQuery {
	q.JoinTables = append(q.JoinTables, Join(table, predicates...))
	return q
}

// LeftJoin left joins a new Table to the SQLServerDeleteQuery.
func (q SQLServerDeleteQuery) LeftJoin(table Table, predicates ...Predicate) SQLServerDeleteQuery {
	q.JoinTables = append(q.JoinTables, LeftJoin(table, predicates...))
	return q
}

// FullJoin full joins a new Table to the SQLServerDeleteQuery.
func (q SQLServerDeleteQuery) FullJoin(table Table, predicates ...Predicate) SQLServerDeleteQuery {
	q.JoinTables = append(q.JoinTables, FullJoin(table, predicates...))
	return q
}

// CrossJoin cross joins a new Table to the SQLServerDeleteQuery.
func (q SQLServerDeleteQuery) CrossJoin(table Table) SQLServerDeleteQuery {
	q.JoinTables = append(q.JoinTables, CrossJoin(table))
	return q
}

// CustomJoin joins a new Table to the SQLServerDeleteQuery with a custom join
// operator.
func (q SQLServerDeleteQuery) CustomJoin(joinOperator string, table Table, predicates ...Predicate) SQLServerDeleteQuery {
	q.JoinTables = append(q.JoinTables, CustomJoin(joinOperator, table, predicates...))
	return q
}

// Where appends to the WherePredicate field of the SQLServerDeleteQuery.
func (q SQLServerDeleteQuery) Where(predicates ...Predicate) SQLServerDeleteQuery {
	q.WherePredicate = appendPredicates(q.WherePredicate, predicates)
	return q
}

// SetFetchableFields implements the Query interface.
func (q SQLServerDeleteQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return DeleteQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SQLServerDeleteQuery) GetFetchableFields() []Field {
	return DeleteQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q SQLServerDeleteQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q SQLServerDeleteQuery) SetDialect(dialect string) SQLServerDeleteQuery {
	q.Dialect = dialect
	return q
}
