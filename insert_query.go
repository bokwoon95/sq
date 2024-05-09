package sq

import (
	"bytes"
	"context"
	"fmt"
)

// InsertQuery represents an SQL INSERT query.
type InsertQuery struct {
	Dialect      string
	ColumnMapper func(*Column)
	// WITH
	CTEs []CTE
	// INSERT INTO
	InsertIgnore  bool
	InsertTable   Table
	InsertColumns []Field
	// VALUES
	RowValues []RowValue
	RowAlias  string
	// SELECT
	SelectQuery Query
	// ON CONFLICT
	Conflict ConflictClause
	// RETURNING
	ReturningFields []Field
}

var _ Query = (*InsertQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q InsertQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) (err error) {
	if q.ColumnMapper != nil {
		col := &Column{
			dialect:  q.Dialect,
			isUpdate: false,
		}
		defer mapperFunctionPanicked(&err)
		q.ColumnMapper(col)
		if err != nil {
			return err
		}
		q.InsertColumns, q.RowValues = col.insertColumns, col.rowValues
	}
	// WITH
	if len(q.CTEs) > 0 {
		if dialect == DialectMySQL {
			return fmt.Errorf("mysql does not support CTEs with INSERT")
		}
		err = writeCTEs(ctx, dialect, buf, args, params, q.CTEs)
		if err != nil {
			return fmt.Errorf("WITH: %w", err)
		}
	}
	// INSERT INTO
	if q.InsertIgnore {
		if dialect != DialectMySQL {
			return fmt.Errorf("%s does not support INSERT IGNORE", dialect)
		}
		buf.WriteString("INSERT IGNORE INTO ")
	} else {
		buf.WriteString("INSERT INTO ")
	}
	if q.InsertTable == nil {
		return fmt.Errorf("no table provided to INSERT")
	}
	err = q.InsertTable.WriteSQL(ctx, dialect, buf, args, params)
	if err != nil {
		return fmt.Errorf("INSERT INTO: %w", err)
	}
	if alias := getAlias(q.InsertTable); alias != "" {
		if dialect == DialectMySQL || dialect == DialectSQLServer {
			return fmt.Errorf("%s does not allow an alias for the INSERT table", dialect)
		}
		buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
	}
	// Columns
	if len(q.InsertColumns) > 0 {
		buf.WriteString(" (")
		err = writeFieldsWithPrefix(ctx, dialect, buf, args, params, q.InsertColumns, "", false)
		if err != nil {
			return fmt.Errorf("INSERT INTO: %w", err)
		}
		buf.WriteString(")")
	}
	// OUTPUT
	if len(q.ReturningFields) > 0 && dialect == DialectSQLServer {
		buf.WriteString(" OUTPUT ")
		for i, field := range q.ReturningFields {
			if i > 0 {
				buf.WriteString(", ")
			}
			err = WriteValue(ctx, dialect, buf, args, params, withPrefix(field, "INSERTED"))
			if err != nil {
				return err
			}
			if alias := getAlias(field); alias != "" {
				buf.WriteString(" AS " + QuoteIdentifier(dialect, alias))
			}
		}
	}
	// VALUES
	if len(q.RowValues) > 0 {
		buf.WriteString(" VALUES ")
		err = RowValues(q.RowValues).WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("VALUES: %w", err)
		}
		if q.RowAlias != "" {
			if dialect != DialectMySQL {
				return fmt.Errorf("%s does not support row aliases", dialect)
			}
			buf.WriteString(" AS " + q.RowAlias)
		}
	} else if q.SelectQuery != nil { // SELECT
		buf.WriteString(" ")
		err = q.SelectQuery.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("SELECT: %w", err)
		}
	} else {
		return fmt.Errorf("InsertQuery missing RowValues and SelectQuery (either one is required)")
	}
	// ON CONFLICT
	err = q.Conflict.WriteSQL(ctx, dialect, buf, args, params)
	if err != nil {
		return err
	}
	// RETURNING
	if len(q.ReturningFields) > 0 && dialect != DialectSQLServer {
		if dialect != DialectPostgres && dialect != DialectSQLite && dialect != DialectMySQL {
			return fmt.Errorf("%s INSERT does not support RETURNING", dialect)
		}
		buf.WriteString(" RETURNING ")
		err = writeFields(ctx, dialect, buf, args, params, q.ReturningFields, true)
		if err != nil {
			return fmt.Errorf("RETURNING: %w", err)
		}
	}
	return nil
}

// InsertInto creates a new InsertQuery.
func InsertInto(table Table) InsertQuery {
	return InsertQuery{InsertTable: table}
}

// Columns sets the InsertColumns field of the InsertQuery.
func (q InsertQuery) Columns(fields ...Field) InsertQuery {
	q.InsertColumns = fields
	return q
}

// Values sets the RowValues field of the InsertQuery.
func (q InsertQuery) Values(values ...any) InsertQuery {
	q.RowValues = append(q.RowValues, values)
	return q
}

// ColumnValues sets the ColumnMapper field of the InsertQuery.
func (q InsertQuery) ColumnValues(colmapper func(*Column)) InsertQuery {
	q.ColumnMapper = colmapper
	return q
}

// Select sets the SelectQuery field of the InsertQuery.
func (q InsertQuery) Select(query Query) InsertQuery {
	q.SelectQuery = query
	return q
}

// ConflictClause represents an SQL conflict clause e.g. ON CONFLICT DO
// NOTHING/DO UPDATE or ON DUPLICATE KEY UPDATE.
type ConflictClause struct {
	ConstraintName      string
	Fields              []Field
	Predicate           Predicate
	DoNothing           bool
	Resolution          []Assignment
	ResolutionPredicate Predicate
}

// WriteSQL implements the SQLWriter interface.
func (c ConflictClause) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	if c.ConstraintName == "" && len(c.Fields) == 0 && len(c.Resolution) == 0 && !c.DoNothing {
		return nil
	}
	if dialect != DialectSQLite && dialect != DialectPostgres && dialect != DialectMySQL {
		return nil
	}
	if dialect == DialectMySQL {
		if len(c.Resolution) > 0 {
			buf.WriteString(" ON DUPLICATE KEY UPDATE ")
			err = Assignments(c.Resolution).WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("ON DUPLICATE KEY UPDATE: %w", err)
			}
		}
		return nil
	}
	buf.WriteString(" ON CONFLICT")
	if c.ConstraintName != "" {
		buf.WriteString(" ON CONSTRAINT " + QuoteIdentifier(dialect, c.ConstraintName))
	} else if len(c.Fields) > 0 {
		buf.WriteString(" (")
		err = writeFieldsWithPrefix(ctx, dialect, buf, args, params, c.Fields, "", false)
		if err != nil {
			return fmt.Errorf("ON CONFLICT: %w", err)
		}
		buf.WriteString(")")
		if c.Predicate != nil {
			buf.WriteString(" WHERE ")
			switch predicate := c.Predicate.(type) {
			case VariadicPredicate:
				predicate.Toplevel = true
				err = predicate.WriteSQL(ctx, dialect, buf, args, params)
				if err != nil {
					return fmt.Errorf("ON CONFLICT ... WHERE: %w", err)
				}
			default:
				err = c.Predicate.WriteSQL(ctx, dialect, buf, args, params)
				if err != nil {
					return fmt.Errorf("ON CONFLICT ... WHERE: %w", err)
				}
			}
		}
	}
	if len(c.Resolution) == 0 || c.DoNothing {
		buf.WriteString(" DO NOTHING")
		return nil
	}
	buf.WriteString(" DO UPDATE SET ")
	err = Assignments(c.Resolution).WriteSQL(ctx, dialect, buf, args, params)
	if err != nil {
		return fmt.Errorf("DO UPDATE SET: %w", err)
	}
	if c.ResolutionPredicate != nil {
		buf.WriteString(" WHERE ")
		switch predicate := c.ResolutionPredicate.(type) {
		case VariadicPredicate:
			predicate.Toplevel = true
			err = predicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("DO UPDATE SET ... WHERE: %w", err)
			}
		default:
			err = c.ResolutionPredicate.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("DO UPDATE SET ... WHERE: %w", err)
			}
		}
	}
	return nil
}

// SetFetchableFields implements the Query interface.
func (q InsertQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	switch q.Dialect {
	case DialectPostgres, DialectSQLite:
		if len(q.ReturningFields) == 0 {
			q.ReturningFields = fields
			return q, true
		}
		return q, false
	default:
		return q, false
	}
}

// GetFetchableFields returns the fetchable fields of the query.
func (q InsertQuery) GetFetchableFields() []Field {
	switch q.Dialect {
	case DialectPostgres, DialectSQLite:
		return q.ReturningFields
	default:
		return nil
	}
}

// GetDialect implements the Query interface.
func (q InsertQuery) GetDialect() string { return q.Dialect }

// SetDialect sets the dialect of the query.
func (q InsertQuery) SetDialect(dialect string) InsertQuery {
	q.Dialect = dialect
	return q
}

// SQLiteInsertQuery represents an SQLite INSERT query.
type SQLiteInsertQuery InsertQuery

var _ Query = (*SQLiteInsertQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLiteInsertQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return InsertQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// InsertInto creates a new SQLiteInsertQuery.
func (b sqliteQueryBuilder) InsertInto(table Table) SQLiteInsertQuery {
	return SQLiteInsertQuery{
		Dialect:     DialectSQLite,
		CTEs:        b.ctes,
		InsertTable: table,
	}
}

// Columns sets the InsertColumns field of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) Columns(fields ...Field) SQLiteInsertQuery {
	q.InsertColumns = fields
	return q
}

// Values sets the RowValues field of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) Values(values ...any) SQLiteInsertQuery {
	q.RowValues = append(q.RowValues, values)
	return q
}

// ColumnValues sets the ColumnMapper field of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) ColumnValues(colmapper func(*Column)) SQLiteInsertQuery {
	q.ColumnMapper = colmapper
	return q
}

// Select sets the SelectQuery field of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) Select(query Query) SQLiteInsertQuery {
	q.SelectQuery = query
	return q
}

type sqliteInsertConflict struct{ q *SQLiteInsertQuery }

// OnConflict starts the ON CONFLICT clause of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) OnConflict(fields ...Field) sqliteInsertConflict {
	q.Conflict.Fields = fields
	return sqliteInsertConflict{q: &q}
}

// Where adds predicates to the ON CONFLICT clause of the SQLiteInsertQuery.
func (c sqliteInsertConflict) Where(predicates ...Predicate) sqliteInsertConflict {
	c.q.Conflict.Predicate = appendPredicates(c.q.Conflict.Predicate, predicates)
	return c
}

// DoNothing resolves the ON CONFLICT clause of the SQLiteInsertQuery with DO
// NOTHING.
func (c sqliteInsertConflict) DoNothing() SQLiteInsertQuery {
	c.q.Conflict.DoNothing = true
	return *c.q
}

// DoUpdateSet resolves the ON CONFLICT CLAUSE of the SQLiteInsertQuery with DO UPDATE SET.
func (c sqliteInsertConflict) DoUpdateSet(assignments ...Assignment) SQLiteInsertQuery {
	c.q.Conflict.Resolution = assignments
	return *c.q
}

// Where adds predicates to the DO UPDATE SET clause of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) Where(predicates ...Predicate) SQLiteInsertQuery {
	q.Conflict.ResolutionPredicate = appendPredicates(q.Conflict.ResolutionPredicate, predicates)
	return q
}

// Returning adds fields to the RETURNING clause of the SQLiteInsertQuery.
func (q SQLiteInsertQuery) Returning(fields ...Field) SQLiteInsertQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q SQLiteInsertQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return InsertQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SQLiteInsertQuery) GetFetchableFields() []Field {
	return InsertQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q SQLiteInsertQuery) GetDialect() string { return q.Dialect }

// SetDialect returns the dialect of the query.
func (q SQLiteInsertQuery) SetDialect(dialect string) SQLiteInsertQuery {
	q.Dialect = dialect
	return q
}

// PostgresInsertQuery represents a Postgres INSERT query.
type PostgresInsertQuery InsertQuery

var _ Query = (*PostgresInsertQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q PostgresInsertQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return InsertQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// InsertInto creates a new PostgresInsertQuery.
func (b postgresQueryBuilder) InsertInto(table Table) PostgresInsertQuery {
	return PostgresInsertQuery{
		Dialect:     DialectPostgres,
		CTEs:        b.ctes,
		InsertTable: table,
	}
}

// Columns sets the InsertColumns field of the PostgresInsertQuery.
func (q PostgresInsertQuery) Columns(fields ...Field) PostgresInsertQuery {
	q.InsertColumns = fields
	return q
}

// Values sets the RowValues field of the PostgresInsertQuery.
func (q PostgresInsertQuery) Values(values ...any) PostgresInsertQuery {
	q.RowValues = append(q.RowValues, values)
	return q
}

// ColumnValues sets the ColumnMapper field of the PostgresInsertQuery.
func (q PostgresInsertQuery) ColumnValues(colmapper func(*Column)) PostgresInsertQuery {
	q.ColumnMapper = colmapper
	return q
}

// Select sets the SelectQuery field of the PostgresInsertQuery.
func (q PostgresInsertQuery) Select(query Query) PostgresInsertQuery {
	q.SelectQuery = query
	return q
}

type postgresInsertConflict struct{ q *PostgresInsertQuery }

// OnConflict starts the ON CONFLICT clause of the PostgresInsertQuery.
func (q PostgresInsertQuery) OnConflict(fields ...Field) postgresInsertConflict {
	q.Conflict.Fields = fields
	return postgresInsertConflict{q: &q}
}

// OnConflict starts the ON CONFLICT clause of the PostgresInsertQuery.
func (q PostgresInsertQuery) OnConflictOnConstraint(constraintName string) postgresInsertConflict {
	q.Conflict.ConstraintName = constraintName
	return postgresInsertConflict{q: &q}
}

// Where adds predicates to the ON CONFLICT clause of the PostgresInsertQuery.
func (c postgresInsertConflict) Where(predicates ...Predicate) postgresInsertConflict {
	c.q.Conflict.Predicate = appendPredicates(c.q.Conflict.Predicate, predicates)
	return c
}

// DoNothing resolves the ON CONFLICT clause of the PostgresInsertQuery with DO
// NOTHING.
func (c postgresInsertConflict) DoNothing() PostgresInsertQuery {
	c.q.Conflict.DoNothing = true
	return *c.q
}

// DoUpdateSet resolves the ON CONFLICT CLAUSE of the PostgresInsertQuery with DO UPDATE SET.
func (c postgresInsertConflict) DoUpdateSet(assignments ...Assignment) PostgresInsertQuery {
	c.q.Conflict.Resolution = assignments
	return *c.q
}

// Where adds predicates to the DO UPDATE SET clause of the PostgresInsertQuery.
func (q PostgresInsertQuery) Where(predicates ...Predicate) PostgresInsertQuery {
	q.Conflict.ResolutionPredicate = appendPredicates(q.Conflict.ResolutionPredicate, predicates)
	return q
}

// Returning adds fields to the RETURNING clause of the PostgresInsertQuery.
func (q PostgresInsertQuery) Returning(fields ...Field) PostgresInsertQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q PostgresInsertQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return InsertQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q PostgresInsertQuery) GetFetchableFields() []Field {
	return InsertQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q PostgresInsertQuery) GetDialect() string { return q.Dialect }

// SetDialect returns the dialect of the query.
func (q PostgresInsertQuery) SetDialect(dialect string) PostgresInsertQuery {
	q.Dialect = dialect
	return q
}

// MySQLInsertQuery represents a MySQL INSERT query.
type MySQLInsertQuery InsertQuery

var _ Query = (*MySQLInsertQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q MySQLInsertQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return InsertQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// InsertInto creates a new MySQLInsertQuery.
func (b mysqlQueryBuilder) InsertInto(table Table) MySQLInsertQuery {
	return MySQLInsertQuery{
		Dialect:     DialectMySQL,
		CTEs:        b.ctes,
		InsertTable: table,
	}
}

// InsertInto creates a new MySQLInsertQuery.
func (b mysqlQueryBuilder) InsertIgnoreInto(table Table) MySQLInsertQuery {
	return MySQLInsertQuery{
		Dialect:      DialectMySQL,
		CTEs:         b.ctes,
		InsertTable:  table,
		InsertIgnore: true,
	}
}

// Columns sets the InsertColumns field of the MySQLInsertQuery.
func (q MySQLInsertQuery) Columns(fields ...Field) MySQLInsertQuery {
	q.InsertColumns = fields
	return q
}

// Values sets the RowValues field of the MySQLInsertQuery.
func (q MySQLInsertQuery) Values(values ...any) MySQLInsertQuery {
	q.RowValues = append(q.RowValues, values)
	return q
}

// As sets the RowAlias field of the MySQLInsertQuery.
func (q MySQLInsertQuery) As(rowAlias string) MySQLInsertQuery {
	q.RowAlias = rowAlias
	return q
}

// ColumnValues sets the ColumnMapper field of the MySQLInsertQuery.
func (q MySQLInsertQuery) ColumnValues(colmapper func(*Column)) MySQLInsertQuery {
	q.ColumnMapper = colmapper
	return q
}

// Select sets the SelectQuery field of the MySQLInsertQuery.
func (q MySQLInsertQuery) Select(query Query) MySQLInsertQuery {
	q.SelectQuery = query
	return q
}

// OnDuplicateKeyUpdate sets the ON DUPLICATE KEY UPDATE clause of the
// MySQLInsertQuery.
func (q MySQLInsertQuery) OnDuplicateKeyUpdate(assignments ...Assignment) MySQLInsertQuery {
	q.Conflict.Resolution = assignments
	return q
}

// Returning adds fields to the RETURNING clause of the MySQLInsertQuery.
func (q MySQLInsertQuery) Returning(fields ...Field) MySQLInsertQuery {
	q.ReturningFields = append(q.ReturningFields, fields...)
	return q
}

// SetFetchableFields implements the Query interface.
func (q MySQLInsertQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return InsertQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q MySQLInsertQuery) GetFetchableFields() []Field {
	return InsertQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q MySQLInsertQuery) GetDialect() string { return q.Dialect }

// SetDialect returns the dialect of the query.
func (q MySQLInsertQuery) SetDialect(dialect string) MySQLInsertQuery {
	q.Dialect = dialect
	return q
}

// SQLServerInsertQuery represents an SQL Server INSERT query.
type SQLServerInsertQuery InsertQuery

var _ Query = (*SQLServerInsertQuery)(nil)

// WriteSQL implements the SQLWriter interface.
func (q SQLServerInsertQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return InsertQuery(q).WriteSQL(ctx, dialect, buf, args, params)
}

// InsertInto creates a new SQLServerInsertQuery.
func (b sqlserverQueryBuilder) InsertInto(table Table) SQLServerInsertQuery {
	return SQLServerInsertQuery{
		Dialect:     DialectSQLServer,
		CTEs:        b.ctes,
		InsertTable: table,
	}
}

// Columns sets the InsertColumns field of the SQLServerInsertQuery.
func (q SQLServerInsertQuery) Columns(fields ...Field) SQLServerInsertQuery {
	q.InsertColumns = fields
	return q
}

// Values sets the RowValues field of the SQLServerInsertQuery.
func (q SQLServerInsertQuery) Values(values ...any) SQLServerInsertQuery {
	q.RowValues = append(q.RowValues, values)
	return q
}

// ColumnValues sets the ColumnMapper field of the SQLServerInsertQuery.
func (q SQLServerInsertQuery) ColumnValues(colmapper func(*Column)) SQLServerInsertQuery {
	q.ColumnMapper = colmapper
	return q
}

// Select sets the SelectQuery field of the SQLServerInsertQuery.
func (q SQLServerInsertQuery) Select(query Query) SQLServerInsertQuery {
	q.SelectQuery = query
	return q
}

// SetFetchableFields implements the Query interface.
func (q SQLServerInsertQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return InsertQuery(q).SetFetchableFields(fields)
}

// GetFetchableFields returns the fetchable fields of the query.
func (q SQLServerInsertQuery) GetFetchableFields() []Field {
	return InsertQuery(q).GetFetchableFields()
}

// GetDialect implements the Query interface.
func (q SQLServerInsertQuery) GetDialect() string { return q.Dialect }

// SetDialect returns the dialect of the query.
func (q SQLServerInsertQuery) SetDialect(dialect string) SQLServerInsertQuery {
	q.Dialect = dialect
	return q
}
