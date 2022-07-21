package sq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
)

// CTE represents an SQL common table expression (CTE).
type CTE struct {
	query        Query
	columns      []string
	recursive    bool
	materialized sql.NullBool
	name         string
	alias        string
}

var _ Table = (*CTE)(nil)

// NewCTE creates a new CTE.
func NewCTE(name string, columns []string, query Query) CTE {
	return CTE{name: name, columns: columns, query: query}
}

// NewRecursiveCTE creates a new recursive CTE.
func NewRecursiveCTE(name string, columns []string, query Query) CTE {
	return CTE{name: name, columns: columns, query: query, recursive: true}
}

// WriteSQL implements the SQLWriter interface.
func (cte CTE) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString(QuoteIdentifier(dialect, cte.name))
	return nil
}

// As returns a new CTE with the given alias.
func (cte CTE) As(alias string) CTE {
	cte.alias = alias
	return cte
}

// Materialized returns a new CTE marked as MATERIALIZED. This only works on
// postgres.
func (cte CTE) Materialized() CTE {
	cte.materialized.Valid = true
	cte.materialized.Bool = true
	return cte
}

// Materialized returns a new CTE marked as NOT MATERIALIZED. This only works
// on postgres.
func (cte CTE) NotMaterialized() CTE {
	cte.materialized.Valid = true
	cte.materialized.Bool = false
	return cte
}

// Field returns a Field from the CTE.
func (cte CTE) Field(name string) AnyField {
	return NewAnyField(name, NewTableStruct("", cte.name, cte.alias))
}

// GetAlias returns the alias of the CTE.
func (cte CTE) GetAlias() string { return cte.alias }

// AssertTable implements the Table interface.
func (cte CTE) IsTable() {}

func writeCTEs(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, ctes []CTE) error {
	var hasRecursiveCTE bool
	for _, cte := range ctes {
		if cte.recursive {
			hasRecursiveCTE = true
			break
		}
	}
	if hasRecursiveCTE {
		buf.WriteString("WITH RECURSIVE ")
	} else {
		buf.WriteString("WITH ")
	}
	for i, cte := range ctes {
		if i > 0 {
			buf.WriteString(", ")
		}
		if cte.name == "" {
			return fmt.Errorf("CTE #%d has no name", i+1)
		}
		buf.WriteString(QuoteIdentifier(dialect, cte.name))
		if len(cte.columns) > 0 {
			buf.WriteString(" (")
			for j, column := range cte.columns {
				if j > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(QuoteIdentifier(dialect, column))
			}
			buf.WriteString(")")
		}
		buf.WriteString(" AS ")
		if dialect == DialectPostgres && cte.materialized.Valid {
			if cte.materialized.Bool {
				buf.WriteString("MATERIALIZED ")
			} else {
				buf.WriteString("NOT MATERIALIZED ")
			}
		}
		buf.WriteString("(")
		switch query := cte.query.(type) {
		case nil:
			return fmt.Errorf("CTE #%d query is nil", i+1)
		case VariadicQuery:
			query.Toplevel = true
			err := query.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("CTE #%d failed to build query: %w", i+1, err)
			}
		default:
			err := query.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return fmt.Errorf("CTE #%d failed to build query: %w", i+1, err)
			}
		}
		buf.WriteString(")")
	}
	buf.WriteString(" ")
	return nil
}

// VariadicQueryOperator represents a variadic query operator.
type VariadicQueryOperator string

// VariadicQuery operators.
const (
	QueryUnion        VariadicQueryOperator = "UNION"
	QueryUnionAll     VariadicQueryOperator = "UNION ALL"
	QueryIntersect    VariadicQueryOperator = "INTERSECT"
	QueryIntersectAll VariadicQueryOperator = "INTERSECT ALL"
	QueryExcept       VariadicQueryOperator = "EXCEPT"
	QueryExceptAll    VariadicQueryOperator = "EXCEPT ALL"
)

// VariadicQuery represents the 'x UNION y UNION z...' etc SQL constructs.
type VariadicQuery struct {
	Toplevel bool
	Operator VariadicQueryOperator
	Queries  []Query
}

var _ Query = (*VariadicQuery)(nil)

// Union joins the queries together with the UNION operator.
func Union(queries ...Query) VariadicQuery {
	return VariadicQuery{Operator: QueryUnion, Queries: queries}
}

// UnionAll joins the queries together with the UNION ALL operator.
func UnionAll(queries ...Query) VariadicQuery {
	return VariadicQuery{Operator: QueryUnionAll, Queries: queries}
}

// Intersect joins the queries together with the INTERSECT operator.
func Intersect(queries ...Query) VariadicQuery {
	return VariadicQuery{Operator: QueryIntersect, Queries: queries}
}

// IntersectAll joins the queries together with the INTERSECT ALL operator.
func IntersectAll(queries ...Query) VariadicQuery {
	return VariadicQuery{Operator: QueryIntersectAll, Queries: queries}
}

// Except joins the queries together with the EXCEPT operator.
func Except(queries ...Query) VariadicQuery {
	return VariadicQuery{Operator: QueryExcept, Queries: queries}
}

// ExceptAll joins the queries together with the EXCEPT ALL operator.
func ExceptAll(queries ...Query) VariadicQuery {
	return VariadicQuery{Operator: QueryExceptAll, Queries: queries}
}

// WriteSQL implements the SQLWriter interface.
func (q VariadicQuery) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	if q.Operator == "" {
		q.Operator = QueryUnion
	}
	if len(q.Queries) == 0 {
		return fmt.Errorf("VariadicQuery empty")
	}

	if len(q.Queries) == 1 {
		switch q1 := q.Queries[0].(type) {
		case nil:
			return fmt.Errorf("query #1 is nil")
		case VariadicQuery:
			q1.Toplevel = q.Toplevel
			err = q1.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return err
			}
		default:
			err = q.Queries[0].WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if !q.Toplevel {
		buf.WriteString("(")
	}
	for i, query := range q.Queries {
		if i > 0 {
			buf.WriteString(" " + string(q.Operator) + " ")
		}
		if query == nil {
			return fmt.Errorf("query #%d is nil", i+1)
		}
		err = query.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("query #%d: %w", i+1, err)
		}
	}
	if !q.Toplevel {
		buf.WriteString(")")
	}
	return nil
}

// SetFetchableFields implements the Query interface.
func (q VariadicQuery) SetFetchableFields(fields []Field) (query Query, ok bool) {
	return q, false
}

// GetFetchableFields implements the Query interface.
func (q VariadicQuery) GetFetchableFields() []Field {
	return nil
}

// GetDialect returns the SQL dialect of the VariadicQuery.
func (q VariadicQuery) GetDialect() string {
	if len(q.Queries) == 0 {
		return ""
	}
	q1 := q.Queries[0]
	if q1 == nil {
		return ""
	}
	return q1.GetDialect()
}
