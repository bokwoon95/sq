package sq

import (
	"bytes"
	"context"
	"fmt"
)

// Join operators.
const (
	JoinInner = "JOIN"
	JoinLeft  = "LEFT JOIN"
	JoinRight = "RIGHT JOIN"
	JoinFull  = "FULL JOIN"
	JoinCross = "CROSS JOIN"
)

// JoinTable represents a join on a table.
type JoinTable struct {
	JoinOperator string
	Table        Table
	OnPredicate  Predicate
	UsingFields  []Field
}

// JoinUsing creates a new JoinTable with the USING operator.
func JoinUsing(table Table, fields ...Field) JoinTable {
	return JoinTable{JoinOperator: JoinInner, Table: table, UsingFields: fields}
}

// Join creates a new JoinTable with the JOIN operator.
func Join(table Table, predicates ...Predicate) JoinTable {
	return CustomJoin(JoinInner, table, predicates...)
}

// LeftJoin creates a new JoinTable with the LEFT JOIN operator.
func LeftJoin(table Table, predicates ...Predicate) JoinTable {
	return CustomJoin(JoinLeft, table, predicates...)
}

// FullJoin creates a new JoinTable with the FULL JOIN operator.
func FullJoin(table Table, predicates ...Predicate) JoinTable {
	return CustomJoin(JoinFull, table, predicates...)
}

// CrossJoin creates a new JoinTable with the CROSS JOIN operator.
func CrossJoin(table Table) JoinTable {
	return CustomJoin(JoinCross, table)
}

// CustomJoin creates a new JoinTable with the a custom join operator.
func CustomJoin(joinOperator string, table Table, predicates ...Predicate) JoinTable {
	switch len(predicates) {
	case 0:
		return JoinTable{JoinOperator: joinOperator, Table: table}
	case 1:
		return JoinTable{JoinOperator: joinOperator, Table: table, OnPredicate: predicates[0]}
	default:
		return JoinTable{JoinOperator: joinOperator, Table: table, OnPredicate: And(predicates...)}
	}
}

// WriteSQL implements the SQLWriter interface.
func (join JoinTable) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	if join.JoinOperator == "" {
		join.JoinOperator = JoinInner
	}
	variadicPredicate, isVariadic := join.OnPredicate.(VariadicPredicate)
	hasNoPredicate := join.OnPredicate == nil && len(variadicPredicate.Predicates) == 0 && len(join.UsingFields) == 0
	if hasNoPredicate && (join.JoinOperator == JoinInner ||
		join.JoinOperator == JoinLeft ||
		join.JoinOperator == JoinRight ||
		join.JoinOperator == JoinFull) &&
		// exclude sqlite from this check because they allow join without predicate
		dialect != DialectSQLite {
		return fmt.Errorf("%s requires at least one predicate specified", join.JoinOperator)
	}
	if dialect == DialectSQLite && (join.JoinOperator == JoinRight || join.JoinOperator == JoinFull) {
		return fmt.Errorf("sqlite does not support %s", join.JoinOperator)
	}

	// JOIN
	buf.WriteString(string(join.JoinOperator) + " ")
	if join.Table == nil {
		return fmt.Errorf("joining on a nil table")
	}

	// <table>
	_, isQuery := join.Table.(Query)
	if isQuery {
		buf.WriteString("(")
	}
	err := join.Table.WriteSQL(ctx, dialect, buf, args, params)
	if err != nil {
		return err
	}
	if isQuery {
		buf.WriteString(")")
	}

	// AS
	if tableAlias := getAlias(join.Table); tableAlias != "" {
		buf.WriteString(" AS " + QuoteIdentifier(dialect, tableAlias) + quoteTableColumns(dialect, join.Table))
	} else if isQuery && dialect != DialectSQLite {
		return fmt.Errorf("%s %s subquery must have alias", dialect, join.JoinOperator)
	}

	if isVariadic {
		// ON VariadicPredicate
		buf.WriteString(" ON ")
		variadicPredicate.Toplevel = true
		err = variadicPredicate.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return err
		}
	} else if join.OnPredicate != nil {
		// ON Predicate
		buf.WriteString(" ON ")
		err = join.OnPredicate.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return err
		}
	} else if len(join.UsingFields) > 0 {
		// USING Fields
		buf.WriteString(" USING (")
		err = writeFieldsWithPrefix(ctx, dialect, buf, args, params, join.UsingFields, "", false)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	}
	return nil
}

func writeJoinTables(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, joinTables []JoinTable) error {
	var err error
	for i, joinTable := range joinTables {
		if i > 0 {
			buf.WriteString(" ")
		}
		err = joinTable.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("join #%d: %w", i+1, err)
		}
	}
	return nil
}
