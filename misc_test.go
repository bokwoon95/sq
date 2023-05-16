package sq

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestValueExpression(t *testing.T) {
	t.Run("alias", func(t *testing.T) {
		t.Parallel()
		expr := Value(1).As("num")
		if diff := testutil.Diff(expr.GetAlias(), "num"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	tests := []TestTable{{
		description: "basic",
		item:        Value(Param("xyz", 42)),
		wantQuery:   "?",
		wantArgs:    []any{42},
		wantParams:  map[string][]int{"xyz": {0}},
	}, {
		description: "In", item: Value(1).In([]int{18, 21, 32}),
		wantQuery: "? IN (?, ?, ?)", wantArgs: []any{1, 18, 21, 32},
	}, {
		description: "Eq", item: Value(1).Eq(34),
		wantQuery: "? = ?", wantArgs: []any{1, 34},
	}, {
		description: "Ne", item: Value(1).Ne(34),
		wantQuery: "? <> ?", wantArgs: []any{1, 34},
	}, {
		description: "Lt", item: Value(1).Lt(34),
		wantQuery: "? < ?", wantArgs: []any{1, 34},
	}, {
		description: "Le", item: Value(1).Le(34),
		wantQuery: "? <= ?", wantArgs: []any{1, 34},
	}, {
		description: "Gt", item: Value(1).Gt(34),
		wantQuery: "? > ?", wantArgs: []any{1, 34},
	}, {
		description: "Ge", item: Value(1).Ge(34),
		wantQuery: "? >= ?", wantArgs: []any{1, 34},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestLiteralExpression(t *testing.T) {
	t.Run("alias", func(t *testing.T) {
		t.Parallel()
		expr := Literal(1).As("num")
		if diff := testutil.Diff(expr.GetAlias(), "num"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	tests := []TestTable{{
		description: "binary",
		item:        Literal([]byte{0xab, 0xcd, 0xef}),
		wantQuery:   "x'abcdef'",
	}, {
		description: "time", item: Literal(time.Unix(0, 0).UTC()),
		wantQuery: "'1970-01-01 00:00:00'",
	}, {
		description: "In", item: Literal(1).In([]any{Literal(18), Literal(21), Literal(32)}),
		wantQuery: "1 IN (18, 21, 32)",
	}, {
		description: "Eq", item: Literal(true).Eq(Literal(false)),
		wantQuery: "TRUE = FALSE",
	}, {
		description: "Ne", item: Literal("one").Ne(Literal("thirty four")),
		wantQuery: "'one' <> 'thirty four'",
	}, {
		description: "Lt", item: Literal(1).Lt(Literal(34)),
		wantQuery: "1 < 34",
	}, {
		description: "Le", item: Literal(1).Le(Literal(34)),
		wantQuery: "1 <= 34",
	}, {
		description: "Gt", item: Literal(1).Gt(Literal(34)),
		wantQuery: "1 > 34",
	}, {
		description: "Ge", item: Literal(1).Ge(Literal(34)),
		wantQuery: "1 >= 34",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestDialectExpression(t *testing.T) {
	t.Parallel()
	expr := DialectValue(Expr("default")).
		DialectValue(DialectSQLite, Expr("sqlite")).
		DialectValue(DialectPostgres, Expr("postgres")).
		DialectValue(DialectMySQL, Expr("mysql")).
		DialectExpr(DialectSQLServer, "{}", Expr("sqlserver"))
	var tt TestTable
	tt.item = expr
	// default
	tt.wantQuery = "default"
	tt.assert(t)
	// sqlite
	tt.dialect = DialectSQLite
	tt.wantQuery = "sqlite"
	tt.assert(t)
	// postgres
	tt.dialect = DialectPostgres
	tt.wantQuery = "postgres"
	tt.assert(t)
	// mysql
	tt.dialect = DialectMySQL
	tt.wantQuery = "mysql"
	tt.assert(t)
	// sqlserver
	tt.dialect = DialectSQLServer
	tt.wantQuery = "sqlserver"
	tt.assert(t)
}

func TestCaseExpressions(t *testing.T) {
	t.Run("name and alias", func(t *testing.T) {
		t.Parallel()
		// CaseExpression
		caseExpr := CaseWhen(Value(true), 1).As("result_a")
		if diff := testutil.Diff(caseExpr.GetAlias(), "result_a"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		// SimpleCaseExpression
		simpleCaseExpr := Case(1).When(1, 2).As("result_b")
		if diff := testutil.Diff(simpleCaseExpr.GetAlias(), "result_b"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("CaseExpression", func(t *testing.T) {
		t.Parallel()
		TestTable{
			item:      CaseWhen(Expr("x = y"), 1).When(Expr("a = b"), 2).Else(3),
			wantQuery: "CASE WHEN x = y THEN ? WHEN a = b THEN ? ELSE ? END",
			wantArgs:  []any{1, 2, 3},
		}.assert(t)
	})

	t.Run("SimpleCaseExpression", func(t *testing.T) {
		t.Parallel()
		TestTable{
			item:      Case(Expr("a")).When(1, 2).When(3, 4).Else(5),
			wantQuery: "CASE a WHEN ? THEN ? WHEN ? THEN ? ELSE ? END",
			wantArgs:  []any{1, 2, 3, 4, 5},
		}.assert(t)
	})

	t.Run("CaseExpression cannot be empty", func(t *testing.T) {
		t.Parallel()
		TestTable{item: CaseExpression{}}.assertNotOK(t)
	})

	t.Run("SimpleCaseExpression cannot be empty", func(t *testing.T) {
		t.Parallel()
		TestTable{item: SimpleCaseExpression{}}.assertNotOK(t)
	})

	errTests := []TestTable{{
		description: "CASE WHEN predicate err", item: CaseWhen(FaultySQL{}, 1),
	}, {
		description: "CASE WHEN result err", item: CaseWhen(Value(true), FaultySQL{}),
	}, {
		description: "CASE WHEN fallback err", item: CaseWhen(Value(true), 1).Else(FaultySQL{}),
	}, {
		description: "CASE expression err", item: Case(FaultySQL{}).When(1, 2),
	}, {
		description: "CASE value err", item: Case(1).When(FaultySQL{}, 2),
	}, {
		description: "CASE result err", item: Case(1).When(2, FaultySQL{}),
	}, {
		description: "CASE fallback err", item: Case(1).When(2, 3).Else(FaultySQL{}),
	}}

	for _, tt := range errTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertErr(t, ErrFaultySQL)
		})
	}
}

func TestSelectValues(t *testing.T) {
	type TestTable struct {
		description string
		dialect     string
		item        SelectValues
		wantQuery   string
		wantArgs    []any
	}

	t.Run("dialect alias and fields", func(t *testing.T) {
		selectValues := SelectValues{
			Alias: "aaa",
		}
		if diff := testutil.Diff(selectValues.GetAlias(), "aaa"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(selectValues.GetDialect(), ""); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := selectValues.SetFetchableFields(nil)
		if diff := testutil.Diff(ok, false); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		gotField, _, _ := ToSQL("", selectValues.Field("bbb"), nil)
		if diff := testutil.Diff(gotField, "aaa.bbb"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	tests := []TestTable{{
		description: "empty",
		item:        SelectValues{},
		wantQuery:   "",
		wantArgs:    nil,
	}, {
		description: "no columns",
		item: SelectValues{
			RowValues: [][]any{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		wantQuery: "SELECT ?, ?, ?" +
			" UNION ALL SELECT ?, ?, ?" +
			" UNION ALL SELECT ?, ?, ?",
		wantArgs: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, {
		description: "postgres",
		dialect:     DialectPostgres,
		item: SelectValues{
			Columns: []string{"a", "b", "c"},
			RowValues: [][]any{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		wantQuery: "SELECT $1 AS a, $2 AS b, $3 AS c" +
			" UNION ALL SELECT $4, $5, $6" +
			" UNION ALL SELECT $7, $8, $9",
		wantArgs: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			var gotArgs []any
			err := tt.item.WriteSQL(context.Background(), tt.dialect, &buf, &gotArgs, nil)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			gotQuery := buf.String()
			if diff := testutil.Diff(gotQuery, tt.wantQuery); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			if diff := testutil.Diff(gotArgs, tt.wantArgs); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}

func TestTableValues(t *testing.T) {
	type TestTable struct {
		description string
		dialect     string
		item        TableValues
		wantQuery   string
		wantArgs    []any
	}

	t.Run("dialect alias columns and fields", func(t *testing.T) {
		tableValues := TableValues{
			Alias:   "aaa",
			Columns: []string{"a", "b", "c"},
		}
		if diff := testutil.Diff(tableValues.GetAlias(), "aaa"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(tableValues.GetDialect(), ""); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := tableValues.SetFetchableFields(nil)
		if diff := testutil.Diff(ok, false); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		gotColumns := tableValues.GetColumns()
		wantColumns := []string{"a", "b", "c"}
		if diff := testutil.Diff(gotColumns, wantColumns); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		gotField, _, _ := ToSQL("", tableValues.Field("bbb"), nil)
		wantField := "aaa.bbb"
		if diff := testutil.Diff(gotField, wantField); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	tests := []TestTable{{
		description: "empty",
		item:        TableValues{},
		wantQuery:   "",
		wantArgs:    nil,
	}, {
		description: "no columns",
		item: TableValues{
			RowValues: [][]any{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		wantQuery: "VALUES (?, ?, ?)" +
			", (?, ?, ?)" +
			", (?, ?, ?)",
		wantArgs: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, {
		description: "postgres",
		dialect:     DialectPostgres,
		item: TableValues{
			Columns: []string{"a", "b", "c"},
			RowValues: [][]any{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		wantQuery: "VALUES ($1, $2, $3)" +
			", ($4, $5, $6)" +
			", ($7, $8, $9)",
		wantArgs: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, {
		description: "mysql",
		dialect:     DialectMySQL,
		item: TableValues{
			Columns: []string{"a", "b", "c"},
			RowValues: [][]any{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		wantQuery: "VALUES ROW(?, ?, ?)" +
			", ROW(?, ?, ?)" +
			", ROW(?, ?, ?)",
		wantArgs: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			var gotArgs []any
			err := tt.item.WriteSQL(context.Background(), tt.dialect, &buf, &gotArgs, nil)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			gotQuery := buf.String()
			if diff := testutil.Diff(gotQuery, tt.wantQuery); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			if diff := testutil.Diff(gotArgs, tt.wantArgs); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}
