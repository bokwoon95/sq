package sq

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestExpression(t *testing.T) {
	t.Run("schema, name and alias", func(t *testing.T) {
		t.Parallel()
		expr := Expr("COUNT(*)").As("total")
		if diff := testutil.Diff(expr.GetAlias(), "total"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	tests := []TestTable{{
		description: "basic",
		dialect:     DialectSQLServer,
		item:        Expr("CONCAT(CONCAT(name, {}), {})", "abc", sql.Named("xyz", "def")),
		wantQuery:   "CONCAT(CONCAT(name, @p1), @xyz)",
		wantArgs:    []any{"abc", sql.Named("xyz", "def")},
		wantParams:  map[string][]int{"xyz": {1}},
	}, {
		description: "In", item: Expr("age").In([]int{18, 21, 32}),
		wantQuery: "age IN (?, ?, ?)", wantArgs: []any{18, 21, 32},
	}, {
		description: "Eq", item: Expr("age").Eq(34),
		wantQuery: "age = ?", wantArgs: []any{34},
	}, {
		description: "Ne", item: Expr("age").Ne(34),
		wantQuery: "age <> ?", wantArgs: []any{34},
	}, {
		description: "Lt", item: Expr("age").Lt(34),
		wantQuery: "age < ?", wantArgs: []any{34},
	}, {
		description: "Le", item: Expr("age").Le(34),
		wantQuery: "age <= ?", wantArgs: []any{34},
	}, {
		description: "Gt", item: Expr("age").Gt(34),
		wantQuery: "age > ?", wantArgs: []any{34},
	}, {
		description: "Ge", item: Expr("age").Ge(34),
		wantQuery: "age >= ?", wantArgs: []any{34},
	}, {
		description: "Exists", item: Exists(Queryf("SELECT 1 FROM tbl WHERE 1 = 1")),
		wantQuery: "EXISTS (SELECT 1 FROM tbl WHERE 1 = 1)",
	}, {
		description: "NotExists", item: NotExists(Queryf("SELECT 1 FROM tbl WHERE 1 = 1")),
		wantQuery: "NOT EXISTS (SELECT 1 FROM tbl WHERE 1 = 1)",
	}, {
		description: "Count", item: Count(Expr("name")), wantQuery: "COUNT(name)",
	}, {
		description: "CountStar", item: CountStar(), wantQuery: "COUNT(*)",
	}, {
		description: "Sum", item: Sum(Expr("score")), wantQuery: "SUM(score)",
	}, {
		description: "Avg", item: Avg(Expr("score")), wantQuery: "AVG(score)",
	}, {
		description: "Min", item: Min(Expr("score")), wantQuery: "MIN(score)",
	}, {
		description: "Max", item: Max(Expr("score")), wantQuery: "MAX(score)",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}

	t.Run("FaultySQL", func(t *testing.T) {
		t.Parallel()
		TestTable{item: Expr("SELECT {}", FaultySQL{})}.assertNotOK(t)
	})
}

func TestVariadicPredicate(t *testing.T) {
	t.Run("name and alias", func(t *testing.T) {
		t.Parallel()
		p := And(Expr("True"), Expr("FALSE")).As("is_false")
		if diff := testutil.Diff(p.GetAlias(), "is_false"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = VariadicPredicate{}
		tt.assertNotOK(t)
	})

	t.Run("nil predicate", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = And(nil)
		tt.assertNotOK(t)
	})

	t.Run("1 predicate", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = And(cmp("=", Expr("score"), 21))
		tt.wantQuery = "score = ?"
		tt.wantArgs = []any{21}
		tt.assert(t)
	})

	t.Run("2 predicate", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = And(cmp("=", Expr("score"), 21), cmp("=", Expr("name"), "bob"))
		tt.wantQuery = "(score = ? AND name = ?)"
		tt.wantArgs = []any{21, "bob"}
		tt.assert(t)
	})

	t.Run("multiple nested VariadicPredicate collapses into one", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = And(And(And(And(cmp("=", Expr("score"), 21)))))
		tt.wantQuery = "score = ?"
		tt.wantArgs = []any{21}
		tt.assert(t)
	})

	t.Run("multiple predicates", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		user_id, name, age := Expr("user_id"), Expr("name"), Expr("age")
		tt.item = Or(
			Expr("{} IS NULL", name),
			cmp("=", age, age),
			And(cmp("=", age, age)),
			And(
				cmp("=", user_id, 1),
				cmp("<>", user_id, 2),
				cmp("<", user_id, 3),
				cmp("<=", user_id, 4),
				cmp(">", user_id, 5),
				cmp(">=", user_id, 6),
			),
		)
		tt.wantQuery = "(name IS NULL" +
			" OR age = age" +
			" OR age = age" +
			" OR (" +
			"user_id = ?" +
			" AND user_id <> ?" +
			" AND user_id < ?" +
			" AND user_id <= ?" +
			" AND user_id > ?" +
			" AND user_id >= ?" +
			"))"
		tt.wantArgs = []any{1, 2, 3, 4, 5, 6}
		tt.assert(t)
	})

	t.Run("multiple predicates with nil", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Or(
			Expr("1 = 1"),
			And(Expr("TRUE"), Predicate(nil)),
		)
		tt.assertNotOK(t)
	})

	t.Run("VariadicPredicate alias", func(t *testing.T) {
		t.Parallel()
		p1 := And(Expr("TRUE")).As("abc")
		if diff := testutil.Diff(p1.GetAlias(), "abc"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		p2 := p1.As("def")
		if diff := testutil.Diff(p1.GetAlias(), "abc"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(p2.GetAlias(), "def"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("VariadicPredicate FaultySQL", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		// AND, 1 predicate
		tt.item = And(FaultySQL{})
		tt.assertErr(t, ErrFaultySQL)
		// AND, multiple predicates
		tt.item = And(Expr("FALSE"), FaultySQL{})
		tt.assertErr(t, ErrFaultySQL)
		// nested AND
		tt.item = And(And(FaultySQL{}))
		tt.assertErr(t, ErrFaultySQL)
	})
}

func TestQueryf(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Queryf("SELECT {field} FROM {table} WHERE {predicate}",
			sql.Named("field", Expr("name")),
			sql.Named("table", Expr("users")),
			sql.Named("predicate", Expr("user_id = {}", 5)),
		)
		tt.wantQuery = "SELECT name FROM users WHERE user_id = ?"
		tt.wantArgs = []any{5}
		tt.assert(t)
	})

	t.Run("select star", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		q := Queryf("SELECT {*} FROM {table} WHERE {predicate}",
			sql.Named("table", Expr("users")),
			sql.Named("predicate", Expr("user_id = {}", 5)),
		)
		if diff := testutil.Diff(q.GetDialect(), ""); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q2, ok := q.SetFetchableFields([]Field{Expr("name"), Expr("age")})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
		tt.item = q2
		tt.wantQuery = "SELECT name, age FROM users WHERE user_id = ?"
		tt.wantArgs = []any{5}
		tt.assert(t)
	})

	t.Run("escape curly brace", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		q := Queryf(`WITH cte AS (SELECT '{{*}' AS name) SELECT {*} FROM cte`)
		q2, ok := q.SetFetchableFields([]Field{Expr("name")})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
		tt.item = q2
		tt.wantQuery = "WITH cte AS (SELECT '{*}' AS name) SELECT name FROM cte"
		tt.assert(t)
	})

	t.Run("mixed", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		q := Queryf(`{1} {3} {name} {} SELECT {*} FROM {1} {} {name} {}`, 5, sql.Named("name", "bob"), 10)
		q2, ok := q.SetFetchableFields([]Field{Expr("alpha"), Expr("SUBSTR({}, {})", "apple", 77), Expr("beta")})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
		tt.dialect = DialectPostgres
		tt.item = q2
		tt.wantQuery = "$1 $2 $3 $4 SELECT alpha, SUBSTR($5, $6), beta FROM $1 $3 $3 $7"
		tt.wantArgs = []any{5, 10, "bob", 5, "apple", 77, 10}
		tt.wantParams = map[string][]int{"name": {2}}
		tt.assert(t)
	})

	t.Run("append", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		q := Queryf("SELECT {*} FROM tbl WHERE 1 = 1")
		q = q.Append("AND name = {}", "bob")
		q = q.Append("AND email = {email}", sql.Named("email", "bob@email.com"))
		q = q.Append("AND age = {age}", sql.Named("age", 27))
		q2, ok := q.SetFetchableFields([]Field{Expr("name"), Expr("email")})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
		tt.item = q2
		tt.wantQuery = "SELECT name, email FROM tbl WHERE 1 = 1 AND name = ? AND email = ? AND age = ?"
		tt.wantArgs = []any{"bob", "bob@email.com", 27}
		tt.wantParams = map[string][]int{"email": {1}, "age": {2}}
		tt.assert(t)
	})
}

func TestAssign(t *testing.T) {
	t.Run("AssignValue nil field", func(t *testing.T) {
		t.Parallel()
		_, _, err := ToSQL("", Set(nil, 1), nil)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("AssignValue", func(t *testing.T) {
		t.Parallel()
		TestTable{
			item:      Set(tmpfield("tbl.field"), 1),
			wantQuery: "field = ?", wantArgs: []any{1},
		}.assert(t)
	})

	t.Run("mysql AssignValue", func(t *testing.T) {
		t.Parallel()
		TestTable{
			dialect:   DialectMySQL,
			item:      Set(tmpfield("tbl.field"), 1),
			wantQuery: "tbl.field = ?", wantArgs: []any{1},
		}.assert(t)
	})

	t.Run("AssignValue err", func(t *testing.T) {
		t.Parallel()
		TestTable{
			item: Set(tmpfield("tbl.field"), FaultySQL{}),
		}.assertErr(t, ErrFaultySQL)
	})

	t.Run("Assignf nil field", func(t *testing.T) {
		t.Parallel()
		_, _, err := ToSQL("", Setf(nil, ""), nil)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("Assignf", func(t *testing.T) {
		t.Parallel()
		TestTable{
			item:      Setf(tmpfield("tbl.field"), "EXCLUDED.field"),
			wantQuery: "field = EXCLUDED.field",
		}.assert(t)
	})

	t.Run("Assignf err", func(t *testing.T) {
		t.Parallel()
		TestTable{
			item: Setf(tmpfield("tbl.field"), "EXCLUDED.{}", FaultySQL{}),
		}.assertErr(t, ErrFaultySQL)
	})
}

func TestRowValuesFieldsAssignments(t *testing.T) {
	tests := []TestTable{{
		description: "RowValue", item: RowValue{1, 2, 3},
		wantQuery: "(?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "RowValue with query",
		item:        RowValue{1, 2, Queryf("SELECT {}", 3)},
		wantQuery:   "(?, ?, (SELECT ?))",
		wantArgs:    []any{1, 2, 3},
	}, {
		description: "RowValue In",
		item:        RowValue{1, 2, 3}.In(RowValues{{4, 5, 6}, {7, 8, 9}}),
		wantQuery:   "(?, ?, ?) IN ((?, ?, ?), (?, ?, ?))",
		wantArgs:    []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, {
		description: "RowValue Eq",
		item:        RowValue{1, 2, 3}.Eq(RowValue{4, 5, 6}),
		wantQuery:   "(?, ?, ?) = (?, ?, ?)",
		wantArgs:    []any{1, 2, 3, 4, 5, 6},
	}, {
		description: "Fields",
		item:        Fields{Expr("tbl.f1"), Expr("tbl.f2"), Expr("tbl.f3")},
		wantQuery:   "tbl.f1, tbl.f2, tbl.f3",
	}, {
		description: "Assignments",
		item:        Assignments{Set(Expr("f1"), 1), Set(Expr("f2"), 2), Set(Expr("f3"), 3)},
		wantQuery:   "f1 = ?, f2 = ?, f3 = ?", wantArgs: []any{1, 2, 3},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}

	t.Run("nil fields/assignments", func(t *testing.T) {
		t.Parallel()
		// Fields
		TestTable{item: Fields{Expr("f1"), Expr("f2"), nil}}.assertNotOK(t)
		// Assignments
		TestTable{item: Assignments{Set(Expr("f1"), 1), Set(Expr("f2"), 2), nil}}.assertNotOK(t)
	})

	errTests := []TestTable{{
		description: "RowValue err", item: RowValue{1, 2, FaultySQL{}},
	}, {
		description: "RowValues err", item: RowValues{{1, 2, FaultySQL{}}},
	}, {
		description: "Fields err", item: Fields{Expr("f1"), Expr("f2"), FaultySQL{}},
	}, {
		description: "Assignments err",
		item:        Assignments{Set(Expr("f1"), 1), Set(Expr("f2"), 2), FaultySQL{}},
	}}

	for _, tt := range errTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertErr(t, ErrFaultySQL)
		})
	}
}

func TestToSQL(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		gotQuery, _, err := ToSQL("", Queryf("SELECT {fields} FROM {table}",
			sql.Named("fields", Fields{Expr("f1"), Expr("f2"), Expr("f3")}),
			sql.Named("table", Expr("tbl")),
		), nil)
		if err != nil {
			t.Error(testutil.Callers(), err)
		}
		wantQuery := "SELECT f1, f2, f3 FROM tbl"
		if diff := testutil.Diff(gotQuery, wantQuery); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("nil SQLWriter", func(t *testing.T) {
		_, _, err := ToSQL("", nil, nil)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("err", func(t *testing.T) {
		_, _, err := ToSQL("", Queryf("SELECT {}", FaultySQL{}), nil)
		if !errors.Is(err, ErrFaultySQL) {
			t.Errorf(testutil.Callers()+"expected '%v' but got '%v'", ErrFaultySQL, err)
		}
	})
}

func Test_in_cmp(t *testing.T) {
	tests := []TestTable{{
		description: "!Query IN !RowValue",
		item:        In(Expr("{}", "tom"), Queryf("SELECT name FROM users WHERE name LIKE {}", "t%")),
		wantQuery:   "? IN (SELECT name FROM users WHERE name LIKE ?)", wantArgs: []any{"tom", "t%"},
	}, {
		description: "!Query IN RowValue", item: In(Expr("name"), RowValue{"tom", "dick", "harry"}),
		wantQuery: "name IN (?, ?, ?)", wantArgs: []any{"tom", "dick", "harry"},
	}, {
		description: "Query IN !RowValue", item: In(Queryf("SELECT {}", "tom"), []string{"tom", "dick", "harry"}),
		wantQuery: "(SELECT ?) IN (?, ?, ?)", wantArgs: []any{"tom", "tom", "dick", "harry"},
	}, {
		description: "Query IN RowValue", item: In(Queryf("SELECT {}", "tom"), RowValue{"tom", "dick", "harry"}),
		wantQuery: "(SELECT ?) IN (?, ?, ?)", wantArgs: []any{"tom", "tom", "dick", "harry"},
	}, {
		description: "!Query = !Query", item: cmp("=", 1, 1),
		wantQuery: "? = ?", wantArgs: []any{1, 1},
	}, {
		description: "!Query = Query", item: cmp("=", Expr("score"), Queryf("SELECT score FROM users WHERE id = {}", 5)),
		wantQuery: "score = (SELECT score FROM users WHERE id = ?)", wantArgs: []any{5},
	}, {
		description: "Query = !Query", item: cmp("=", Queryf("SELECT score FROM users WHERE id = {}", 5), Expr("{}", 7)),
		wantQuery: "(SELECT score FROM users WHERE id = ?) = ?", wantArgs: []any{5, 7},
	}, {
		description: "Query = Query", item: cmp("=", Queryf("SELECT 1"), Queryf("SELECT 2")),
		wantQuery: "(SELECT 1) = (SELECT 2)",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

type policyTableStub struct {
	policy Predicate
	err    error
}

var _ PolicyTable = (*policyTableStub)(nil)

func (tbl policyTableStub) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString("policy_table_stub")
	return nil
}

func (tbl policyTableStub) GetAlias() string { return "" }

func (tbl policyTableStub) Policy(ctx context.Context, dialect string) (Predicate, error) {
	return tbl.policy, tbl.err
}

func (tbl policyTableStub) IsTable() {}

func Test_appendPolicy(t *testing.T) {
	type TT struct {
		description  string
		table        Table
		wantPolicies []Predicate
		wantErr      error
	}

	tests := []TT{{
		description: "table doesn't implement PolicyTable",
		table:       Expr("tbl"),
	}, {
		description: "PolicyTable returns err",
		table:       policyTableStub{err: ErrFaultySQL},
		wantErr:     ErrFaultySQL,
	}, {
		description:  "PolicyTable returns policy",
		table:        policyTableStub{policy: Expr("TRUE")},
		wantPolicies: []Predicate{Expr("TRUE")},
	}, {
		description: "PolicyTable returns nil policy",
		table:       policyTableStub{},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			policies, err := appendPolicy(context.Background(), "", nil, tt.table)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf(testutil.Callers()+"expected error '%v' but got '%v'", tt.wantErr, err)
			}
			if diff := testutil.Diff(policies, tt.wantPolicies); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}

func Test_appendPredicates(t *testing.T) {
	type TT struct {
		description   string
		predicate     Predicate
		predicates    []Predicate
		wantPredicate VariadicPredicate
	}

	p1, p2, p3 := Expr("p1"), Expr("p2"), Expr("p3")
	tests := []TT{{
		description: "nil predicate",
		predicate:   nil, predicates: []Predicate{p2, p3},
		wantPredicate: And(p2, p3),
	}, {
		description: "AND predicate",
		predicate:   And(p1), predicates: []Predicate{p2, p3},
		wantPredicate: And(p1, p2, p3),
	}, {
		description: "OR predicate",
		predicate:   Or(p1), predicates: []Predicate{p2, p3},
		wantPredicate: And(Or(p1), p2, p3),
	}, {
		description: "non-VariadicPredicate predicate",
		predicate:   p1, predicates: []Predicate{p2, p3},
		wantPredicate: And(p1, p2, p3),
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			gotPredicate := appendPredicates(tt.predicate, tt.predicates)
			if diff := testutil.Diff(gotPredicate, tt.wantPredicate); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}

func Test_substituteParams(t *testing.T) {
	t.Run("no params provided", func(t *testing.T) {
		t.Parallel()
		args := []any{1, 2, 3}
		params := map[string][]int{"one": {0}, "two": {1}, "three": {2}}
		gotArgs, err := substituteParams("", args, params, nil)
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
		wantArgs := []any{1, 2, 3}
		if diff := testutil.Diff(gotArgs, wantArgs); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("not all params provided", func(t *testing.T) {
		t.Parallel()
		args := []any{1, 2, 3}
		params := map[string][]int{"one": {0}, "two": {1}, "three": {2}}
		paramValues := Params{"one": "One", "two": "Two"}
		gotArgs, err := substituteParams("", args, params, paramValues)
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
		wantArgs := []any{"One", "Two", 3}
		if diff := testutil.Diff(gotArgs, wantArgs); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("params substituted", func(t *testing.T) {
		t.Parallel()
		type Data struct {
			id   int
			name string
		}
		args := []any{
			0,
			sql.Named("one", 1),
			sql.Named("two", 2),
			3,
		}
		params := map[string][]int{
			"zero":  {0},
			"one":   {1},
			"two":   {2},
			"three": {3},
		}
		paramValues := Params{
			"one":   "[one]",
			"two":   "[two]",
			"three": "[three]",
		}
		wantArgs := []any{
			0,
			sql.Named("one", "[one]"),
			sql.Named("two", "[two]"),
			"[three]",
		}
		gotArgs, err := substituteParams("", args, params, paramValues)
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
		if diff := testutil.Diff(gotArgs, wantArgs); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})
}

func Test_writeTop(t *testing.T) {
	type TT struct {
		description     string
		topLimit        any
		topPercentLimit any
		withTies        bool
		wantQuery       string
		wantArgs        []any
		wantParams      map[string][]int
	}

	t.Run("err", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		dialect := DialectSQLServer
		buf := &bytes.Buffer{}
		args := &[]any{}
		params := make(map[string][]int)
		// topLimit
		err := writeTop(ctx, dialect, buf, args, params, FaultySQL{}, nil, false)
		if !errors.Is(err, ErrFaultySQL) {
			t.Errorf(testutil.Callers()+"expected error '%v' but got '%v'", ErrFaultySQL, err)
		}
		// topPercentLimit
		err = writeTop(ctx, dialect, buf, args, params, nil, FaultySQL{}, false)
		if !errors.Is(err, ErrFaultySQL) {
			t.Errorf(testutil.Callers()+"expected error '%v' but got '%v'", ErrFaultySQL, err)
		}
	})

	tests := []TT{{
		description: "empty", topLimit: nil, topPercentLimit: nil, withTies: true,
		wantQuery: "", wantArgs: nil, wantParams: nil,
	}, {
		description: "TOP n", topLimit: 5,
		wantQuery: "TOP (@p1) ", wantArgs: []any{5},
	}, {
		description: "TOP n PERCENT", topPercentLimit: 10,
		wantQuery: "TOP (@p1) PERCENT ", wantArgs: []any{10},
	}, {
		description: "TOP n WITH TIES", topLimit: 5, withTies: true,
		wantQuery: "TOP (@p1) WITH TIES ", wantArgs: []any{5},
	}, {
		description: "TOP expr", topLimit: Expr("5"),
		wantQuery: "TOP (5) ",
	}, {
		description: "TOP param", topLimit: IntParam("limit", 5),
		wantQuery: "TOP (@limit) ", wantArgs: []any{sql.Named("limit", 5)},
		wantParams: map[string][]int{"limit": {0}},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dialect := DialectSQLServer
			buf := &bytes.Buffer{}
			args := &[]any{}
			params := make(map[string][]int)
			err := writeTop(ctx, dialect, buf, args, params, tt.topLimit, tt.topPercentLimit, tt.withTies)
			if err != nil {
				t.Error(testutil.Callers(), err)
			}
			if diff := testutil.Diff(buf.String(), tt.wantQuery); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			if diff := testutil.Diff(*args, tt.wantArgs); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			if len(params) > 0 || len(tt.wantParams) > 0 {
				if diff := testutil.Diff(params, tt.wantParams); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			}
		})
	}
}

type TestTable struct {
	description string
	ctx         context.Context
	dialect     string
	item        any
	wantQuery   string
	wantArgs    []any
	wantParams  map[string][]int
}

func (tt TestTable) assert(t *testing.T) {
	if tt.ctx == nil {
		tt.ctx = context.Background()
	}
	if tt.dialect == "" {
		if query, ok := tt.item.(Query); ok {
			tt.dialect = query.GetDialect()
		}
	}
	buf := &bytes.Buffer{}
	args := &[]any{}
	params := make(map[string][]int)
	err := WriteValue(tt.ctx, tt.dialect, buf, args, params, tt.item)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(buf.String(), tt.wantQuery); diff != "" {
		t.Error(testutil.Callers(), diff)
	}
	if len(*args) > 0 || len(tt.wantArgs) > 0 {
		if diff := testutil.Diff(*args, tt.wantArgs); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	}
	if len(params) > 0 || len(tt.wantParams) > 0 {
		if diff := testutil.Diff(params, tt.wantParams); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	}
}

func (tt TestTable) assertErr(t *testing.T, wantErr error) {
	if tt.ctx == nil {
		tt.ctx = context.Background()
	}
	if tt.dialect == "" {
		if query, ok := tt.item.(Query); ok {
			tt.dialect = query.GetDialect()
		}
	}
	buf := &bytes.Buffer{}
	args := &[]any{}
	params := make(map[string][]int)
	gotErr := WriteValue(tt.ctx, tt.dialect, buf, args, params, tt.item)
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf(testutil.Callers()+"expected error '%v' but got '%v'", wantErr, gotErr)
	}
}

func (tt TestTable) assertNotOK(t *testing.T) {
	if tt.ctx == nil {
		tt.ctx = context.Background()
	}
	if tt.dialect == "" {
		if query, ok := tt.item.(Query); ok {
			tt.dialect = query.GetDialect()
		}
	}
	buf := &bytes.Buffer{}
	args := &[]any{}
	params := make(map[string][]int)
	gotErr := WriteValue(tt.ctx, tt.dialect, buf, args, params, tt.item)
	if gotErr == nil {
		t.Fatal(testutil.Callers(), "expected error but got nil")
	}
}
