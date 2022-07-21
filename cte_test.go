package sq

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestCTE(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cte := NewCTE("cte", []string{"n"}, Queryf("SELECT 1")).Materialized().NotMaterialized().As("c")
		TestTable{item: cte, wantQuery: "cte"}.assert(t)
		field := NewAnyField("ff", TableStruct{name: "cte", alias: "c"})
		if diff := testutil.Diff(cte.Field("ff"), field); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(cte.materialized, sql.NullBool{Valid: true, Bool: false}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(cte.GetAlias(), "c"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})
}

func TestCTEs(t *testing.T) {
	type TT struct {
		description string
		dialect     string
		ctes        []CTE
		wantQuery   string
		wantArgs    []any
		wantParams  map[string][]int
	}

	tests := []TT{{
		description: "basic",
		ctes:        []CTE{NewCTE("cte", nil, Queryf("SELECT 1"))},
		wantQuery:   "WITH cte AS (SELECT 1) ",
	}, {
		description: "recursive",
		ctes: []CTE{
			NewCTE("cte", nil, Queryf("SELECT 1")),
			NewRecursiveCTE("nums", []string{"n"}, Union(
				Queryf("SELECT 1"),
				Queryf("SELECT n+1 FROM nums WHERE n < 10"),
			)),
		},
		wantQuery: "WITH RECURSIVE cte AS (SELECT 1)" +
			", nums (n) AS (SELECT 1 UNION SELECT n+1 FROM nums WHERE n < 10) ",
	}, {
		description: "mysql materialized",
		dialect:     DialectMySQL,
		ctes:        []CTE{NewCTE("cte", nil, Queryf("SELECT 1")).Materialized()},
		wantQuery:   "WITH cte AS (SELECT 1) ",
	}, {
		description: "postgres materialized",
		dialect:     DialectPostgres,
		ctes:        []CTE{NewCTE("cte", nil, Queryf("SELECT 1")).Materialized()},
		wantQuery:   "WITH cte AS MATERIALIZED (SELECT 1) ",
	}, {
		description: "postgres not materialized",
		dialect:     DialectPostgres,
		ctes:        []CTE{NewCTE("cte", nil, Queryf("SELECT 1")).NotMaterialized()},
		wantQuery:   "WITH cte AS NOT MATERIALIZED (SELECT 1) ",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			buf, args, params := bufpool.Get().(*bytes.Buffer), &[]any{}, make(map[string][]int)
			buf.Reset()
			defer bufpool.Put(buf)
			err := writeCTEs(context.Background(), tt.dialect, buf, args, params, tt.ctes)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(buf.String(), tt.wantQuery); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			if diff := testutil.Diff(*args, tt.wantArgs); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			if diff := testutil.Diff(params, tt.wantParams); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}

	t.Run("invalid cte", func(t *testing.T) {
		t.Parallel()
		buf, args, params := bufpool.Get().(*bytes.Buffer), &[]any{}, make(map[string][]int)
		buf.Reset()
		defer bufpool.Put(buf)
		// no name
		err := writeCTEs(context.Background(), "", buf, args, params, []CTE{
			NewCTE("", nil, Queryf("SELECT 1")),
		})
		if err == nil {
			t.Fatal(testutil.Callers(), "expected error but got nil")
		}
		// no query
		err = writeCTEs(context.Background(), "", buf, args, params, []CTE{
			NewCTE("cte", nil, nil),
		})
		if err == nil {
			t.Fatal(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("err", func(t *testing.T) {
		t.Parallel()
		buf, args, params := bufpool.Get().(*bytes.Buffer), &[]any{}, make(map[string][]int)
		buf.Reset()
		defer bufpool.Put(buf)
		// VariadicQuery
		err := writeCTEs(context.Background(), "", buf, args, params, []CTE{
			NewCTE("cte", nil, Union(
				Queryf("SELECT 1"),
				Queryf("SELECT {}", FaultySQL{}),
			)),
		})
		if !errors.Is(err, ErrFaultySQL) {
			t.Errorf(testutil.Callers()+"expected error %q but got %q", ErrFaultySQL, err)
		}
		// Query
		err = writeCTEs(context.Background(), "", buf, args, params, []CTE{
			NewCTE("cte", nil, Queryf("SELECT {}", FaultySQL{})),
		})
		if !errors.Is(err, ErrFaultySQL) {
			t.Errorf(testutil.Callers()+"expected error %q but got %q", ErrFaultySQL, err)
		}
	})
}

func TestVariadicQuery(t *testing.T) {
	q1, q2, q3 := Queryf("SELECT 1"), Queryf("SELECT 2"), Queryf("SELECT 3")
	tests := []TestTable{{
		description: "Union",
		item:        Union(q1, q2, q3),
		wantQuery:   "(SELECT 1 UNION SELECT 2 UNION SELECT 3)",
	}, {
		description: "UnionAll",
		item:        UnionAll(q1, q2, q3),
		wantQuery:   "(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)",
	}, {
		description: "Intersect",
		item:        Intersect(q1, q2, q3),
		wantQuery:   "(SELECT 1 INTERSECT SELECT 2 INTERSECT SELECT 3)",
	}, {
		description: "IntersectAll",
		item:        IntersectAll(q1, q2, q3),
		wantQuery:   "(SELECT 1 INTERSECT ALL SELECT 2 INTERSECT ALL SELECT 3)",
	}, {
		description: "Except",
		item:        Except(q1, q2, q3),
		wantQuery:   "(SELECT 1 EXCEPT SELECT 2 EXCEPT SELECT 3)",
	}, {
		description: "ExceptAll",
		item:        ExceptAll(q1, q2, q3),
		wantQuery:   "(SELECT 1 EXCEPT ALL SELECT 2 EXCEPT ALL SELECT 3)",
	}, {
		description: "No operator specified",
		item:        VariadicQuery{Queries: []Query{q1, q2, q3}},
		wantQuery:   "(SELECT 1 UNION SELECT 2 UNION SELECT 3)",
	}, {
		description: "nested VariadicQuery",
		item:        Union(Union(Union(q1, q2, q3))),
		wantQuery:   "(SELECT 1 UNION SELECT 2 UNION SELECT 3)",
	}, {
		description: "1 query",
		item:        Union(q1),
		wantQuery:   "SELECT 1",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}

	t.Run("invalid VariadicQuery", func(t *testing.T) {
		t.Parallel()
		// empty
		TestTable{item: Union()}.assertNotOK(t)
		// nil query
		TestTable{item: Union(nil)}.assertNotOK(t)
		// nil query
		TestTable{item: Union(q1, q2, nil)}.assertNotOK(t)
	})

	t.Run("err", func(t *testing.T) {
		t.Parallel()
		// VariadicQuery
		TestTable{
			item: Union(
				Union(
					Queryf("SELECT 1"),
					Queryf("SELECT {}", FaultySQL{}),
				),
			),
		}.assertErr(t, ErrFaultySQL)
		// Query
		TestTable{
			item: Union(Queryf("SELECT {}", FaultySQL{})),
		}.assertErr(t, ErrFaultySQL)
	})

	t.Run("SetFetchableFields", func(t *testing.T) {
		t.Parallel()
		_, ok := Union().SetFetchableFields([]Field{Expr("f1")})
		if ok {
			t.Error(testutil.Callers(), "expected not ok but got ok")
		}
	})

	t.Run("GetDialect", func(t *testing.T) {
		// empty VariadicQuery
		if diff := testutil.Diff(Union().GetDialect(), ""); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		// nil query
		if diff := testutil.Diff(Union(nil).GetDialect(), ""); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		// empty dialect propagated
		if diff := testutil.Diff(Union(Queryf("SELECT 1")).GetDialect(), ""); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})
}
