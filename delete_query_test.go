package sq

import (
	"testing"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestSQLiteDeleteQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("a")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := SQLite.DeleteFrom(a).Returning(a.FIRST_NAME).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectSQLite)
		fields := q1.GetFetchableFields()
		if diff := testutil.Diff(fields, []Field{a.FIRST_NAME}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("Delete Returning", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			DeleteFrom(a).
			Where(a.ACTOR_ID.EqInt(1)).
			Returning(a.FIRST_NAME, a.LAST_NAME)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" DELETE FROM actor AS a" +
			" WHERE a.actor_id = $1" +
			" RETURNING a.first_name, a.last_name"
		tt.wantArgs = []any{1}
		tt.assert(t)
	})
}

func TestPostgresDeleteQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("a")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := Postgres.DeleteFrom(a).Returning(a.FIRST_NAME).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectPostgres)
		fields := q1.GetFetchableFields()
		if diff := testutil.Diff(fields, []Field{a.FIRST_NAME}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("Delete Returning", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			DeleteFrom(a).
			Where(a.ACTOR_ID.EqInt(1)).
			Returning(a.FIRST_NAME, a.LAST_NAME)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" DELETE FROM actor AS a" +
			" WHERE a.actor_id = $1" +
			" RETURNING a.first_name, a.last_name"
		tt.wantArgs = []any{1}
		tt.assert(t)
	})

	t.Run("Join", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			DeleteFrom(a).
			Using(a).
			Join(a, Expr("1 = 1")).
			LeftJoin(a, Expr("1 = 1")).
			FullJoin(a, Expr("1 = 1")).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME)
		tt.wantQuery = "DELETE FROM actor AS a" +
			" USING actor AS a" +
			" JOIN actor AS a ON 1 = 1" +
			" LEFT JOIN actor AS a ON 1 = 1" +
			" FULL JOIN actor AS a ON 1 = 1" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)"
		tt.assert(t)
	})
}

func TestMySQLDeleteQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := MySQL.DeleteFrom(a).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectMySQL)
		fields := q1.GetFetchableFields()
		if len(fields) != 0 {
			t.Error(testutil.Callers(), "expected 0 fields but got %v", fields)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Error(testutil.Callers(), "expected not ok but got ok")
		}
	})

	t.Run("Where", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			DeleteFrom(a).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" DELETE FROM actor" +
			" WHERE actor.actor_id = ?"
		tt.wantArgs = []any{1}
		tt.assert(t)
	})

	t.Run("OrderBy Limit", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			DeleteFrom(a).
			OrderBy(a.ACTOR_ID).
			Limit(5)
		tt.wantQuery = "DELETE FROM actor" +
			" ORDER BY actor.actor_id" +
			" LIMIT ?"
		tt.wantArgs = []any{5}
		tt.assert(t)
	})

	t.Run("Delete Returning", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			DeleteFrom(a).
			Where(a.ACTOR_ID.EqInt(1)).
			Returning(a.FIRST_NAME, a.LAST_NAME)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" DELETE FROM actor" +
			" WHERE actor.actor_id = ?" +
			" RETURNING actor.first_name, actor.last_name"
		tt.wantArgs = []any{1}
		tt.assert(t)
	})

	t.Run("Join", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			Delete(a).
			From(a).
			Join(a, Expr("1 = 1")).
			LeftJoin(a, Expr("1 = 1")).
			FullJoin(a, Expr("1 = 1")).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME)
		tt.wantQuery = "DELETE actor" +
			" FROM actor" +
			" JOIN actor ON 1 = 1" +
			" LEFT JOIN actor ON 1 = 1" +
			" FULL JOIN actor ON 1 = 1" +
			" CROSS JOIN actor" +
			" , actor" +
			" JOIN actor USING (first_name, last_name)"
		tt.assert(t)
	})
}

func TestSQLServerDeleteQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := SQLServer.DeleteFrom(a).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectSQLServer)
		q1 = q1.SetDialect(DialectMySQL)
		fields := q1.GetFetchableFields()
		if len(fields) != 0 {
			t.Error(testutil.Callers(), "expected 0 fields but got %v", fields)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Error(testutil.Callers(), "expected not ok but got ok")
		}
	})

	t.Run("Where", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			DeleteFrom(a).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" DELETE FROM actor" +
			" WHERE actor.actor_id = @p1"
		tt.wantArgs = []any{1}
		tt.assert(t)
	})

	t.Run("Join", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			DeleteFrom(a).
			From(a).
			Join(a, Expr("1 = 1")).
			LeftJoin(a, Expr("1 = 1")).
			FullJoin(a, Expr("1 = 1")).
			CrossJoin(a).
			CustomJoin(",", a)
		tt.wantQuery = "DELETE FROM actor" +
			" FROM actor" +
			" JOIN actor ON 1 = 1" +
			" LEFT JOIN actor ON 1 = 1" +
			" FULL JOIN actor ON 1 = 1" +
			" CROSS JOIN actor" +
			" , actor"
		tt.assert(t)
	})
}

func TestDeleteQuery(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := DeleteQuery{DeleteTable: Expr("tbl"), Dialect: "lorem ipsum"}
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("PolicyTable", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = DeleteQuery{
			DeleteTable:    policyTableStub{policy: And(Expr("1 = 1"), Expr("2 = 2"))},
			WherePredicate: Expr("3 = 3"),
		}
		tt.wantQuery = "DELETE FROM policy_table_stub WHERE (1 = 1 AND 2 = 2) AND 3 = 3"
		tt.assert(t)
	})

	notOKTests := []TestTable{{
		description: "nil FromTable not allowed",
		item: DeleteQuery{
			DeleteTable: nil,
		},
	}, {
		description: "sqlite does not support JOIN",
		item: DeleteQuery{
			Dialect:     DialectSQLite,
			DeleteTable: Expr("tbl"),
			UsingTable:  Expr("tbl"),
			JoinTables: []JoinTable{
				Join(Expr("tbl"), Expr("1 = 1")),
			},
		},
	}, {
		description: "postgres does not allow JOIN without USING",
		item: DeleteQuery{
			Dialect:     DialectPostgres,
			DeleteTable: Expr("tbl"),
			JoinTables: []JoinTable{
				Join(Expr("tbl"), Expr("1 = 1")),
			},
		},
	}, {
		description: "dialect does not support ORDER BY",
		item: DeleteQuery{
			Dialect:       DialectPostgres,
			DeleteTable:   Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
		},
	}, {
		description: "dialect does not support LIMIT",
		item: DeleteQuery{
			Dialect:     DialectPostgres,
			DeleteTable: Expr("tbl"),
			LimitRows:   5,
		},
	}}

	for _, tt := range notOKTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertNotOK(t)
		})
	}

	errTests := []TestTable{{
		description: "FromTable Policy err",
		item: DeleteQuery{
			DeleteTable: policyTableStub{err: ErrFaultySQL},
		},
	}, {
		description: "UsingTable Policy err",
		item: DeleteQuery{
			DeleteTable: Expr("tbl"),
			UsingTable:  policyTableStub{err: ErrFaultySQL},
		},
	}, {
		description: "JoinTables Policy err",
		item: DeleteQuery{
			DeleteTable: Expr("tbl"),
			UsingTable:  Expr("tbl"),
			JoinTables: []JoinTable{
				Join(policyTableStub{err: ErrFaultySQL}, Expr("1 = 1")),
			},
		},
	}, {
		description: "CTEs err",
		item: DeleteQuery{
			CTEs:        []CTE{NewCTE("cte", nil, Queryf("SELECT {}", FaultySQL{}))},
			DeleteTable: Expr("tbl"),
		},
	}, {
		description: "FromTable err",
		item: DeleteQuery{
			DeleteTable: FaultySQL{},
		},
	}, {
		description: "postgres UsingTable err",
		item: DeleteQuery{
			Dialect:     DialectPostgres,
			DeleteTable: Expr("tbl"),
			UsingTable:  FaultySQL{},
		},
	}, {
		description: "sqlserver UsingTable err",
		item: DeleteQuery{
			Dialect:     DialectSQLServer,
			DeleteTable: Expr("tbl"),
			UsingTable:  FaultySQL{},
		},
	}, {
		description: "JoinTables err",
		item: DeleteQuery{
			Dialect:     DialectPostgres,
			DeleteTable: Expr("tbl"),
			UsingTable:  Expr("tbl"),
			JoinTables: []JoinTable{
				Join(Expr("tbl"), FaultySQL{}),
			},
		},
	}, {
		description: "WherePredicate Variadic err",
		item: DeleteQuery{
			DeleteTable:    Expr("tbl"),
			WherePredicate: And(FaultySQL{}),
		},
	}, {
		description: "WherePredicate err",
		item: DeleteQuery{
			DeleteTable:    Expr("tbl"),
			WherePredicate: FaultySQL{},
		},
	}, {
		description: "OrderByFields err",
		item: DeleteQuery{
			Dialect:       DialectMySQL,
			DeleteTable:   Expr("tbl"),
			OrderByFields: Fields{FaultySQL{}},
		},
	}, {
		description: "LimitRows err",
		item: DeleteQuery{
			Dialect:       DialectMySQL,
			DeleteTable:   Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
			LimitRows:     FaultySQL{},
		},
	}, {
		description: "ReturningFields err",
		item: DeleteQuery{
			Dialect:         DialectPostgres,
			DeleteTable:     Expr("tbl"),
			ReturningFields: Fields{FaultySQL{}},
		},
	}}

	for _, tt := range errTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertErr(t, ErrFaultySQL)
		})
	}
}
