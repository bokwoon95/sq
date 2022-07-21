package sq

import (
	"testing"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestSQLiteSelectQuery(t *testing.T) {
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
		q1 := SQLite.SelectOne().From(a).SetDialect("lorem ipsum").As("q1")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(q1.GetAlias(), "q1"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.ACTOR_ID})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("subquery, cte and joins", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		subquery := SQLite.Select(Expr("*")).From(a).As("subquery")
		cte := NewCTE("cte", nil, SQLite.From(a).Select(Expr("*")))
		tt.item = SQLite.
			With(cte).
			From(a).From(a).
			Join(subquery, Eq(subquery.Field("actor_id"), a.ACTOR_ID)).
			LeftJoin(cte, Eq(cte.Field("actor_id"), a.ACTOR_ID)).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME).
			SelectOne()
		tt.wantQuery = "WITH cte AS (SELECT * FROM actor AS a)" +
			" SELECT 1" +
			" FROM actor AS a" +
			" JOIN (SELECT * FROM actor AS a) AS subquery ON subquery.actor_id = a.actor_id" +
			" LEFT JOIN cte ON cte.actor_id = a.actor_id" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)"
		tt.assert(t)
	})

	t.Run("all", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			From(a).
			Where(a.ACTOR_ID.GtInt(5)).
			GroupBy(a.FIRST_NAME).
			Having(a.FIRST_NAME.IsNotNull()).
			OrderBy(a.LAST_NAME).
			Limit(10).
			Offset(20)
		tt.wantQuery = "SELECT DISTINCT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" WHERE a.actor_id > $1" +
			" GROUP BY a.first_name" +
			" HAVING a.first_name IS NOT NULL" +
			" ORDER BY a.last_name" +
			" LIMIT $2" +
			" OFFSET $3"
		tt.wantArgs = []any{5, 10, 20}
		tt.assert(t)
	})
}

func TestPostgresSelectQuery(t *testing.T) {
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
		q1 := Postgres.SelectOne().From(a).SetDialect("lorem ipsum").As("q1")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(q1.GetAlias(), "q1"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.ACTOR_ID})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("subquery, cte and joins", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		subquery := Postgres.Select(Expr("*")).From(a).As("subquery")
		cte := NewCTE("cte", nil, Postgres.From(a).Select(Expr("*")))
		tt.item = Postgres.
			With(cte).
			From(a).From(a).
			Join(subquery, Eq(subquery.Field("actor_id"), a.ACTOR_ID)).
			LeftJoin(cte, Eq(cte.Field("actor_id"), a.ACTOR_ID)).
			FullJoin(a, Expr("1 = 1")).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME).
			SelectOne()
		tt.wantQuery = "WITH cte AS (SELECT * FROM actor AS a)" +
			" SELECT 1" +
			" FROM actor AS a" +
			" JOIN (SELECT * FROM actor AS a) AS subquery ON subquery.actor_id = a.actor_id" +
			" LEFT JOIN cte ON cte.actor_id = a.actor_id" +
			" FULL JOIN actor AS a ON 1 = 1" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)"
		tt.assert(t)
	})

	t.Run("all", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			From(a).
			Where(a.ACTOR_ID.GtInt(5)).
			GroupBy(a.FIRST_NAME).
			Having(a.FIRST_NAME.IsNotNull()).
			OrderBy(a.LAST_NAME).
			Limit(10).
			Offset(20)
		tt.wantQuery = "SELECT DISTINCT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" WHERE a.actor_id > $1" +
			" GROUP BY a.first_name" +
			" HAVING a.first_name IS NOT NULL" +
			" ORDER BY a.last_name" +
			" LIMIT $2" +
			" OFFSET $3"
		tt.wantArgs = []any{5, 10, 20}
		tt.assert(t)
	})

	t.Run("DistinctOn", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			DistinctOn(a.FIRST_NAME, a.LAST_NAME).
			From(a)
		tt.wantQuery = "SELECT DISTINCT ON (a.first_name, a.last_name)" +
			" a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a"
		tt.assert(t)
	})

	t.Run("FetchNext, WithTies, LockRows", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			From(a).
			OrderBy(a.ACTOR_ID).
			Offset(10).
			FetchNext(20).WithTies().
			LockRows("FOR UPDATE")
		tt.wantQuery = "SELECT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" ORDER BY a.actor_id" +
			" OFFSET $1" +
			" FETCH NEXT $2 ROWS WITH TIES" +
			" FOR UPDATE"
		tt.wantArgs = []any{10, 20}
		tt.assert(t)
	})
}

func TestMySQLSelectQuery(t *testing.T) {
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
		q1 := MySQL.SelectOne().From(a).SetDialect("lorem ipsum").As("q1")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(q1.GetAlias(), "q1"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.ACTOR_ID})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("subquery, cte and joins", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		subquery := MySQL.Select(Expr("*")).From(a).As("subquery")
		cte := NewCTE("cte", nil, MySQL.From(a).Select(Expr("*")))
		tt.item = MySQL.
			With(cte).
			From(a).From(a).
			Join(subquery, Eq(subquery.Field("actor_id"), a.ACTOR_ID)).
			LeftJoin(cte, Eq(cte.Field("actor_id"), a.ACTOR_ID)).
			FullJoin(a, Expr("1 = 1")).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME).
			SelectOne()
		tt.wantQuery = "WITH cte AS (SELECT * FROM actor AS a)" +
			" SELECT 1" +
			" FROM actor AS a" +
			" JOIN (SELECT * FROM actor AS a) AS subquery ON subquery.actor_id = a.actor_id" +
			" LEFT JOIN cte ON cte.actor_id = a.actor_id" +
			" FULL JOIN actor AS a ON 1 = 1" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)"
		tt.assert(t)
	})

	t.Run("all", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			From(a).
			Where(a.ACTOR_ID.GtInt(5)).
			GroupBy(a.FIRST_NAME).
			Having(a.FIRST_NAME.IsNotNull()).
			OrderBy(a.LAST_NAME).
			Limit(10).
			Offset(20)
		tt.wantQuery = "SELECT DISTINCT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" WHERE a.actor_id > ?" +
			" GROUP BY a.first_name" +
			" HAVING a.first_name IS NOT NULL" +
			" ORDER BY a.last_name" +
			" LIMIT ?" +
			" OFFSET ?"
		tt.wantArgs = []any{5, 10, 20}
		tt.assert(t)
	})

	t.Run("LockRows", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			From(a).
			OrderBy(a.ACTOR_ID).
			Offset(10).
			LockRows("FOR UPDATE")
		tt.wantQuery = "SELECT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" ORDER BY a.actor_id" +
			" OFFSET ?" +
			" FOR UPDATE"
		tt.wantArgs = []any{10}
		tt.assert(t)
	})
}

func TestSQLServerSelectQuery(t *testing.T) {
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
		q1 := SQLServer.SelectOne().From(a).SetDialect("lorem ipsum").As("q1")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(q1.GetAlias(), "q1"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.ACTOR_ID})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("subquery, cte and joins", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		subquery := SQLServer.Select(Expr("*")).From(a).As("subquery")
		cte := NewCTE("cte", nil, SQLServer.From(a).Select(Expr("*")))
		tt.item = SQLServer.
			With(cte).
			From(a).From(a).
			Join(subquery, Eq(subquery.Field("actor_id"), a.ACTOR_ID)).
			LeftJoin(cte, Eq(cte.Field("actor_id"), a.ACTOR_ID)).
			FullJoin(a, Expr("1 = 1")).
			CrossJoin(a).
			CustomJoin(",", a).
			SelectOne()
		tt.wantQuery = "WITH cte AS (SELECT * FROM actor AS a)" +
			" SELECT 1" +
			" FROM actor AS a" +
			" JOIN (SELECT * FROM actor AS a) AS subquery ON subquery.actor_id = a.actor_id" +
			" LEFT JOIN cte ON cte.actor_id = a.actor_id" +
			" FULL JOIN actor AS a ON 1 = 1" +
			" CROSS JOIN actor AS a" +
			" , actor AS a"
		tt.assert(t)
	})

	t.Run("all", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			SelectDistinct(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			From(a).
			Where(a.ACTOR_ID.GtInt(5)).
			GroupBy(a.FIRST_NAME).
			Having(a.FIRST_NAME.IsNotNull()).
			OrderBy(a.LAST_NAME).
			Offset(10).
			FetchNext(20)
		tt.wantQuery = "SELECT DISTINCT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" WHERE a.actor_id > @p1" +
			" GROUP BY a.first_name" +
			" HAVING a.first_name IS NOT NULL" +
			" ORDER BY a.last_name" +
			" OFFSET @p2 ROWS" +
			" FETCH NEXT @p3 ROWS ONLY"
		tt.wantArgs = []any{5, 10, 20}
		tt.assert(t)
	})

	t.Run("TopPercent", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			TopPercent(5).
			From(a).
			OrderBy(a.ACTOR_ID)
		tt.wantQuery = "SELECT TOP (@p1) PERCENT a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a" +
			" ORDER BY a.actor_id"
		tt.wantArgs = []any{5}
		tt.assert(t)
	})

	t.Run("Top, WithTies", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
			Top(10).WithTies().
			From(Expr("{} AS a WITH (UPDLOCK, ROWLOCK)", a)).
			OrderBy(a.ACTOR_ID)
		tt.wantQuery = "SELECT TOP (@p1) WITH TIES a.actor_id, a.first_name, a.last_name" +
			" FROM actor AS a WITH (UPDLOCK, ROWLOCK)" +
			" ORDER BY a.actor_id"
		tt.wantArgs = []any{10}
		tt.assert(t)
	})
}

func TestSelectQuery(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := SelectQuery{FromTable: Expr("tbl"), Dialect: "lorem ipsum", Alias: "q1"}
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(q1.GetAlias(), "q1"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{Expr("f1")})
		if !ok {
			t.Fatal(testutil.Callers(), "not ok")
		}
	})

	t.Run("PolicyTable", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SelectQuery{
			SelectFields:   Fields{Expr("1")},
			FromTable:      policyTableStub{policy: And(Expr("1 = 1"), Expr("2 = 2"))},
			WherePredicate: Expr("3 = 3"),
		}
		tt.wantQuery = "SELECT 1 FROM policy_table_stub WHERE (1 = 1 AND 2 = 2) AND 3 = 3"
		tt.assert(t)
	})

	t.Run("Where Having Window", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		w1 := NamedWindow{Name: "w1", Definition: PartitionBy(Expr("f1"))}
		w2 := NamedWindow{Name: "w2", Definition: OrderBy(Expr("f2"))}
		w3 := NamedWindow{Name: "w3", Definition: OrderBy(Expr("f3")).Frame("ROWS UNBOUNDED PRECEDING")}
		tt.item = SelectQuery{
			SelectFields:    Fields{CountStarOver(w1), CountStarOver(w2), CountStarOver(w3)},
			FromTable:       Expr("tbl"),
			WherePredicate:  Expr("1 = 1"),
			GroupByFields:   Fields{Expr("f2")},
			HavingPredicate: Expr("2 = 2"),
			NamedWindows:    NamedWindows{w1, w2, w3},
		}
		tt.wantQuery = "SELECT COUNT(*) OVER w1, COUNT(*) OVER w2, COUNT(*) OVER w3" +
			" FROM tbl" +
			" WHERE 1 = 1" +
			" GROUP BY f2" +
			" HAVING 2 = 2" +
			" WINDOW w1 AS (PARTITION BY f1)" +
			", w2 AS (ORDER BY f2)" +
			", w3 AS (ORDER BY f3 ROWS UNBOUNDED PRECEDING)"
		tt.assert(t)
	})

	notOKTests := []TestTable{{
		description: "no fields provided not allowed",
		item:        SelectQuery{},
	}, {
		description: "dialect does not support TOP",
		item: SelectQuery{
			Dialect:      DialectSQLite,
			SelectFields: Fields{Expr("f1")},
			LimitTop:     5,
		},
	}, {
		description: "sqlserver does not allow TOP without ORDER BY",
		item: SelectQuery{
			Dialect:      DialectSQLServer,
			SelectFields: Fields{Expr("f1")},
			LimitTop:     5,
		},
	}, {
		description: "dialect does not support DISTINCT ON",
		item: SelectQuery{
			Dialect:          DialectSQLite,
			SelectFields:     Fields{Expr("f1")},
			DistinctOnFields: Fields{Expr("f2")},
		},
	}, {
		description: "postgres does not allow both DISTINCT and DISTINCT ON",
		item: SelectQuery{
			Dialect:          DialectPostgres,
			SelectFields:     Fields{Expr("f1")},
			Distinct:         true,
			DistinctOnFields: Fields{Expr("f2")},
		},
	}, {
		description: "postgres does not allow subquery no alias",
		item: SelectQuery{
			Dialect:      DialectPostgres,
			SelectFields: Fields{Expr("f1")},
			FromTable:    SelectQuery{SelectFields: Fields{Expr("f1")}},
		},
	}, {
		description: "JOIN without FROM not allowed",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			JoinTables: []JoinTable{
				Join(Expr("tbl"), Expr("1 = 1")),
			},
		},
	}, {
		description: "sqlserver does not support LIMIT",
		item: SelectQuery{
			Dialect:      DialectSQLServer,
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			LimitRows:    5,
		},
	}, {
		description: "sqlserver does not allow OFFSET without ORDER BY",
		item: SelectQuery{
			Dialect:      DialectSQLServer,
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			OffsetRows:   5,
		},
	}, {
		description: "sqlserver does not support OFFSET with TOP",
		item: SelectQuery{
			Dialect:       DialectSQLServer,
			SelectFields:  Fields{Expr("f1")},
			LimitTop:      5,
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
			OffsetRows:    10,
		},
	}, {
		description: "postgres does not allow FETCH NEXT with LIMIT",
		item: SelectQuery{
			Dialect:       DialectPostgres,
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
			LimitRows:     5,
			OffsetRows:    10,
			FetchNextRows: 20,
		},
	}, {
		description: "sqlserver does not allow FETCH NEXT with TOP",
		item: SelectQuery{
			Dialect:       DialectSQLServer,
			SelectFields:  Fields{Expr("f1")},
			LimitTop:      5,
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
			FetchNextRows: 10,
		},
	}, {
		description: "dialect does not support FETCH NEXT",
		item: SelectQuery{
			Dialect:       DialectSQLite,
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
			FetchNextRows: 20,
		},
	}, {
		description: "sqlserver does not support FETCH NEXT with WITH TIES",
		item: SelectQuery{
			Dialect:       DialectSQLServer,
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
			FetchNextRows: 20,
			FetchWithTies: true,
		},
	}, {
		description: "postgres does not allow WITH TIES without ORDER BY",
		item: SelectQuery{
			Dialect:       DialectPostgres,
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			FetchNextRows: 20,
			FetchWithTies: true,
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
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    policyTableStub{err: ErrFaultySQL},
		},
	}, {
		description: "JoinTables Policy err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			JoinTables: []JoinTable{
				Join(policyTableStub{err: ErrFaultySQL}, Expr("1 = 1")),
			},
		},
	}, {
		description: "CTEs err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			CTEs: []CTE{
				NewCTE("cte", nil, Queryf("{}", FaultySQL{})),
			},
			FromTable: Expr("tbl"),
		},
	}, {
		description: "sqlserver LimitTop err",
		item: SelectQuery{
			Dialect:       DialectSQLServer,
			SelectFields:  Fields{Expr("f1")},
			LimitTop:      FaultySQL{},
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{Expr("f1")},
		},
	}, {
		description: "postgres DistinctOnFields err",
		item: SelectQuery{
			Dialect:          DialectPostgres,
			SelectFields:     Fields{Expr("f1")},
			DistinctOnFields: Fields{FaultySQL{}},
			FromTable:        Expr("tbl"),
		},
	}, {
		description: "SelectFields err",
		item: SelectQuery{
			SelectFields: Fields{FaultySQL{}},
			FromTable:    Expr("tbl"),
		},
	}, {
		description: "FromTable err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    FaultySQL{},
		},
	}, {
		description: "JoinTables err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			JoinTables: []JoinTable{
				Join(Expr("tbl"), FaultySQL{}),
			},
		},
	}, {
		description: "WherePredicate VariadicPredicate err",
		item: SelectQuery{
			SelectFields:   Fields{Expr("f1")},
			FromTable:      Expr("tbl"),
			WherePredicate: And(FaultySQL{}),
		},
	}, {
		description: "WherePredicate err",
		item: SelectQuery{
			SelectFields:   Fields{Expr("f1")},
			FromTable:      Expr("tbl"),
			WherePredicate: FaultySQL{},
		},
	}, {
		description: "GroupBy err",
		item: SelectQuery{
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			GroupByFields: Fields{FaultySQL{}},
		},
	}, {
		description: "HavingPredicate VariadicPredicate err",
		item: SelectQuery{
			SelectFields:    Fields{Expr("f1")},
			FromTable:       Expr("tbl"),
			GroupByFields:   Fields{Expr("f1")},
			HavingPredicate: And(FaultySQL{}),
		},
	}, {
		description: "HavingPredicate err",
		item: SelectQuery{
			SelectFields:    Fields{Expr("f1")},
			FromTable:       Expr("tbl"),
			GroupByFields:   Fields{Expr("f1")},
			HavingPredicate: FaultySQL{},
		},
	}, {
		description: "NamedWindows err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			NamedWindows: NamedWindows{{
				Name:       "w",
				Definition: OrderBy(FaultySQL{}),
			}},
		},
	}, {
		description: "OrderByFields err",
		item: SelectQuery{
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			OrderByFields: Fields{FaultySQL{}},
		},
	}, {
		description: "LimitRows err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			LimitRows:    FaultySQL{},
		},
	}, {
		description: "OffsetRows err",
		item: SelectQuery{
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			OffsetRows:   FaultySQL{},
		},
	}, {
		description: "FetchNext err",
		item: SelectQuery{
			Dialect:       DialectPostgres,
			SelectFields:  Fields{Expr("f1")},
			FromTable:     Expr("tbl"),
			FetchNextRows: FaultySQL{},
		},
	}, {
		description: "LockClause err",
		item: SelectQuery{
			Dialect:      DialectPostgres,
			SelectFields: Fields{Expr("f1")},
			FromTable:    Expr("tbl"),
			LockClause:   "FOR UPDATE OF {}",
			LockValues:   []any{FaultySQL{}},
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
