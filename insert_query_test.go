package sq

import (
	"testing"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestSQLiteInsertQuery(t *testing.T) {
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
		q1 := SQLite.InsertInto(a).Returning(a.FIRST_NAME).SetDialect("lorem ipsum")
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

	t.Run("Columns Values", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland")
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("ColumnValues", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			ColumnValues(func(col *Column){
				// bob
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
				// alice
				col.SetString(a.FIRST_NAME, "alice")
				col.SetString(a.LAST_NAME, "in wonderland")
			})
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("Select Returning", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Select(SQLite.Select(a.FIRST_NAME, a.LAST_NAME).From(a)).
			Returning(a.ACTOR_ID)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" SELECT a.first_name, a.last_name FROM actor AS a" +
			" RETURNING a.actor_id"
		tt.assert(t)
	})

	t.Run("OnConflict DoNothing", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland").
			OnConflict(a.FIRST_NAME, a.LAST_NAME).
			Where(And(a.FIRST_NAME.IsNotNull(), a.LAST_NAME.IsNotNull())).
			DoNothing()
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)" +
			" ON CONFLICT (first_name, last_name)" +
			" WHERE a.first_name IS NOT NULL AND a.last_name IS NOT NULL" +
			" DO NOTHING"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("OnConflict DoUpdateSet", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland").
			OnConflict(a.FIRST_NAME, a.LAST_NAME).
			DoUpdateSet(
				a.FIRST_NAME.Set(a.FIRST_NAME.WithPrefix("EXCLUDED")),
				a.LAST_NAME.Set(a.LAST_NAME.WithPrefix("EXCLUDED")),
			).
			Where(And(a.FIRST_NAME.IsNotNull(), a.LAST_NAME.IsNotNull()))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)" +
			" ON CONFLICT (first_name, last_name)" +
			" DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name" +
			" WHERE a.first_name IS NOT NULL AND a.last_name IS NOT NULL"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})
}

func TestPostgresInsertQuery(t *testing.T) {
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
		q1 := Postgres.InsertInto(a).Returning(a.FIRST_NAME).SetDialect("lorem ipsum")
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

	t.Run("Columns Values", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland")
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("ColumnValues", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			ColumnValues(func(col *Column) {
				// bob
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
				// alice
				col.SetString(a.FIRST_NAME, "alice")
				col.SetString(a.LAST_NAME, "in wonderland")
			})
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("Select Returning", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Select(Postgres.Select(a.FIRST_NAME, a.LAST_NAME).From(a)).
			Returning(a.ACTOR_ID)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" SELECT a.first_name, a.last_name FROM actor AS a" +
			" RETURNING a.actor_id"
		tt.assert(t)
	})

	t.Run("OnConflict DoNothing", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland").
			OnConflict(a.FIRST_NAME, a.LAST_NAME).
			Where(And(a.FIRST_NAME.IsNotNull(), a.LAST_NAME.IsNotNull())).
			DoNothing()
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)" +
			" ON CONFLICT (first_name, last_name)" +
			" WHERE a.first_name IS NOT NULL AND a.last_name IS NOT NULL" +
			" DO NOTHING"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("OnConflictOnConstraint DoUpdateSet", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland").
			OnConflictOnConstraint("actor_first_name_last_name_key").
			DoUpdateSet(
				a.FIRST_NAME.Set(a.FIRST_NAME.WithPrefix("EXCLUDED")),
				a.LAST_NAME.Set(a.LAST_NAME.WithPrefix("EXCLUDED")),
			).
			Where(And(a.FIRST_NAME.IsNotNull(), a.LAST_NAME.IsNotNull()))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor AS a (first_name, last_name)" +
			" VALUES ($1, $2), ($3, $4)" +
			" ON CONFLICT ON CONSTRAINT actor_first_name_last_name_key" +
			" DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name" +
			" WHERE a.first_name IS NOT NULL AND a.last_name IS NOT NULL"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})
}

func TestMySQLInsertQuery(t *testing.T) {
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
		q1 := MySQL.InsertInto(a).SetDialect("lorem ipsum")
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

	t.Run("Columns Values", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland")
		tt.wantQuery = "INSERT INTO actor (first_name, last_name)" +
			" VALUES (?, ?), (?, ?)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("ColumnValues", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			InsertInto(a).
			ColumnValues(func(col *Column) {
				// bob
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
				// alice
				col.SetString(a.FIRST_NAME, "alice")
				col.SetString(a.LAST_NAME, "in wonderland")
			})
		tt.wantQuery = "INSERT INTO actor (first_name, last_name)" +
			" VALUES (?, ?), (?, ?)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("Select InsertIgnore", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			InsertIgnoreInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Select(MySQL.Select(a.FIRST_NAME, a.LAST_NAME).From(a))
		tt.wantQuery = "INSERT IGNORE INTO actor (first_name, last_name)" +
			" SELECT actor.first_name, actor.last_name FROM actor"
		tt.assert(t)
	})

	t.Run("OnDuplicateKey DoNothing", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland").
			OnDuplicateKeyUpdate(a.FIRST_NAME.Set(a.FIRST_NAME), a.LAST_NAME.Set(a.LAST_NAME))
		tt.wantQuery = "INSERT INTO actor (first_name, last_name)" +
			" VALUES (?, ?), (?, ?)" +
			" ON DUPLICATE KEY UPDATE actor.first_name = actor.first_name, actor.last_name = actor.last_name"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("OnDuplicateKey", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland").
			As("new").
			OnDuplicateKeyUpdate(
				a.FIRST_NAME.Set(a.FIRST_NAME.WithPrefix("new")),
				a.LAST_NAME.Set(a.LAST_NAME.WithPrefix("new")),
			)
		tt.wantQuery = "INSERT INTO actor (first_name, last_name)" +
			" VALUES (?, ?), (?, ?) AS new" +
			" ON DUPLICATE KEY UPDATE actor.first_name = new.first_name, actor.last_name = new.last_name"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})
}

func TestSQLServerInsertQuery(t *testing.T) {
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
		q1 := SQLServer.InsertInto(a).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectSQLServer)
		fields := q1.GetFetchableFields()
		if len(fields) != 0 {
			t.Error(testutil.Callers(), "expected 0 fields but got %v", fields)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Error(testutil.Callers(), "expected not ok but got ok")
		}
	})

	t.Run("Columns Values", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Values("bob", "the builder").
			Values("alice", "in wonderland")
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor (first_name, last_name)" +
			" VALUES (@p1, @p2), (@p3, @p4)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("ColumnValues", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			ColumnValues(func(col *Column) {
				// bob
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
				// alice
				col.SetString(a.FIRST_NAME, "alice")
				col.SetString(a.LAST_NAME, "in wonderland")
			})
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor (first_name, last_name)" +
			" VALUES (@p1, @p2), (@p3, @p4)"
		tt.wantArgs = []any{"bob", "the builder", "alice", "in wonderland"}
		tt.assert(t)
	})

	t.Run("Select", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			InsertInto(a).
			Columns(a.FIRST_NAME, a.LAST_NAME).
			Select(SQLServer.Select(a.FIRST_NAME, a.LAST_NAME).From(a))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" INSERT INTO actor (first_name, last_name)" +
			" SELECT actor.first_name, actor.last_name FROM actor"
		tt.assert(t)
	})
}

func TestInsertQuery(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := InsertQuery{InsertTable: Expr("tbl"), Dialect: "lorem ipsum"}
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	f1, f2, f3 := Expr("f1"), Expr("f2"), Expr("f3")
	colmapper := func(col *Column) {
		col.Set(f1, 1)
		col.Set(f2, 2)
		col.Set(f3, 3)
	}

	notOKTests := []TestTable{{
		description: "mysql does not support CTEs with INSERT",
		item: InsertQuery{
			Dialect:      DialectMySQL,
			CTEs:         []CTE{NewCTE("cte", nil, Queryf("SELECT 1"))},
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
		},
	}, {
		description: "dialect does not support INSERT IGNORE",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			InsertIgnore: true,
			ColumnMapper: colmapper,
		},
	}, {
		description: "nil IntoTable not allowed",
		item: InsertQuery{
			InsertTable:  nil,
			ColumnMapper: colmapper,
		},
	}, {
		description: "dialect does not support IntoTable alias",
		item: InsertQuery{
			Dialect:      DialectMySQL,
			InsertTable:  Expr("tbl").As("t"),
			ColumnMapper: colmapper,
		},
	}, {
		description: "nil Field in InsertColumns not allowed",
		item: InsertQuery{
			Dialect:       DialectMySQL,
			InsertTable:   Expr("tbl"),
			InsertColumns: Fields{nil},
			RowValues:     RowValues{{1}},
		},
	}, {
		description: "dialect does not support row alias",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			RowAlias:     "new",
		},
	}, {
		description: "missing both Values and Select not allowed (either one is required)",
		item: InsertQuery{
			InsertTable: Expr("tbl"),
		},
	}, {
		description: "missing both Values and Select not allowed (either one is required)",
		item: InsertQuery{
			InsertTable: Expr("tbl"),
		},
	}, {
		description: "nil Field in ConflictFields not allowed",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict:     ConflictClause{Fields: Fields{nil}},
		},
	}, {
		description: "dialect does not support RETURNING",
		item: InsertQuery{
			Dialect:         DialectMySQL,
			InsertTable:     Expr("tbl"),
			ColumnMapper:    colmapper,
			ReturningFields: Fields{f1, f2},
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
		description: "ColumnMapper err",
		item: InsertQuery{
			InsertTable:  Expr("tbl"),
			ColumnMapper: func(*Column) { panic(ErrFaultySQL) },
		},
	}, {
		description: "CTEs err",
		item: InsertQuery{
			CTEs:         []CTE{NewCTE("cte", nil, FaultySQL{})},
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
		},
	}, {
		description: "IntoTable err",
		item: InsertQuery{
			InsertTable:  FaultySQL{},
			ColumnMapper: colmapper,
		},
	}, {
		description: "RowValues err",
		item: InsertQuery{
			InsertTable:   Expr("tbl"),
			InsertColumns: Fields{f1, f2, f3},
			RowValues:     RowValues{{1, 2, FaultySQL{}}},
		},
	}, {
		description: "SelectQuery err",
		item: InsertQuery{
			InsertTable:   Expr("tbl"),
			InsertColumns: Fields{f1, f2, f3},
			SelectQuery:   Queryf("SELECT 1, 2, {}", FaultySQL{}),
		},
	}, {
		description: "ConflictPredicate VariadicPredicate err",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict: ConflictClause{
				Fields:    Fields{f1, f2},
				Predicate: And(FaultySQL{}),
			},
		},
	}, {
		description: "ConflictPredicate err",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict: ConflictClause{
				Fields:    Fields{f1, f2},
				Predicate: FaultySQL{},
			},
		},
	}, {
		description: "Resolution err",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict: ConflictClause{
				Fields:     Fields{f1, f2},
				Resolution: Assignments{FaultySQL{}},
			},
		},
	}, {
		description: "ResolutionPredicate VariadicPredicate err",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict: ConflictClause{
				Fields:              Fields{f1, f2},
				Resolution:          Assignments{FaultySQL{}},
				ResolutionPredicate: And(FaultySQL{}),
			},
		},
	}, {
		description: "ResolutionPredicate err",
		item: InsertQuery{
			Dialect:      DialectPostgres,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict: ConflictClause{
				Fields:              Fields{f1, f2},
				Resolution:          Assignments{Set(f1, f1)},
				ResolutionPredicate: FaultySQL{},
			},
		},
	}, {
		description: "mysql Resolution err",
		item: InsertQuery{
			Dialect:      DialectMySQL,
			InsertTable:  Expr("tbl"),
			ColumnMapper: colmapper,
			Conflict: ConflictClause{
				Resolution: Assignments{Set(f1, FaultySQL{})},
			},
		},
	}, {
		description: "ReturningFields err",
		item: InsertQuery{
			Dialect:         DialectPostgres,
			InsertTable:     Expr("tbl"),
			ColumnMapper:    colmapper,
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
