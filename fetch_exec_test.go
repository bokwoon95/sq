package sq

import (
	"database/sql"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
	_ "github.com/mattn/go-sqlite3"
)

var ACTOR = New[struct {
	TableStruct `sq:"actor"`
	ACTOR_ID    NumberField
	FIRST_NAME  StringField
	LAST_NAME   StringField
	LAST_UPDATE TimeField
}]("")

type Actor struct {
	ActorID    int
	FirstName  string
	LastName   string
	LastUpdate time.Time
}

func actorRowMapper(row *Row) Actor {
	var actor Actor
	actorID, _ := row.Value("actor.actor_id").(int64)
	actor.ActorID = int(actorID)
	actor.FirstName = row.StringField(ACTOR.FIRST_NAME)
	actor.LastName = row.StringField(ACTOR.LAST_NAME)
	actor.LastUpdate, _ = row.Value("actor.last_update").(time.Time)
	return actor
}

func actorRowMapperRawSQL(row *Row) Actor {
	result := make(map[string]any)
	values := row.Values()
	for i, column := range row.Columns() {
		result[column] = values[i]
	}
	var actor Actor
	actorID, _ := result["actor_id"].(int64)
	actor.ActorID = int(actorID)
	actor.FirstName, _ = result["first_name"].(string)
	actor.LastName, _ = result["last_name"].(string)
	actor.LastUpdate, _ = result["last_update"].(time.Time)
	return actor
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

func Test_getFieldMappings(t *testing.T) {
	type TestTable struct {
		description       string
		dialect           string
		fields            []Field
		scanDest          []any
		wantFieldMappings string
	}

	var tests = []TestTable{{
		description:       "empty",
		wantFieldMappings: "",
	}, {
		description: "basic",
		fields: []Field{
			Expr("actor_id"),
			Expr("first_name || {} || last_name", " "),
			Expr("last_update"),
		},
		scanDest: []any{
			&sql.NullInt64{},
			&sql.NullString{},
			&sql.NullTime{},
		},
		wantFieldMappings: "" +
			"\n 01. actor_id => *sql.NullInt64" +
			"\n 02. first_name || ' ' || last_name => *sql.NullString" +
			"\n 03. last_update => *sql.NullTime",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			gotFieldMappings := getFieldMappings(tt.dialect, tt.fields, tt.scanDest)
			if diff := testutil.Diff(gotFieldMappings, tt.wantFieldMappings); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}

func TestFetchExec(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	var referenceActors = []Actor{
		{ActorID: 1, FirstName: "PENELOPE", LastName: "GUINESS", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 2, FirstName: "NICK", LastName: "WAHLBERG", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 3, FirstName: "ED", LastName: "CHASE", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 4, FirstName: "JENNIFER", LastName: "DAVIS", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 5, FirstName: "JOHNNY", LastName: "LOLLOBRIGIDA", LastUpdate: time.Unix(1, 0).UTC()},
	}

	// Exec.
	res, err := Exec(Log(db), SQLite.
		InsertInto(ACTOR).
		ColumnValues(func(col *Column) {
			for _, actor := range referenceActors {
				col.SetInt(ACTOR.ACTOR_ID, actor.ActorID)
				col.SetString(ACTOR.FIRST_NAME, actor.FirstName)
				col.SetString(ACTOR.LAST_NAME, actor.LastName)
				col.SetTime(ACTOR.LAST_UPDATE, actor.LastUpdate)
			}
		}),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(res.RowsAffected, int64(len(referenceActors))); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// FetchOne.
	actor, err := FetchOne(Log(db), SQLite.
		From(ACTOR).
		Where(ACTOR.ACTOR_ID.EqInt(1)),
		actorRowMapper,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, referenceActors[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// FetchOne (Raw SQL).
	actor, err = FetchOne(Log(db),
		SQLite.Queryf("SELECT * FROM actor WHERE actor_id = {}", 1),
		actorRowMapperRawSQL,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, referenceActors[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// FetchAll.
	actors, err := FetchAll(VerboseLog(db), SQLite.
		From(ACTOR).
		OrderBy(ACTOR.ACTOR_ID),
		actorRowMapper,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, referenceActors); diff != "" {
		t.Fatal(testutil.Callers(), err)
	}

	// FetchAll (RawSQL).
	actors, err = FetchAll(VerboseLog(db),
		SQLite.Queryf("SELECT * FROM actor ORDER BY actor_id"),
		actorRowMapperRawSQL,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, referenceActors); diff != "" {
		t.Fatal(testutil.Callers(), err)
	}
}

func TestCompiledFetchExec(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	var referenceActors = []Actor{
		{ActorID: 1, FirstName: "PENELOPE", LastName: "GUINESS", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 2, FirstName: "NICK", LastName: "WAHLBERG", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 3, FirstName: "ED", LastName: "CHASE", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 4, FirstName: "JENNIFER", LastName: "DAVIS", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 5, FirstName: "JOHNNY", LastName: "LOLLOBRIGIDA", LastUpdate: time.Unix(1, 0).UTC()},
	}

	// CompiledExec.
	compiledExec, err := CompileExec(SQLite.
		InsertInto(ACTOR).
		ColumnValues(func(col *Column) {
			col.Set(ACTOR.ACTOR_ID, IntParam("actor_id", 0))
			col.Set(ACTOR.FIRST_NAME, StringParam("first_name", ""))
			col.Set(ACTOR.LAST_NAME, StringParam("last_name", ""))
			col.Set(ACTOR.LAST_UPDATE, TimeParam("last_update", time.Time{}))
		}),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	for _, actor := range referenceActors {
		_, err = compiledExec.Exec(Log(db), Params{
			"actor_id":    actor.ActorID,
			"first_name":  actor.FirstName,
			"last_name":   actor.LastName,
			"last_update": actor.LastUpdate,
		})
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
	}

	// CompiledFetch FetchOne.
	compiledFetch, err := CompileFetch(SQLite.
		From(ACTOR).
		Where(ACTOR.ACTOR_ID.Eq(IntParam("actor_id", 0))),
		actorRowMapper,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actor, err := compiledFetch.FetchOne(Log(db), Params{"actor_id": 1})
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, referenceActors[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// CompiledFetch FetchOne (Raw SQL).
	compiledFetch, err = CompileFetch(
		SQLite.Queryf("SELECT * FROM actor WHERE actor_id = {actor_id}", IntParam("actor_id", 0)),
		actorRowMapperRawSQL,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actor, err = compiledFetch.FetchOne(Log(db), Params{"actor_id": 1})
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, referenceActors[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// CompiledFetch FetchAll.
	compiledFetch, err = CompileFetch(SQLite.
		From(ACTOR).
		OrderBy(ACTOR.ACTOR_ID),
		actorRowMapper,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actors, err := compiledFetch.FetchAll(VerboseLog(db), nil)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, referenceActors); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// CompiledFetch FetchAll (Raw SQL).
	compiledFetch, err = CompileFetch(
		SQLite.Queryf("SELECT * FROM actor ORDER BY actor_id"),
		actorRowMapperRawSQL,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actors, err = compiledFetch.FetchAll(VerboseLog(db), nil)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, referenceActors); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}
}

func TestPreparedFetchExec(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	var referenceActors = []Actor{
		{ActorID: 1, FirstName: "PENELOPE", LastName: "GUINESS", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 2, FirstName: "NICK", LastName: "WAHLBERG", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 3, FirstName: "ED", LastName: "CHASE", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 4, FirstName: "JENNIFER", LastName: "DAVIS", LastUpdate: time.Unix(1, 0).UTC()},
		{ActorID: 5, FirstName: "JOHNNY", LastName: "LOLLOBRIGIDA", LastUpdate: time.Unix(1, 0).UTC()},
	}

	// PreparedExec.
	preparedExec, err := PrepareExec(Log(db), SQLite.
		InsertInto(ACTOR).
		ColumnValues(func(col *Column) {
			col.Set(ACTOR.ACTOR_ID, IntParam("actor_id", 0))
			col.Set(ACTOR.FIRST_NAME, StringParam("first_name", ""))
			col.Set(ACTOR.LAST_NAME, StringParam("last_name", ""))
			col.Set(ACTOR.LAST_UPDATE, TimeParam("last_update", time.Time{}))
		}),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	for _, actor := range referenceActors {
		_, err = preparedExec.Exec(Params{
			"actor_id":    actor.ActorID,
			"first_name":  actor.FirstName,
			"last_name":   actor.LastName,
			"last_update": actor.LastUpdate,
		})
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
	}

	// PreparedFetch FetchOne.
	preparedFetch, err := PrepareFetch(Log(db), SQLite.
		From(ACTOR).
		Where(ACTOR.ACTOR_ID.Eq(IntParam("actor_id", 0))),
		actorRowMapper,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actor, err := preparedFetch.FetchOne(Params{"actor_id": 1})
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, referenceActors[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// PreparedFetch FetchOne (Raw SQL).
	preparedFetch, err = PrepareFetch(
		Log(db),
		SQLite.Queryf("SELECT * FROM actor WHERE actor_id = {actor_id}", IntParam("actor_id", 0)),
		actorRowMapperRawSQL,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actor, err = preparedFetch.FetchOne(Params{"actor_id": 1})
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, referenceActors[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// PreparedFetch FetchAll.
	preparedFetch, err = PrepareFetch(VerboseLog(db), SQLite.
		From(ACTOR).
		OrderBy(ACTOR.ACTOR_ID),
		actorRowMapper,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actors, err := preparedFetch.FetchAll(nil)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, referenceActors); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// PreparedFetch FetchAll (Raw SQL).
	preparedFetch, err = PrepareFetch(
		VerboseLog(db),
		SQLite.Queryf("SELECT * FROM actor ORDER BY actor_id"),
		actorRowMapperRawSQL,
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actors, err = preparedFetch.FetchAll(nil)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, referenceActors); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}
}

func newDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	_, err = db.Exec(`CREATE TABLE actor (
    actor_id INTEGER PRIMARY KEY AUTOINCREMENT
    ,first_name TEXT NOT NULL
    ,last_name TEXT NOT NULL
    ,last_update DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	return db
}
