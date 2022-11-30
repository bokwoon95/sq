package sq

import (
	"database/sql"
	"io"
	"log"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
	_ "github.com/mattn/go-sqlite3"
)

type ACTOR struct {
	TableStruct
	ACTOR_ID    NumberField
	FIRST_NAME  StringField
	LAST_NAME   StringField
	LAST_UPDATE TimeField
}

type Actor struct {
	ActorID    int
	FirstName  string
	LastName   string
	LastUpdate time.Time
}

func (actor Actor) RowMapper(a ACTOR) func(*Row) Actor {
	return func(row *Row) Actor {
		actor.ActorID = row.IntField(a.ACTOR_ID)
		actor.FirstName = row.StringField(a.FIRST_NAME)
		actor.LastName = row.StringField(a.LAST_NAME)
		actor.LastUpdate = row.TimeField(a.LAST_UPDATE)
		return actor
	}
}

var __timestamp__ = time.Unix(1, 0).UTC()

var __actors__ = []Actor{
	{ActorID: 1, FirstName: "PENELOPE", LastName: "GUINESS", LastUpdate: __timestamp__},
	{ActorID: 2, FirstName: "NICK", LastName: "WAHLBERG", LastUpdate: __timestamp__},
	{ActorID: 3, FirstName: "ED", LastName: "CHASE", LastUpdate: __timestamp__},
	{ActorID: 4, FirstName: "JENNIFER", LastName: "DAVIS", LastUpdate: __timestamp__},
	{ActorID: 5, FirstName: "JOHNNY", LastName: "LOLLOBRIGIDA", LastUpdate: __timestamp__},
}

func init() {
	// For tests, silence the logger (the output is too noisy).
	defaultLogger = NewLogger(io.Discard, "", log.LstdFlags, LoggerConfig{
		ShowTimeTaken: true,
		ShowCaller:    true,
		HideArgs:      true,
	})
	verboseLogger = NewLogger(io.Discard, "", log.LstdFlags, LoggerConfig{
		ShowTimeTaken:      true,
		ShowCaller:         true,
		ShowResults:        5,
		InterpolateVerbose: true,
	})
}

func TestFetchExec(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	a := New[ACTOR]("a")

	// Exec
	res, err := Exec(Log(db), SQLite.
		InsertInto(a).
		ColumnValues(func(col *Column) {
			for _, actor := range __actors__ {
				col.SetInt(a.ACTOR_ID, actor.ActorID)
				col.SetString(a.FIRST_NAME, actor.FirstName)
				col.SetString(a.LAST_NAME, actor.LastName)
				col.SetTime(a.LAST_UPDATE, actor.LastUpdate)
			}
		}),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(res.RowsAffected, int64(len(__actors__))); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// FetchOne
	actor, err := FetchOne(Log(db), SQLite.
		From(a).
		Where(a.ACTOR_ID.EqInt(1)),
		Actor{}.RowMapper(a),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, __actors__[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// FetchAll
	actors, err := FetchAll(VerboseLog(db), SQLite.
		From(a).
		OrderBy(a.ACTOR_ID),
		Actor{}.RowMapper(a),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, __actors__); diff != "" {
		t.Fatal(testutil.Callers(), err)
	}
}

func TestCompiledFetchExec(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	a := New[ACTOR]("a")

	// CompiledExec
	insertActor, err := CompileExec(SQLite.
		InsertInto(a).
		ColumnValues(func(col *Column) {
			col.Set(a.ACTOR_ID, IntParam("actor_id", 0))
			col.Set(a.FIRST_NAME, StringParam("first_name", ""))
			col.Set(a.LAST_NAME, StringParam("last_name", ""))
			col.Set(a.LAST_UPDATE, TimeParam("last_update", time.Time{}))
		}),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	for _, actor := range __actors__ {
		_, err = insertActor.Exec(Log(db), Params{
			"actor_id":    actor.ActorID,
			"first_name":  actor.FirstName,
			"last_name":   actor.LastName,
			"last_update": actor.LastUpdate,
		})
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
	}

	// CompiledFetch One
	fetchActor, err := CompileFetch(SQLite.
		From(a).
		Where(a.ACTOR_ID.Eq(IntParam("actor_id", 0))),
		Actor{}.RowMapper(a),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actor, err := fetchActor.FetchOne(Log(db), Params{"actor_id": 1})
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, __actors__[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// CompiledFetch All
	fetchActors, err := CompileFetch(SQLite.
		From(a).
		OrderBy(a.ACTOR_ID),
		Actor{}.RowMapper(a),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actors, err := fetchActors.FetchAll(VerboseLog(db), nil)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, __actors__); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}
}

func TestPreparedFetchExec(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	a := New[ACTOR]("a")

	// PreparedExec
	insertActor, err := PrepareExec(Log(db), SQLite.
		InsertInto(a).
		ColumnValues(func(col *Column) {
			col.Set(a.ACTOR_ID, IntParam("actor_id", 0))
			col.Set(a.FIRST_NAME, StringParam("first_name", ""))
			col.Set(a.LAST_NAME, StringParam("last_name", ""))
			col.Set(a.LAST_UPDATE, TimeParam("last_update", time.Time{}))
		}),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	for _, actor := range __actors__ {
		_, err = insertActor.Exec(Params{
			"actor_id":    actor.ActorID,
			"first_name":  actor.FirstName,
			"last_name":   actor.LastName,
			"last_update": actor.LastUpdate,
		})
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
	}

	// PreparedFetch One
	fetchActor, err := PrepareFetch(Log(db), SQLite.
		From(a).
		Where(a.ACTOR_ID.Eq(IntParam("actor_id", 0))),
		Actor{}.RowMapper(a),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actor, err := fetchActor.FetchOne(Params{"actor_id": 1})
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actor, __actors__[0]); diff != "" {
		t.Fatal(testutil.Callers(), diff)
	}

	// PreparedFetch All
	fetchActors, err := PrepareFetch(VerboseLog(db), SQLite.
		From(a).
		OrderBy(a.ACTOR_ID),
		Actor{}.RowMapper(a),
	)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	actors, err := fetchActors.FetchAll(nil)
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	if diff := testutil.Diff(actors, __actors__); diff != "" {
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
