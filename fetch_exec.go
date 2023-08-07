package sq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	errMixedCalls       = fmt.Errorf("rowmapper cannot mix calls to row.Values()/row.Columns()/row.ColumnTypes() with the other row methods")
	errNoFieldsAccessed = fmt.Errorf("rowmapper did not access any fields, unable to determine fields to insert into query")
	errForbiddenCalls   = fmt.Errorf("rowmapper can only contain calls to row.Values()/row.Columns()/row.ColumnTypes() because query's SELECT clause is not dynamic")
)

// Default dialect used by all queries (if no dialect is explicitly provided).
var DefaultDialect atomic.Pointer[string]

// A Cursor represents a database cursor.
type Cursor[T any] struct {
	ctx           context.Context
	row           *Row
	rowmapper     func(*Row) T
	queryStats    QueryStats
	logSettings   LogSettings
	logger        SqLogger
	logged        int32
	fieldNames    []string
	resultsBuffer *bytes.Buffer
}

// FetchCursor returns a new cursor.
func FetchCursor[T any](db DB, query Query, rowmapper func(*Row) T) (*Cursor[T], error) {
	return fetchCursor(context.Background(), db, query, rowmapper, 1)
}

// FetchCursorContext is like FetchCursor but additionally requires a context.Context.
func FetchCursorContext[T any](ctx context.Context, db DB, query Query, rowmapper func(*Row) T) (*Cursor[T], error) {
	return fetchCursor(ctx, db, query, rowmapper, 1)
}

func fetchCursor[T any](ctx context.Context, db DB, query Query, rowmapper func(*Row) T, skip int) (cursor *Cursor[T], err error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if query == nil {
		return nil, fmt.Errorf("query is nil")
	}
	if rowmapper == nil {
		return nil, fmt.Errorf("rowmapper is nil")
	}
	dialect := query.GetDialect()
	if dialect == "" {
		defaultDialect := DefaultDialect.Load()
		if defaultDialect != nil {
			dialect = *defaultDialect
		}
	}
	cursor = &Cursor[T]{
		ctx:       ctx,
		rowmapper: rowmapper,
		row: &Row{
			dialect: dialect,
		},
		queryStats: QueryStats{
			Dialect:  dialect,
			RowCount: sql.NullInt64{Valid: true},
			Params:   make(map[string][]int),
		},
	}

	// Call the rowmapper to populate row.fields and row.scanDest.
	defer mapperFunctionPanicked(&err)
	_ = cursor.rowmapper(cursor.row)
	var ok bool
	if cursor.row.rawSQLMode && len(cursor.row.fields) > 0 {
		return nil, errMixedCalls
	}

	// Insert the fields into the query.
	query, ok = query.SetFetchableFields(cursor.row.fields)
	if ok && len(cursor.row.fields) == 0 {
		return nil, errNoFieldsAccessed
	}
	if !ok && len(cursor.row.fields) > 0 {
		return nil, errForbiddenCalls
	}

	// Build query.
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	err = query.WriteSQL(ctx, dialect, buf, &cursor.queryStats.Args, cursor.queryStats.Params)
	cursor.queryStats.Query = buf.String()
	if err != nil {
		return nil, err
	}

	// Setup logger.
	cursor.logger, _ = db.(SqLogger)
	if cursor.logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			cursor.logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	if cursor.logger != nil {
		cursor.logger.SqLogSettings(ctx, &cursor.logSettings)
		if cursor.logSettings.IncludeCaller {
			cursor.queryStats.CallerFile, cursor.queryStats.CallerLine, cursor.queryStats.CallerFunction = caller(skip + 1)
		}
	}

	// Run query.
	if cursor.logSettings.IncludeTime {
		cursor.queryStats.StartedAt = time.Now()
	}
	cursor.row.sqlRows, cursor.queryStats.Err = db.QueryContext(ctx, cursor.queryStats.Query, cursor.queryStats.Args...)
	if cursor.logSettings.IncludeTime {
		cursor.queryStats.TimeTaken = time.Since(cursor.queryStats.StartedAt)
	}
	if cursor.queryStats.Err != nil {
		cursor.log()
		return nil, cursor.queryStats.Err
	}

	// Allocate the resultsBuffer.
	if cursor.logSettings.IncludeResults > 0 {
		cursor.resultsBuffer = bufpool.Get().(*bytes.Buffer)
		cursor.resultsBuffer.Reset()
	}
	return cursor, nil
}

// Next advances the cursor to the next result.
func (cursor *Cursor[T]) Next() bool {
	hasNext := cursor.row.sqlRows.Next()
	if hasNext {
		cursor.queryStats.RowCount.Int64++
	} else {
		cursor.log()
	}
	return hasNext
}

// RowCount returns the current row number so far.
func (cursor *Cursor[T]) RowCount() int64 { return cursor.queryStats.RowCount.Int64 }

// Result returns the cursor result.
func (cursor *Cursor[T]) Result() (result T, err error) {
	if !cursor.row.rawSQLMode {
		err = cursor.row.sqlRows.Scan(cursor.row.scanDest...)
		if err != nil {
			cursor.log()
			fieldMappings := getFieldMappings(cursor.queryStats.Dialect, cursor.row.fields, cursor.row.scanDest)
			return result, fmt.Errorf("please check if your mapper function is correct:%s\n%w", fieldMappings, err)
		}
	}
	// If results should be logged, write the row into the resultsBuffer.
	if cursor.resultsBuffer != nil && cursor.queryStats.RowCount.Int64 <= int64(cursor.logSettings.IncludeResults) {
		if len(cursor.fieldNames) == 0 {
			cursor.fieldNames = getFieldNames(cursor.ctx, cursor.row)
		}
		cursor.resultsBuffer.WriteString("\n----[ Row " + strconv.FormatInt(cursor.queryStats.RowCount.Int64, 10) + " ]----")
		for i := range cursor.row.scanDest {
			cursor.resultsBuffer.WriteString("\n")
			if i < len(cursor.fieldNames) {
				cursor.resultsBuffer.WriteString(cursor.fieldNames[i])
			}
			cursor.resultsBuffer.WriteString(": ")
			scanDest := cursor.row.scanDest[i]
			rhs, err := Sprint(cursor.queryStats.Dialect, scanDest)
			if err != nil {
				cursor.resultsBuffer.WriteString("%!(error=" + err.Error() + ")")
				continue
			}
			cursor.resultsBuffer.WriteString(rhs)
		}
	}
	cursor.row.index = 0
	defer mapperFunctionPanicked(&err)
	result = cursor.rowmapper(cursor.row)
	return result, nil
}

func (cursor *Cursor[T]) log() {
	if !atomic.CompareAndSwapInt32(&cursor.logged, 0, 1) {
		return
	}
	if cursor.resultsBuffer != nil {
		cursor.queryStats.Results = cursor.resultsBuffer.String()
		bufpool.Put(cursor.resultsBuffer)
	}
	if cursor.logger == nil {
		return
	}
	if cursor.logSettings.LogAsynchronously {
		go cursor.logger.SqLogQuery(cursor.ctx, cursor.queryStats)
	} else {
		cursor.logger.SqLogQuery(cursor.ctx, cursor.queryStats)
	}
}

// Close closes the cursor.
func (cursor *Cursor[T]) Close() error {
	cursor.log()
	if err := cursor.row.sqlRows.Close(); err != nil {
		return err
	}
	if err := cursor.row.sqlRows.Err(); err != nil {
		return err
	}
	return nil
}

// FetchOne returns the first result from running the given Query on the given
// DB.
func FetchOne[T any](db DB, query Query, rowmapper func(*Row) T) (T, error) {
	cursor, err := fetchCursor(context.Background(), db, query, rowmapper, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchOneContext is like FetchOne but additionally requires a context.Context.
func FetchOneContext[T any](ctx context.Context, db DB, query Query, rowmapper func(*Row) T) (T, error) {
	cursor, err := fetchCursor(ctx, db, query, rowmapper, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchAll returns all results from running the given Query on the given DB.
func FetchAll[T any](db DB, query Query, rowmapper func(*Row) T) ([]T, error) {
	cursor, err := fetchCursor(context.Background(), db, query, rowmapper, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// FetchAllContext is like FetchAll but additionally requires a context.Context.
func FetchAllContext[T any](ctx context.Context, db DB, query Query, rowmapper func(*Row) T) ([]T, error) {
	cursor, err := fetchCursor(ctx, db, query, rowmapper, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// CompiledFetch is the result of compiling a Query down into a query string
// and args slice. A CompiledFetch can be safely executed in parallel.
type CompiledFetch[T any] struct {
	dialect   string
	query     string
	args      []any
	params    map[string][]int
	rowmapper func(*Row) T
}

// NewCompiledFetch returns a new CompiledFetch.
func NewCompiledFetch[T any](dialect string, query string, args []any, params map[string][]int, rowmapper func(*Row) T) *CompiledFetch[T] {
	return &CompiledFetch[T]{
		dialect:   dialect,
		query:     query,
		args:      args,
		params:    params,
		rowmapper: rowmapper,
	}
}

// CompileFetch returns a new CompileFetch.
func CompileFetch[T any](q Query, rowmapper func(*Row) T) (*CompiledFetch[T], error) {
	return CompileFetchContext(context.Background(), q, rowmapper)
}

// CompileFetchContext is like CompileFetch but accepts a context.Context.
func CompileFetchContext[T any](ctx context.Context, query Query, rowmapper func(*Row) T) (compiledFetch *CompiledFetch[T], err error) {
	if query == nil {
		return nil, fmt.Errorf("query is nil")
	}
	if rowmapper == nil {
		return nil, fmt.Errorf("rowmapper is nil")
	}
	dialect := query.GetDialect()
	if dialect == "" {
		defaultDialect := DefaultDialect.Load()
		if defaultDialect != nil {
			dialect = *defaultDialect
		}
	}
	compiledFetch = &CompiledFetch[T]{
		dialect:   dialect,
		params:    make(map[string][]int),
		rowmapper: rowmapper,
	}
	row := &Row{
		dialect: dialect,
	}

	// Call the rowmapper to populate row.fields.
	defer mapperFunctionPanicked(&err)
	_ = rowmapper(row)
	var ok bool
	if row.rawSQLMode && len(row.fields) > 0 {
		return nil, errMixedCalls
	}

	// Insert the fields into the query.
	query, ok = query.SetFetchableFields(row.fields)
	if ok && len(row.fields) == 0 {
		return nil, errNoFieldsAccessed
	}
	if !ok && len(row.fields) > 0 {
		return nil, errForbiddenCalls
	}

	// Build query.
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	err = query.WriteSQL(ctx, dialect, buf, &compiledFetch.args, compiledFetch.params)
	compiledFetch.query = buf.String()
	if err != nil {
		return nil, err
	}
	return compiledFetch, nil
}

// FetchCursor returns a new cursor.
func (compiledFetch *CompiledFetch[T]) FetchCursor(db DB, params Params) (*Cursor[T], error) {
	return compiledFetch.fetchCursor(context.Background(), db, params, 1)
}

// FetchCursorContext is like FetchCursor but additionally requires a context.Context.
func (compiledFetch *CompiledFetch[T]) FetchCursorContext(ctx context.Context, db DB, params Params) (*Cursor[T], error) {
	return compiledFetch.fetchCursor(ctx, db, params, 1)
}

func (compiledFetch *CompiledFetch[T]) fetchCursor(ctx context.Context, db DB, params Params, skip int) (cursor *Cursor[T], err error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	cursor = &Cursor[T]{
		ctx:       ctx,
		rowmapper: compiledFetch.rowmapper,
		row: &Row{
			dialect: compiledFetch.dialect,
		},
		queryStats: QueryStats{
			Dialect: compiledFetch.dialect,
			Query:   compiledFetch.query,
			Args:    compiledFetch.args,
			Params:  compiledFetch.params,
		},
	}

	// Call the rowmapper to populate row.scanDest.
	defer mapperFunctionPanicked(&err)
	_ = cursor.rowmapper(cursor.row)
	if err != nil {
		return nil, err
	}

	// Substitute params.
	cursor.queryStats.Args, err = substituteParams(cursor.queryStats.Dialect, cursor.queryStats.Args, cursor.queryStats.Params, params)
	if err != nil {
		return nil, err
	}

	// Setup logger.
	cursor.queryStats.RowCount.Valid = true
	cursor.logger, _ = db.(SqLogger)
	if cursor.logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			cursor.logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	if cursor.logger != nil {
		cursor.logger.SqLogSettings(ctx, &cursor.logSettings)
		if cursor.logSettings.IncludeCaller {
			cursor.queryStats.CallerFile, cursor.queryStats.CallerLine, cursor.queryStats.CallerFunction = caller(skip + 1)
		}
	}

	// Run query.
	if cursor.logSettings.IncludeTime {
		cursor.queryStats.StartedAt = time.Now()
	}
	cursor.row.sqlRows, cursor.queryStats.Err = db.QueryContext(ctx, cursor.queryStats.Query, cursor.queryStats.Args...)
	if cursor.logSettings.IncludeTime {
		cursor.queryStats.TimeTaken = time.Since(cursor.queryStats.StartedAt)
	}
	if cursor.queryStats.Err != nil {
		return nil, cursor.queryStats.Err
	}

	// Allocate the resultsBuffer.
	if cursor.logSettings.IncludeResults > 0 {
		cursor.resultsBuffer = bufpool.Get().(*bytes.Buffer)
		cursor.resultsBuffer.Reset()
	}
	return cursor, nil
}

// FetchOne returns the first result from running the CompiledFetch on the
// given DB with the give params.
func (compiledFetch *CompiledFetch[T]) FetchOne(db DB, params Params) (T, error) {
	cursor, err := compiledFetch.fetchCursor(context.Background(), db, params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchOneContext is like FetchOne but additionally requires a context.Context.
func (compiledFetch *CompiledFetch[T]) FetchOneContext(ctx context.Context, db DB, params Params) (T, error) {
	cursor, err := compiledFetch.fetchCursor(ctx, db, params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchAll returns all the results from running the CompiledFetch on the given
// DB with the give params.
func (compiledFetch *CompiledFetch[T]) FetchAll(db DB, params Params) ([]T, error) {
	cursor, err := compiledFetch.fetchCursor(context.Background(), db, params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// FetchAllContext is like FetchAll but additionally requires a context.Context.
func (compiledFetch *CompiledFetch[T]) FetchAllContext(ctx context.Context, db DB, params Params) ([]T, error) {
	cursor, err := compiledFetch.fetchCursor(ctx, db, params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// GetSQL returns a copy of the dialect, query, args, params and rowmapper that
// make up the CompiledFetch.
func (compiledFetch *CompiledFetch[T]) GetSQL() (dialect string, query string, args []any, params map[string][]int, rowmapper func(*Row) T) {
	dialect = compiledFetch.dialect
	query = compiledFetch.query
	args = make([]any, len(compiledFetch.args))
	params = make(map[string][]int)
	copy(args, compiledFetch.args)
	for name, indexes := range compiledFetch.params {
		indexes2 := make([]int, len(indexes))
		copy(indexes2, indexes)
		params[name] = indexes2
	}
	return dialect, query, args, params, compiledFetch.rowmapper
}

// Prepare creates a PreparedFetch from a CompiledFetch by preparing it on
// the given DB.
func (compiledFetch *CompiledFetch[T]) Prepare(db DB) (*PreparedFetch[T], error) {
	return compiledFetch.PrepareContext(context.Background(), db)
}

// PrepareContext is like Prepare but additionally requires a context.Context.
func (compiledFetch *CompiledFetch[T]) PrepareContext(ctx context.Context, db DB) (*PreparedFetch[T], error) {
	var err error
	preparedFetch := &PreparedFetch[T]{
		compiledFetch: NewCompiledFetch(compiledFetch.GetSQL()),
	}
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	preparedFetch.stmt, err = db.PrepareContext(ctx, compiledFetch.query)
	if err != nil {
		return nil, err
	}
	preparedFetch.logger, _ = db.(SqLogger)
	if preparedFetch.logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			preparedFetch.logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	return preparedFetch, nil
}

// PreparedFetch is the result of preparing a CompiledFetch on a DB.
type PreparedFetch[T any] struct {
	compiledFetch *CompiledFetch[T]
	stmt          *sql.Stmt
	logger        SqLogger
}

// PrepareFetch returns a new PreparedFetch.
func PrepareFetch[T any](db DB, q Query, rowmapper func(*Row) T) (*PreparedFetch[T], error) {
	return PrepareFetchContext(context.Background(), db, q, rowmapper)
}

// PrepareFetchContext is like PrepareFetch but additionally requires a context.Context.
func PrepareFetchContext[T any](ctx context.Context, db DB, q Query, rowmapper func(*Row) T) (*PreparedFetch[T], error) {
	compiledFetch, err := CompileFetchContext(ctx, q, rowmapper)
	if err != nil {
		return nil, err
	}
	return compiledFetch.PrepareContext(ctx, db)
}

// FetchCursor returns a new cursor.
func (preparedFetch PreparedFetch[T]) FetchCursor(params Params) (*Cursor[T], error) {
	return preparedFetch.fetchCursor(context.Background(), params, 1)
}

// FetchCursorContext is like FetchCursor but additionally requires a context.Context.
func (preparedFetch PreparedFetch[T]) FetchCursorContext(ctx context.Context, params Params) (*Cursor[T], error) {
	return preparedFetch.fetchCursor(ctx, params, 1)
}

func (preparedFetch *PreparedFetch[T]) fetchCursor(ctx context.Context, params Params, skip int) (cursor *Cursor[T], err error) {
	cursor = &Cursor[T]{
		ctx:       ctx,
		rowmapper: preparedFetch.compiledFetch.rowmapper,
		row: &Row{
			dialect: preparedFetch.compiledFetch.dialect,
		},
		queryStats: QueryStats{
			Dialect:  preparedFetch.compiledFetch.dialect,
			Query:    preparedFetch.compiledFetch.query,
			Args:     preparedFetch.compiledFetch.args,
			Params:   preparedFetch.compiledFetch.params,
			RowCount: sql.NullInt64{Valid: true},
		},
		logger: preparedFetch.logger,
	}

	// Call the rowmapper to populate row.scanDest.
	defer mapperFunctionPanicked(&err)
	_ = cursor.rowmapper(cursor.row)
	if err != nil {
		return nil, err
	}

	// Substitute params.
	cursor.queryStats.Args, err = substituteParams(cursor.queryStats.Dialect, cursor.queryStats.Args, cursor.queryStats.Params, params)
	if err != nil {
		return nil, err
	}

	// Setup logger.
	if cursor.logger != nil {
		cursor.logger.SqLogSettings(ctx, &cursor.logSettings)
		if cursor.logSettings.IncludeCaller {
			cursor.queryStats.CallerFile, cursor.queryStats.CallerLine, cursor.queryStats.CallerFunction = caller(skip + 1)
		}
	}

	// Run query.
	if cursor.logSettings.IncludeTime {
		cursor.queryStats.StartedAt = time.Now()
	}
	cursor.row.sqlRows, cursor.queryStats.Err = preparedFetch.stmt.QueryContext(ctx, cursor.queryStats.Args...)
	if cursor.logSettings.IncludeTime {
		cursor.queryStats.TimeTaken = time.Since(cursor.queryStats.StartedAt)
	}
	if cursor.queryStats.Err != nil {
		return nil, cursor.queryStats.Err
	}

	// Allocate the resultsBuffer.
	if cursor.logSettings.IncludeResults > 0 {
		cursor.resultsBuffer = bufpool.Get().(*bytes.Buffer)
		cursor.resultsBuffer.Reset()
	}
	return cursor, nil
}

// FetchOne returns the first result from running the PreparedFetch with the
// give params.
func (preparedFetch *PreparedFetch[T]) FetchOne(params Params) (T, error) {
	cursor, err := preparedFetch.fetchCursor(context.Background(), params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchOneContext is like FetchOne but additionally requires a context.Context.
func (preparedFetch *PreparedFetch[T]) FetchOneContext(ctx context.Context, params Params) (T, error) {
	cursor, err := preparedFetch.fetchCursor(ctx, params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchAll returns all the results from running the PreparedFetch with the
// give params.
func (preparedFetch *PreparedFetch[T]) FetchAll(params Params) ([]T, error) {
	cursor, err := preparedFetch.fetchCursor(context.Background(), params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// FetchAllContext is like FetchAll but additionally requires a context.Context.
func (preparedFetch *PreparedFetch[T]) FetchAllContext(ctx context.Context, params Params) ([]T, error) {
	cursor, err := preparedFetch.fetchCursor(ctx, params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// GetCompiled returns a copy of the underlying CompiledFetch.
func (preparedFetch *PreparedFetch[T]) GetCompiled() *CompiledFetch[T] {
	return NewCompiledFetch(preparedFetch.compiledFetch.GetSQL())
}

// Close closes the PreparedFetch.
func (preparedFetch *PreparedFetch[T]) Close() error {
	if preparedFetch.stmt == nil {
		return nil
	}
	return preparedFetch.stmt.Close()
}

// Exec executes the given Query on the given DB.
func Exec(db DB, query Query) (Result, error) {
	return exec(context.Background(), db, query, 1)
}

// ExecContext is like Exec but additionally requires a context.Context.
func ExecContext(ctx context.Context, db DB, query Query) (Result, error) {
	return exec(ctx, db, query, 1)
}

func exec(ctx context.Context, db DB, query Query, skip int) (result Result, err error) {
	if db == nil {
		return result, fmt.Errorf("db is nil")
	}
	if query == nil {
		return result, fmt.Errorf("query is nil")
	}
	dialect := query.GetDialect()
	if dialect == "" {
		defaultDialect := DefaultDialect.Load()
		if defaultDialect != nil {
			dialect = *defaultDialect
		}
	}
	queryStats := QueryStats{
		Dialect: dialect,
		Params:  make(map[string][]int),
	}

	// Build query.
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	err = query.WriteSQL(ctx, dialect, buf, &queryStats.Args, queryStats.Params)
	queryStats.Query = buf.String()
	if err != nil {
		return result, err
	}

	// Setup logger.
	var logSettings LogSettings
	logger, _ := db.(SqLogger)
	if logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	if logger != nil {
		logger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			queryStats.CallerFile, queryStats.CallerLine, queryStats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go logger.SqLogQuery(ctx, queryStats)
			} else {
				logger.SqLogQuery(ctx, queryStats)
			}
		}()
	}

	// Run query.
	if logSettings.IncludeTime {
		queryStats.StartedAt = time.Now()
	}
	var sqlResult sql.Result
	sqlResult, queryStats.Err = db.ExecContext(ctx, queryStats.Query, queryStats.Args...)
	if logSettings.IncludeTime {
		queryStats.TimeTaken = time.Since(queryStats.StartedAt)
	}
	if queryStats.Err != nil {
		return result, queryStats.Err
	}
	return execResult(sqlResult, &queryStats)
}

// CompiledExec is the result of compiling a Query down into a query string and
// args slice. A CompiledExec can be safely executed in parallel.
type CompiledExec struct {
	dialect string
	query   string
	args    []any
	params  map[string][]int
}

// NewCompiledExec returns a new CompiledExec.
func NewCompiledExec(dialect string, query string, args []any, params map[string][]int) *CompiledExec {
	return &CompiledExec{
		dialect: dialect,
		query:   query,
		args:    args,
		params:  params,
	}
}

// CompileExec returns a new CompiledExec.
func CompileExec(query Query) (*CompiledExec, error) {
	return CompileExecContext(context.Background(), query)
}

// CompileExecContext is like CompileExec but additionally requires a context.Context.
func CompileExecContext(ctx context.Context, query Query) (*CompiledExec, error) {
	if query == nil {
		return nil, fmt.Errorf("query is nil")
	}
	dialect := query.GetDialect()
	if dialect == "" {
		defaultDialect := DefaultDialect.Load()
		if defaultDialect != nil {
			dialect = *defaultDialect
		}
	}
	compiledExec := &CompiledExec{
		dialect: dialect,
		params:  make(map[string][]int),
	}

	// Build query.
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	err := query.WriteSQL(ctx, dialect, buf, &compiledExec.args, compiledExec.params)
	compiledExec.query = buf.String()
	if err != nil {
		return nil, err
	}
	return compiledExec, nil
}

// Exec executes the CompiledExec on the given DB with the given params.
func (compiledExec *CompiledExec) Exec(db DB, params Params) (Result, error) {
	return compiledExec.exec(context.Background(), db, params, 1)
}

// ExecContext is like Exec but additionally requires a context.Context.
func (compiledExec *CompiledExec) ExecContext(ctx context.Context, db DB, params Params) (Result, error) {
	return compiledExec.exec(ctx, db, params, 1)
}

func (compiledExec *CompiledExec) exec(ctx context.Context, db DB, params Params, skip int) (result Result, err error) {
	if db == nil {
		return result, fmt.Errorf("db is nil")
	}
	queryStats := QueryStats{
		Dialect: compiledExec.dialect,
		Query:   compiledExec.query,
		Args:    compiledExec.args,
		Params:  compiledExec.params,
	}

	// Setup logger.
	var logSettings LogSettings
	logger, _ := db.(SqLogger)
	if logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	if logger != nil {
		logger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			queryStats.CallerFile, queryStats.CallerLine, queryStats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go logger.SqLogQuery(ctx, queryStats)
			} else {
				logger.SqLogQuery(ctx, queryStats)
			}
		}()
	}

	// Substitute params.
	queryStats.Args, err = substituteParams(queryStats.Dialect, queryStats.Args, queryStats.Params, params)
	if err != nil {
		return result, err
	}

	// Run query.
	if logSettings.IncludeTime {
		queryStats.StartedAt = time.Now()
	}
	var sqlResult sql.Result
	sqlResult, queryStats.Err = db.ExecContext(ctx, queryStats.Query, queryStats.Args...)
	if logSettings.IncludeTime {
		queryStats.TimeTaken = time.Since(queryStats.StartedAt)
	}
	if queryStats.Err != nil {
		return result, queryStats.Err
	}
	return execResult(sqlResult, &queryStats)
}

// GetSQL returns a copy of the dialect, query, args, params and rowmapper that
// make up the CompiledExec.
func (compiledExec *CompiledExec) GetSQL() (dialect string, query string, args []any, params map[string][]int) {
	dialect = compiledExec.dialect
	query = compiledExec.query
	args = make([]any, len(compiledExec.args))
	params = make(map[string][]int)
	copy(args, compiledExec.args)
	for name, indexes := range compiledExec.params {
		indexes2 := make([]int, len(indexes))
		copy(indexes2, indexes)
		params[name] = indexes2
	}
	return dialect, query, args, params
}

// Prepare creates a PreparedExec from a CompiledExec by preparing it on the
// given DB.
func (compiledExec *CompiledExec) Prepare(db DB) (*PreparedExec, error) {
	return compiledExec.PrepareContext(context.Background(), db)
}

// PrepareContext is like Prepare but additionally requires a context.Context.
func (compiledExec *CompiledExec) PrepareContext(ctx context.Context, db DB) (*PreparedExec, error) {
	var err error
	preparedExec := &PreparedExec{
		compiledExec: NewCompiledExec(compiledExec.GetSQL()),
	}
	preparedExec.stmt, err = db.PrepareContext(ctx, compiledExec.query)
	if err != nil {
		return nil, err
	}
	preparedExec.logger, _ = db.(SqLogger)
	if preparedExec.logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			preparedExec.logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	return preparedExec, nil
}

// PrepareExec is the result of preparing a CompiledExec on a DB.
type PreparedExec struct {
	compiledExec *CompiledExec
	stmt         *sql.Stmt
	logger       SqLogger
}

// PrepareExec returns a new PreparedExec.
func PrepareExec(db DB, q Query) (*PreparedExec, error) {
	return PrepareExecContext(context.Background(), db, q)
}

// PrepareExecContext is like PrepareExec but additionally requires a
// context.Context.
func PrepareExecContext(ctx context.Context, db DB, q Query) (*PreparedExec, error) {
	compiledExec, err := CompileExecContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return compiledExec.PrepareContext(ctx, db)
}

// Close closes the PreparedExec.
func (preparedExec *PreparedExec) Close() error {
	if preparedExec.stmt == nil {
		return nil
	}
	return preparedExec.stmt.Close()
}

// Exec executes the PreparedExec with the given params.
func (preparedExec *PreparedExec) Exec(params Params) (Result, error) {
	return preparedExec.exec(context.Background(), params, 1)
}

// ExecContext is like Exec but additionally requires a context.Context.
func (preparedExec *PreparedExec) ExecContext(ctx context.Context, params Params) (Result, error) {
	return preparedExec.exec(ctx, params, 1)
}

func (preparedExec *PreparedExec) exec(ctx context.Context, params Params, skip int) (result Result, err error) {
	queryStats := QueryStats{
		Dialect: preparedExec.compiledExec.dialect,
		Query:   preparedExec.compiledExec.query,
		Args:    preparedExec.compiledExec.args,
		Params:  preparedExec.compiledExec.params,
	}

	// Setup logger.
	var logSettings LogSettings
	if preparedExec.logger != nil {
		preparedExec.logger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			queryStats.CallerFile, queryStats.CallerLine, queryStats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go preparedExec.logger.SqLogQuery(ctx, queryStats)
			} else {
				preparedExec.logger.SqLogQuery(ctx, queryStats)
			}
		}()
	}

	// Substitute params.
	queryStats.Args, err = substituteParams(queryStats.Dialect, queryStats.Args, queryStats.Params, params)
	if err != nil {
		return result, err
	}

	// Run query.
	if logSettings.IncludeTime {
		queryStats.StartedAt = time.Now()
	}
	var sqlResult sql.Result
	sqlResult, queryStats.Err = preparedExec.stmt.ExecContext(ctx, queryStats.Args...)
	if logSettings.IncludeTime {
		queryStats.TimeTaken = time.Since(queryStats.StartedAt)
	}
	if queryStats.Err != nil {
		return result, queryStats.Err
	}
	return execResult(sqlResult, &queryStats)
}

func getFieldNames(ctx context.Context, row *Row) []string {
	if len(row.fields) == 0 {
		columns, _ := row.sqlRows.Columns()
		return columns
	}
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	var args []any
	fieldNames := make([]string, 0, len(row.fields))
	for _, field := range row.fields {
		if alias := getAlias(field); alias != "" {
			fieldNames = append(fieldNames, alias)
			continue
		}
		buf.Reset()
		args = args[:0]
		err := field.WriteSQL(ctx, row.dialect, buf, &args, nil)
		if err != nil {
			fieldNames = append(fieldNames, "%!(error="+err.Error()+")")
			continue
		}
		fieldName, err := Sprintf(row.dialect, buf.String(), args)
		if err != nil {
			fieldNames = append(fieldNames, "%!(error="+err.Error()+")")
			continue
		}
		fieldNames = append(fieldNames, fieldName)
	}
	return fieldNames
}

func getFieldMappings(dialect string, fields []Field, scanDest []any) string {
	var buf bytes.Buffer
	var args []any
	var b strings.Builder
	for i, field := range fields {
		b.WriteString(fmt.Sprintf("\n %02d. ", i+1))
		buf.Reset()
		args = args[:0]
		err := field.WriteSQL(context.Background(), dialect, &buf, &args, nil)
		if err != nil {
			buf.WriteString("%!(error=" + err.Error() + ")")
			continue
		}
		fieldName, err := Sprintf(dialect, buf.String(), args)
		if err != nil {
			b.WriteString("%!(error=" + err.Error() + ")")
			continue
		}
		b.WriteString(fieldName + " => " + reflect.TypeOf(scanDest[i]).String())
	}
	return b.String()
}

func cursorResult[T any](cursor *Cursor[T]) (result T, err error) {
	for cursor.Next() {
		result, err = cursor.Result()
		if err != nil {
			return result, err
		}
		break
	}
	if cursor.RowCount() == 0 {
		return result, sql.ErrNoRows
	}
	return result, cursor.Close()
}

func cursorResults[T any](cursor *Cursor[T]) (results []T, err error) {
	var result T
	for cursor.Next() {
		result, err = cursor.Result()
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, cursor.Close()
}

func execResult(sqlResult sql.Result, queryStats *QueryStats) (Result, error) {
	var err error
	var result Result
	if queryStats.Dialect == DialectSQLite || queryStats.Dialect == DialectMySQL {
		result.LastInsertId, err = sqlResult.LastInsertId()
		if err != nil {
			return result, err
		}
		queryStats.LastInsertId.Valid = true
		queryStats.LastInsertId.Int64 = result.LastInsertId
	}
	result.RowsAffected, err = sqlResult.RowsAffected()
	if err != nil {
		return result, err
	}
	queryStats.RowsAffected.Valid = true
	queryStats.RowsAffected.Int64 = result.RowsAffected
	return result, nil
}

// FetchExists returns a boolean indicating if running the given Query on the
// given DB returned any results.
func FetchExists(db DB, query Query) (exists bool, err error) {
	return fetchExists(context.Background(), db, query, 1)
}

// FetchExistsContext is like FetchExists but additionally requires a
// context.Context.
func FetchExistsContext(ctx context.Context, db DB, query Query) (exists bool, err error) {
	return fetchExists(ctx, db, query, 1)
}

func fetchExists(ctx context.Context, db DB, query Query, skip int) (exists bool, err error) {
	dialect := query.GetDialect()
	if dialect == "" {
		defaultDialect := DefaultDialect.Load()
		if defaultDialect != nil {
			dialect = *defaultDialect
		}
	}
	queryStats := QueryStats{
		Dialect: dialect,
		Exists:  sql.NullBool{Valid: true},
		Params:  make(map[string][]int),
	}

	// Build query.
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	if dialect == DialectSQLServer {
		query = Queryf("SELECT CASE WHEN EXISTS ({}) THEN 1 ELSE 0 END", query)
	} else {
		query = Queryf("SELECT EXISTS ({})", query)
	}
	err = query.WriteSQL(ctx, dialect, buf, &queryStats.Args, queryStats.Params)
	queryStats.Query = buf.String()
	if err != nil {
		return false, err
	}

	// Setup logger.
	var logSettings LogSettings
	logger, _ := db.(SqLogger)
	if logger == nil {
		logQuery, _ := defaultLogQuery.Load().(func(context.Context, QueryStats))
		if logQuery != nil {
			logSettings, _ := defaultLogSettings.Load().(func(context.Context, *LogSettings))
			logger = &sqLogStruct{
				logSettings: logSettings,
				logQuery:    logQuery,
			}
		}
	}
	if logger != nil {
		logger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			queryStats.CallerFile, queryStats.CallerLine, queryStats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go logger.SqLogQuery(ctx, queryStats)
			} else {
				logger.SqLogQuery(ctx, queryStats)
			}
		}()
	}

	// Run query.
	if logSettings.IncludeTime {
		queryStats.StartedAt = time.Now()
	}
	var sqlRows *sql.Rows
	sqlRows, queryStats.Err = db.QueryContext(ctx, queryStats.Query, queryStats.Args...)
	if logSettings.IncludeTime {
		queryStats.TimeTaken = time.Since(queryStats.StartedAt)
	}
	if queryStats.Err != nil {
		return false, queryStats.Err
	}

	for sqlRows.Next() {
		err = sqlRows.Scan(&exists)
		if err != nil {
			return false, err
		}
		break
	}
	queryStats.Exists.Bool = exists

	if err := sqlRows.Close(); err != nil {
		return exists, err
	}
	if err := sqlRows.Err(); err != nil {
		return exists, err
	}
	return exists, nil
}

// substituteParams will return a new args slice by substituting values from
// the given paramValues. The input args slice is untouched.
func substituteParams(dialect string, args []any, paramIndexes map[string][]int, paramValues map[string]any) ([]any, error) {
	if len(paramValues) == 0 {
		return args, nil
	}
	newArgs := make([]any, len(args))
	copy(newArgs, args)
	var err error
	for name, value := range paramValues {
		indexes := paramIndexes[name]
		for _, index := range indexes {
			switch arg := newArgs[index].(type) {
			case sql.NamedArg:
				arg.Value, err = preprocessValue(dialect, value)
				if err != nil {
					return nil, err
				}
				newArgs[index] = arg
			default:
				value, err = preprocessValue(dialect, value)
				if err != nil {
					return nil, err
				}
				newArgs[index] = value
			}
		}
	}
	return newArgs, nil
}

func caller(skip int) (file string, line int, function string) {
	pc, file, line, _ := runtime.Caller(skip + 1)
	fn := runtime.FuncForPC(pc)
	function = fn.Name()
	return file, line, function
}
