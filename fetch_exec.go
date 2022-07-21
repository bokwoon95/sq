package sq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bokwoon95/sq/internal/googleuuid"
)

// A Cursor represents a database cursor.
type Cursor[T any] struct {
	ctx         context.Context
	row         *Row
	sqlRows     *sql.Rows
	rowmapper   func(*Row) (T, error)
	stats       QueryStats
	logSettings LogSettings
	logger      SqLogger
	logged      int32
	fieldNames  []string
	resultsBuf  *bytes.Buffer
}

// FetchCursor returns a new cursor.
func FetchCursor[T any](db DB, q Query, rowmapper func(*Row) (T, error)) (*Cursor[T], error) {
	return fetchCursor(context.Background(), db, q, rowmapper, 1)
}

// FetchCursorContext is like FetchCursor but additionally requires a context.Context.
func FetchCursorContext[T any](ctx context.Context, db DB, q Query, rowmapper func(*Row) (T, error)) (*Cursor[T], error) {
	return fetchCursor(ctx, db, q, rowmapper, 1)
}

func fetchCursor[T any](ctx context.Context, db DB, q Query, rowmapper func(*Row) (T, error), skip int) (c *Cursor[T], err error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if q == nil {
		return nil, fmt.Errorf("query is nil")
	}
	if rowmapper == nil {
		return nil, fmt.Errorf("rowmapper is nil")
	}
	c = &Cursor[T]{ctx: ctx, rowmapper: rowmapper}

	// Get fields and dest from rowmapper
	dialect := q.GetDialect()
	c.row = newRow(dialect)
	defer recoverRowmapperPanic(&err)
	_, err = c.rowmapper(c.row)
	if err != nil {
		return nil, err
	}
	c.row.active = true
	if len(c.row.fields) == 0 || len(c.row.dest) == 0 {
		return nil, fmt.Errorf("rowmapper did not yield any fields")
	}
	var ok bool
	q, ok = q.SetFetchableFields(c.row.fields)
	if !ok {
		return nil, fmt.Errorf("query does not support setting fetchable fields")
	}

	// Setup logger
	c.stats.RowCount.Valid = true
	if c.logger, ok = db.(SqLogger); ok {
		c.logger.SqLogSettings(ctx, &c.logSettings)
		if c.logSettings.IncludeCaller {
			c.stats.CallerFile, c.stats.CallerLine, c.stats.CallerFunction = caller(skip + 1)
		}
	}

	// Build query
	c.stats.Params = make(map[string][]int)
	c.stats.Query, c.stats.Args, c.stats.Err = ToSQLContext(ctx, dialect, q, c.stats.Params)
	if c.stats.Err != nil {
		c.log()
		return nil, c.stats.Err
	}

	// Run query
	if c.logSettings.IncludeTime {
		c.stats.StartedAt = time.Now()
	}
	c.sqlRows, c.stats.Err = db.QueryContext(ctx, c.stats.Query, c.stats.Args...)
	if c.logSettings.IncludeTime {
		c.stats.TimeTaken = time.Since(c.stats.StartedAt)
	}
	if c.stats.Err != nil {
		c.log()
		return nil, c.stats.Err
	}

	// Allocate resultsBuf
	if c.logSettings.IncludeResults > 0 {
		c.resultsBuf = bufpool.Get().(*bytes.Buffer)
		c.resultsBuf.Reset()
	}
	return c, nil
}

// Next advances the cursor to the next result.
func (c *Cursor[T]) Next() bool {
	hasNext := c.sqlRows.Next()
	if hasNext {
		c.stats.RowCount.Int64++
	} else {
		c.log()
	}
	return hasNext
}

// RowCount returns the current row number so far.
func (c *Cursor[T]) RowCount() int64 { return c.stats.RowCount.Int64 }

// Result returns the cursor result.
func (c *Cursor[T]) Result() (result T, err error) {
	c.stats.Err = c.sqlRows.Scan(c.row.dest...)
	// If scan returns an error, annotate the error with the fields and dest
	// types so the user can see what went wrong.
	if c.stats.Err != nil {
		c.log()
		errbuf := bufpool.Get().(*bytes.Buffer)
		errbuf.Reset()
		defer bufpool.Put(errbuf)
		buf := bufpool.Get().(*bytes.Buffer)
		buf.Reset()
		defer bufpool.Put(buf)
		args := new([]any)
		for i := range c.row.dest {
			errbuf.WriteString("\n" + strconv.Itoa(i) + ") ")
			errbuf.Reset()
			*args = (*args)[:0]
			err := c.row.fields[i].WriteSQL(c.ctx, c.stats.Dialect, buf, args, nil)
			if err != nil {
				errbuf.WriteString("%!(error=" + err.Error() + ")")
				continue
			}
			lhs, err := Sprintf(c.stats.Dialect, errbuf.String(), *args)
			if err != nil {
				errbuf.WriteString("%!(error=" + err.Error() + ")")
				continue
			}
			errbuf.WriteString(lhs + " => " + reflect.TypeOf(c.row.dest[i]).String())
		}
		return result, fmt.Errorf("please check if your mapper function is correct:%s\n%w", errbuf.String(), err)
	}
	// If results should be logged, write the row into the resultsBuf.
	if c.resultsBuf != nil && c.stats.RowCount.Int64 <= int64(c.logSettings.IncludeResults) {
		if len(c.fieldNames) == 0 {
			c.fieldNames = getFieldNames(c.ctx, c.stats.Dialect, c.row.fields)
		}
		c.resultsBuf.WriteString("\n----[ Row " + strconv.FormatInt(c.stats.RowCount.Int64, 10) + " ]----")
		for i := range c.row.dest {
			c.resultsBuf.WriteString("\n")
			if i < len(c.fieldNames) {
				c.resultsBuf.WriteString(c.fieldNames[i])
			}
			c.resultsBuf.WriteString(": ")
			destValue := c.row.dest[i]
			if _, ok := c.row.destIsString[i]; ok {
				if b, ok := c.row.dest[i].(*[]byte); ok {
					if len(*b) > 0 {
						destValue = string(*b)
					} else {
						destValue = nil
					}
				}
			} else if c.stats.Dialect == DialectPostgres {
				if _, ok := c.row.destIsUUID[i]; ok {
					if b, ok := c.row.dest[i].(*[]byte); ok {
						if len(*b) > 0 {
							var uuid [16]byte
							var buf [36]byte
							copy(uuid[:], *b)
							googleuuid.EncodeHex(buf[:], uuid)
							destValue = string(buf[:])
						} else {
							destValue = nil
						}
					}
				}
			}
			rhs, err := Sprint(c.stats.Dialect, destValue)
			if err != nil {
				c.resultsBuf.WriteString("%!(error=" + err.Error() + ")")
				continue
			}
			c.resultsBuf.WriteString(rhs)
		}
	}
	c.row.index = 0
	defer recoverRowmapperPanic(&err)
	result, c.stats.Err = c.rowmapper(c.row)
	if c.stats.Err != nil {
		c.log()
		return result, c.stats.Err
	}
	return result, nil
}

func (c *Cursor[T]) log() {
	if !atomic.CompareAndSwapInt32(&c.logged, 0, 1) {
		return
	}
	if c.resultsBuf != nil {
		c.stats.Results = c.resultsBuf.String()
		bufpool.Put(c.resultsBuf)
	}
	if c.logger == nil {
		return
	}
	if c.logSettings.LogAsynchronously {
		go c.logger.SqLogQuery(c.ctx, c.stats)
	} else {
		c.logger.SqLogQuery(c.ctx, c.stats)
	}
}

// Close closes the cursor.
func (c *Cursor[T]) Close() error {
	c.log()
	if err := c.sqlRows.Close(); err != nil {
		return err
	}
	if err := c.sqlRows.Err(); err != nil {
		return err
	}
	return nil
}

// FetchOne returns the first result from running the given Query on the given
// DB.
func FetchOne[T any](db DB, q Query, rowmapper func(*Row) (T, error)) (T, error) {
	cursor, err := fetchCursor(context.Background(), db, q, rowmapper, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchOneContext is like FetchOne but additionally requires a context.Context.
func FetchOneContext[T any](ctx context.Context, db DB, q Query, rowmapper func(*Row) (T, error)) (T, error) {
	cursor, err := fetchCursor(ctx, db, q, rowmapper, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchAll returns all results from running the given Query on the given DB.
func FetchAll[T any](db DB, q Query, rowmapper func(*Row) (T, error)) ([]T, error) {
	cursor, err := fetchCursor(context.Background(), db, q, rowmapper, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// FetchAllContext is like FetchAll but additionally requires a context.Context.
func FetchAllContext[T any](ctx context.Context, db DB, q Query, rowmapper func(*Row) (T, error)) ([]T, error) {
	cursor, err := fetchCursor(ctx, db, q, rowmapper, 1)
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
	rowmapper func(*Row) (T, error)
}

// NewCompiledFetch returns a new CompiledFetch.
func NewCompiledFetch[T any](dialect string, query string, args []any, params map[string][]int, rowmapper func(*Row) (T, error)) *CompiledFetch[T] {
	return &CompiledFetch[T]{
		dialect:   dialect,
		query:     query,
		args:      args,
		params:    params,
		rowmapper: rowmapper,
	}
}

// CompileFetch returns a new CompileFetch.
func CompileFetch[T any](q Query, rowmapper func(*Row) (T, error)) (*CompiledFetch[T], error) {
	return CompileFetchContext(context.Background(), q, rowmapper)
}

// CompileFetchContext is like CompileFetch but accpets a context.Context.
func CompileFetchContext[T any](ctx context.Context, q Query, rowmapper func(*Row) (T, error)) (f *CompiledFetch[T], err error) {
	if q == nil {
		return nil, fmt.Errorf("query is nil")
	}
	if rowmapper == nil {
		return nil, fmt.Errorf("rowmapper is nil")
	}

	// Get fields from rowmapper
	dialect := q.GetDialect()
	row := newRow(dialect)
	defer recoverRowmapperPanic(&err)
	_, err = rowmapper(row)
	if err != nil {
		return nil, err
	}
	if len(row.fields) == 0 {
		return nil, fmt.Errorf("rowmapper did not yield any fields")
	}
	var ok bool
	q, ok = q.SetFetchableFields(row.fields)
	if !ok {
		return nil, fmt.Errorf("query does not support setting fetchable fields")
	}

	// Build query
	f = &CompiledFetch[T]{}
	f.rowmapper = rowmapper
	f.params = make(map[string][]int)
	f.query, f.args, err = ToSQLContext(ctx, dialect, q, f.params)
	if err != nil {
		return f, err
	}
	return f, nil
}

// FetchCursor returns a new cursor.
func (f *CompiledFetch[T]) FetchCursor(db DB, params Params) (*Cursor[T], error) {
	return f.fetchCursor(context.Background(), db, params, 1)
}

// FetchCursorContext is like FetchCursor but additionally requires a context.Context.
func (f *CompiledFetch[T]) FetchCursorContext(ctx context.Context, db DB, params Params) (*Cursor[T], error) {
	return f.fetchCursor(ctx, db, params, 1)
}

func (f *CompiledFetch[T]) fetchCursor(ctx context.Context, db DB, params Params, skip int) (c *Cursor[T], err error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	c = &Cursor[T]{
		ctx: ctx,
		stats: QueryStats{
			Dialect: f.dialect,
			Query:   f.query,
			Args:    f.args,
			Params:  f.params,
		},
		rowmapper: f.rowmapper,
	}

	// Get fields and dest from rowmapper
	c.row = newRow(f.dialect)
	defer recoverRowmapperPanic(&err)
	_, err = c.rowmapper(c.row)
	if err != nil {
		return nil, err
	}
	c.row.active = true
	if len(c.row.fields) == 0 || len(c.row.dest) == 0 {
		return nil, fmt.Errorf("rowmapper did not yield any fields")
	}

	// Substitute params
	c.stats.Args, err = substituteParams(c.stats.Dialect, c.stats.Args, c.stats.Params, params)
	if err != nil {
		return nil, err
	}

	// Setup logger
	var ok bool
	c.stats.RowCount.Valid = true
	if c.logger, ok = db.(SqLogger); ok {
		c.logger.SqLogSettings(ctx, &c.logSettings)
		if c.logSettings.IncludeCaller {
			c.stats.CallerFile, c.stats.CallerLine, c.stats.CallerFunction = caller(skip + 1)
		}
	}

	// Run query
	if c.logSettings.IncludeTime {
		c.stats.StartedAt = time.Now()
	}
	c.sqlRows, c.stats.Err = db.QueryContext(ctx, c.stats.Query, c.stats.Args...)
	if c.logSettings.IncludeTime {
		c.stats.TimeTaken = time.Since(c.stats.StartedAt)
	}
	if c.stats.Err != nil {
		return nil, c.stats.Err
	}

	// Allocate resultsBuf
	if c.logSettings.IncludeResults > 0 {
		c.resultsBuf = bufpool.Get().(*bytes.Buffer)
		c.resultsBuf.Reset()
	}
	return c, nil
}

// FetchOne returns the first result from running the CompiledFetch on the
// given DB with the give params.
func (f *CompiledFetch[T]) FetchOne(db DB, params Params) (T, error) {
	cursor, err := f.fetchCursor(context.Background(), db, params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchOneContext is like FetchOne but additionally requires a context.Context.
func (f *CompiledFetch[T]) FetchOneContext(ctx context.Context, db DB, params Params) (T, error) {
	cursor, err := f.fetchCursor(ctx, db, params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchAll returns all the results from running the CompiledFetch on the given
// DB with the give params.
func (f *CompiledFetch[T]) FetchAll(db DB, params Params) ([]T, error) {
	cursor, err := f.fetchCursor(context.Background(), db, params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// FetchAllContext is like FetchAll but additionally requires a context.Context.
func (f *CompiledFetch[T]) FetchAllContext(ctx context.Context, db DB, params Params) ([]T, error) {
	cursor, err := f.fetchCursor(ctx, db, params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// GetSQL returns a copy of the dialect, query, args, params and rowmapper that
// make up the CompiledFetch.
func (f *CompiledFetch[T]) GetSQL() (dialect string, query string, args []any, params map[string][]int, rowmapper func(*Row) (T, error)) {
	dialect = f.dialect
	query = f.query
	args = make([]any, len(f.args))
	params = make(map[string][]int)
	copy(args, f.args)
	for name, indexes := range f.params {
		indexes2 := make([]int, len(indexes))
		copy(indexes2, indexes)
		params[name] = indexes2
	}
	return dialect, query, args, params, f.rowmapper
}

// Prepare creates a PreparedFetch from a CompiledFetch by preparing it on
// the given DB.
func (f *CompiledFetch[T]) Prepare(db DB) (*PreparedFetch[T], error) {
	return f.PrepareContext(context.Background(), db)
}

// PrepareContext is like Prepare but additionally requires a context.Context.
func (f *CompiledFetch[T]) PrepareContext(ctx context.Context, db DB) (*PreparedFetch[T], error) {
	var err error
	pf := &PreparedFetch[T]{
		compiled: NewCompiledFetch(f.GetSQL()),
	}
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	pf.stmt, err = db.PrepareContext(ctx, f.query)
	if err != nil {
		return nil, err
	}
	pf.logger, _ = db.(SqLogger)
	return pf, nil
}

// PreparedFetch is the result of preparing a CompiledFetch on a DB.
type PreparedFetch[T any] struct {
	compiled *CompiledFetch[T]
	stmt     *sql.Stmt
	logger   SqLogger
}

// PrepareFetch returns a new PreparedFetch.
func PrepareFetch[T any](db DB, q Query, rowmapper func(*Row) (T, error)) (*PreparedFetch[T], error) {
	return PrepareFetchContext(context.Background(), db, q, rowmapper)
}

// PrepareFetchContext is like PrepareFetch but additionally requires a context.Context.
func PrepareFetchContext[T any](ctx context.Context, db DB, q Query, rowmapper func(*Row) (T, error)) (*PreparedFetch[T], error) {
	compiledFetch, err := CompileFetchContext(ctx, q, rowmapper)
	if err != nil {
		return nil, err
	}
	return compiledFetch.PrepareContext(ctx, db)
}

// FetchCursor returns a new cursor.
func (f PreparedFetch[T]) FetchCursor(params Params) (*Cursor[T], error) {
	return f.fetchCursor(context.Background(), params, 1)
}

// FetchCursorContext is like FetchCursor but additionally requires a context.Context.
func (f PreparedFetch[T]) FetchCursorContext(ctx context.Context, params Params) (*Cursor[T], error) {
	return f.fetchCursor(ctx, params, 1)
}

func (f *PreparedFetch[T]) fetchCursor(ctx context.Context, params Params, skip int) (c *Cursor[T], err error) {
	c = &Cursor[T]{
		ctx: ctx,
		stats: QueryStats{
			Dialect: f.compiled.dialect,
			Query:   f.compiled.query,
			Args:    f.compiled.args,
			Params:  f.compiled.params,
		},
		rowmapper: f.compiled.rowmapper,
		logger:    f.logger,
	}

	// Get fields and dest from rowmapper
	c.row = newRow(f.compiled.dialect)
	defer recoverRowmapperPanic(&err)
	_, err = c.rowmapper(c.row)
	if err != nil {
		return nil, err
	}
	c.row.active = true
	if len(c.row.fields) == 0 || len(c.row.dest) == 0 {
		return nil, fmt.Errorf("rowmapper did not yield any fields")
	}

	// Substitute params
	c.stats.Args, err = substituteParams(c.stats.Dialect, c.stats.Args, c.stats.Params, params)
	if err != nil {
		return nil, err
	}

	// Setup logger
	c.stats.RowCount.Valid = true
	if c.logger != nil {
		c.logger.SqLogSettings(ctx, &c.logSettings)
		if c.logSettings.IncludeCaller {
			c.stats.CallerFile, c.stats.CallerLine, c.stats.CallerFunction = caller(skip + 1)
		}
	}

	// Run query
	if c.logSettings.IncludeTime {
		c.stats.StartedAt = time.Now()
	}
	c.sqlRows, c.stats.Err = f.stmt.QueryContext(ctx, c.stats.Args...)
	if c.logSettings.IncludeTime {
		c.stats.TimeTaken = time.Since(c.stats.StartedAt)
	}
	if c.stats.Err != nil {
		return nil, c.stats.Err
	}

	// Allocate resultsBuf
	if c.logSettings.IncludeResults > 0 {
		c.resultsBuf = bufpool.Get().(*bytes.Buffer)
		c.resultsBuf.Reset()
	}
	return c, nil
}

// FetchOne returns the first result from running the PreparedFetch with the
// give params.
func (f *PreparedFetch[T]) FetchOne(params Params) (T, error) {
	cursor, err := f.fetchCursor(context.Background(), params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchOneContext is like FetchOne but additionally requires a context.Context.
func (f *PreparedFetch[T]) FetchOneContext(ctx context.Context, params Params) (T, error) {
	cursor, err := f.fetchCursor(ctx, params, 1)
	if err != nil {
		return *new(T), err
	}
	defer cursor.Close()
	return cursorResult(cursor)
}

// FetchAll returns all the results from running the PreparedFetch with the
// give params.
func (f *PreparedFetch[T]) FetchAll(params Params) ([]T, error) {
	cursor, err := f.fetchCursor(context.Background(), params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// FetchAllContext is like FetchAll but additionally requires a context.Context.
func (f *PreparedFetch[T]) FetchAllContext(ctx context.Context, params Params) ([]T, error) {
	cursor, err := f.fetchCursor(ctx, params, 1)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return cursorResults(cursor)
}

// GetCompiled returns a copy of the underlying CompiledFetch.
func (f *PreparedFetch[T]) GetCompiled() *CompiledFetch[T] {
	return NewCompiledFetch(f.compiled.GetSQL())
}

// Close closes the PreparedFetch.
func (f *PreparedFetch[T]) Close() error {
	if f.stmt == nil {
		return nil
	}
	return f.stmt.Close()
}

// Exec executes the given Query on the given DB.
func Exec(db DB, q Query) (Result, error) {
	return exec(context.Background(), db, q, 1)
}

// ExecContext is like Exec but additionally requires a context.Context.
func ExecContext(ctx context.Context, db DB, q Query) (Result, error) {
	return exec(ctx, db, q, 1)
}

func exec(ctx context.Context, db DB, q Query, skip int) (result Result, err error) {
	if db == nil {
		return result, fmt.Errorf("db is nil")
	}
	if q == nil {
		return result, fmt.Errorf("query is nil")
	}

	// Setup logging
	var logSettings LogSettings
	var stats QueryStats
	if sqLogger, ok := db.(SqLogger); ok {
		sqLogger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			stats.CallerFile, stats.CallerLine, stats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go sqLogger.SqLogQuery(ctx, stats)
			} else {
				sqLogger.SqLogQuery(ctx, stats)
			}
		}()
	}

	// Build query
	stats.Dialect = q.GetDialect()
	stats.Params = make(map[string][]int)
	stats.Query, stats.Args, stats.Err = ToSQLContext(ctx, stats.Dialect, q, stats.Params)
	if stats.Err != nil {
		return result, stats.Err
	}

	// Run query
	if logSettings.IncludeTime {
		stats.StartedAt = time.Now()
	}
	var res sql.Result
	res, stats.Err = db.ExecContext(ctx, stats.Query, stats.Args...)
	if logSettings.IncludeTime {
		stats.TimeTaken = time.Since(stats.StartedAt)
	}
	if stats.Err != nil {
		return result, stats.Err
	}
	return execResult(res, &stats)
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
func CompileExec(q Query) (*CompiledExec, error) {
	return CompileExecContext(context.Background(), q)
}

// CompileExecContext is like CompileExec but additionally requires a context.Context.
func CompileExecContext(ctx context.Context, q Query) (*CompiledExec, error) {
	var err error
	e := &CompiledExec{}
	if q == nil {
		return nil, fmt.Errorf("query is nil")
	}
	e.dialect = q.GetDialect()
	e.params = make(map[string][]int)
	e.query, e.args, err = ToSQLContext(ctx, e.dialect, q, e.params)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Exec executes the CompiledExec on the given DB with the given params.
func (e *CompiledExec) Exec(db DB, params Params) (Result, error) {
	return e.exec(context.Background(), db, params, 1)
}

// ExecContext is like Exec but additionally requires a context.Context.
func (e *CompiledExec) ExecContext(ctx context.Context, db DB, params Params) (Result, error) {
	return e.exec(ctx, db, params, 1)
}

func (e *CompiledExec) exec(ctx context.Context, db DB, params Params, skip int) (result Result, err error) {
	if db == nil {
		return result, fmt.Errorf("db is nil")
	}

	// Setup logging
	var logSettings LogSettings
	stats := QueryStats{
		Dialect: e.dialect,
		Query:   e.query,
		Args:    e.args,
		Params:  e.params,
	}
	if sqLogger, ok := db.(SqLogger); ok {
		sqLogger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			stats.CallerFile, stats.CallerLine, stats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go sqLogger.SqLogQuery(ctx, stats)
			} else {
				sqLogger.SqLogQuery(ctx, stats)
			}
		}()
	}

	// Substitute params
	stats.Args, err = substituteParams(stats.Dialect, stats.Args, stats.Params, params)
	if err != nil {
		return result, err
	}

	// Run query
	if logSettings.IncludeTime {
		stats.StartedAt = time.Now()
	}
	var res sql.Result
	res, stats.Err = db.ExecContext(ctx, stats.Query, stats.Args...)
	if logSettings.IncludeTime {
		stats.TimeTaken = time.Since(stats.StartedAt)
	}
	if stats.Err != nil {
		return result, stats.Err
	}
	return execResult(res, &stats)
}

func (e *CompiledExec) GetSQL() (dialect string, query string, args []any, params map[string][]int) {
	dialect = e.dialect
	query = e.query
	args = make([]any, len(e.args))
	params = make(map[string][]int)
	copy(args, e.args)
	for name, indexes := range e.params {
		indexes2 := make([]int, len(indexes))
		copy(indexes2, indexes)
		params[name] = indexes2
	}
	return dialect, query, args, params
}

// Prepare creates a PreparedExec from a CompiledExec by preparing it on the
// given DB.
func (e *CompiledExec) Prepare(db DB) (*PreparedExec, error) {
	return e.PrepareContext(context.Background(), db)
}

// PrepareContext is like Prepare but additionally requires a context.Context.
func (e *CompiledExec) PrepareContext(ctx context.Context, db DB) (*PreparedExec, error) {
	var err error
	pe := &PreparedExec{
		compiled: NewCompiledExec(e.GetSQL()),
	}
	pe.stmt, err = db.PrepareContext(ctx, e.query)
	if err != nil {
		return nil, err
	}
	pe.logger, _ = db.(SqLogger)
	return pe, nil
}

// PrepareExec is the result of preparing a CompiledExec on a DB.
type PreparedExec struct {
	compiled *CompiledExec
	stmt     *sql.Stmt
	logger   SqLogger
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
func (e *PreparedExec) Close() error {
	if e.stmt == nil {
		return nil
	}
	return e.stmt.Close()
}

// Exec executes the PreparedExec with the given params.
func (e *PreparedExec) Exec(params Params) (Result, error) {
	return e.exec(context.Background(), params, 1)
}

// ExecContext is like Exec but additionally requires a context.Context.
func (e *PreparedExec) ExecContext(ctx context.Context, params Params) (Result, error) {
	return e.exec(ctx, params, 1)
}

func (e *PreparedExec) exec(ctx context.Context, params Params, skip int) (result Result, err error) {
	// Setup logging
	var logSettings LogSettings
	stats := QueryStats{
		Dialect: e.compiled.dialect,
		Query:   e.compiled.query,
		Args:    e.compiled.args,
		Params:  e.compiled.params,
	}
	if e.logger != nil {
		e.logger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			stats.CallerFile, stats.CallerLine, stats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go e.logger.SqLogQuery(ctx, stats)
			} else {
				e.logger.SqLogQuery(ctx, stats)
			}
		}()
	}

	// Substitute params
	stats.Args, err = substituteParams(stats.Dialect, stats.Args, stats.Params, params)
	if err != nil {
		return result, err
	}

	// Run query
	if logSettings.IncludeTime {
		stats.StartedAt = time.Now()
	}
	var res sql.Result
	res, stats.Err = e.stmt.ExecContext(ctx, stats.Args...)
	if logSettings.IncludeTime {
		stats.TimeTaken = time.Since(stats.StartedAt)
	}
	if stats.Err != nil {
		return result, stats.Err
	}
	return execResult(res, &stats)
}

func recoverRowmapperPanic(err *error) {
	if r := recover(); r != nil {
		switch r := r.(type) {
		case error:
			*err = r
		default:
			*err = fmt.Errorf("rowmapper panic: %v", r)
		}
	}
}

func getFieldNames(ctx context.Context, dialect string, fields []Field) []string {
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	args := new([]any)
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		if alias := getAlias(field); alias != "" {
			fieldNames = append(fieldNames, alias)
			continue
		}
		buf.Reset()
		*args = (*args)[:0]
		err := field.WriteSQL(ctx, dialect, buf, args, nil)
		if err != nil {
			fieldNames = append(fieldNames, "%!(error="+err.Error()+")")
			continue
		}
		fieldName, err := Sprintf(dialect, buf.String(), *args)
		if err != nil {
			fieldNames = append(fieldNames, "%!(error="+err.Error()+")")
			continue
		}
		fieldNames = append(fieldNames, fieldName)
	}
	return fieldNames
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

func execResult(res sql.Result, stats *QueryStats) (Result, error) {
	var err error
	var result Result
	if stats.Dialect == DialectSQLite || stats.Dialect == DialectMySQL {
		result.LastInsertId, err = res.LastInsertId()
		if err != nil {
			return result, err
		}
		stats.LastInsertId.Valid = true
		stats.LastInsertId.Int64 = result.LastInsertId
	}
	result.RowsAffected, err = res.RowsAffected()
	if err != nil {
		return result, err
	}
	stats.RowsAffected.Valid = true
	stats.RowsAffected.Int64 = result.RowsAffected
	return result, nil
}

// FetchExists returns a boolean indicating if running the given Query on the
// given DB returned any results.
func FetchExists(db DB, q Query) (exists bool, err error) {
	return fetchExists(context.Background(), db, q, 1)
}

// FetchExistsContext is like FetchExists but additionally requires a
// context.Context.
func FetchExistsContext(ctx context.Context, db DB, q Query) (exists bool, err error) {
	return fetchExists(ctx, db, q, 1)
}

func fetchExists(ctx context.Context, db DB, q Query, skip int) (exists bool, err error) {
	// Setup logger
	var stats QueryStats
	var logSettings LogSettings
	stats.RowCount.Valid = true
	if sqLogger, ok := db.(SqLogger); ok {
		sqLogger.SqLogSettings(ctx, &logSettings)
		if logSettings.IncludeCaller {
			stats.CallerFile, stats.CallerLine, stats.CallerFunction = caller(skip + 1)
		}
		defer func() {
			if logSettings.LogAsynchronously {
				go sqLogger.SqLogQuery(ctx, stats)
			} else {
				sqLogger.SqLogQuery(ctx, stats)
			}
		}()
	}

	// Build query
	stats.Dialect = q.GetDialect()
	var existsQuery Query
	if stats.Dialect == DialectSQLServer {
		existsQuery = Queryf("IF EXISTS ({}) SELECT 1 ELSE SELECT 0", q)
	} else {
		existsQuery = Queryf("SELECT EXISTS ({})", q)
	}
	stats.Params = make(map[string][]int)
	stats.Query, stats.Args, stats.Err = ToSQLContext(ctx, stats.Dialect, existsQuery, stats.Params)
	if stats.Err != nil {
		return false, stats.Err
	}

	// Run query
	if logSettings.IncludeTime {
		stats.StartedAt = time.Now()
	}
	var sqlRows *sql.Rows
	sqlRows, stats.Err = db.QueryContext(ctx, stats.Query, stats.Args...)
	if logSettings.IncludeTime {
		stats.TimeTaken = time.Since(stats.StartedAt)
	}
	if stats.Err != nil {
		return false, stats.Err
	}

	for sqlRows.Next() {
		stats.Err = sqlRows.Scan(&exists)
		if stats.Err != nil {
			return false, stats.Err
		}
		break
	}
	stats.Exists.Valid = true
	stats.Exists.Bool = exists

	if err := sqlRows.Close(); err != nil {
		return exists, err
	}
	if err := sqlRows.Err(); err != nil {
		return exists, err
	}
	return exists, nil
}
