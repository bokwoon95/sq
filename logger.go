package sq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// QueryStats represents the statistics from running a query.
type QueryStats struct {
	// Dialect of the query.
	Dialect string

	// Query string.
	Query string

	// Args slice provided with the query string.
	Args []any

	// Params maps param names back to arguments in the args slice (by index).
	Params map[string][]int

	// Err is the error from running the query.
	Err error

	// RowCount from running the query. Not valid for Exec().
	RowCount sql.NullInt64

	// RowsAffected by running the query. Not valid for
	// FetchOne/FetchAll/FetchCursor.
	RowsAffected sql.NullInt64

	// LastInsertId of the query.
	LastInsertId sql.NullInt64

	// Exists is the result of FetchExists().
	Exists sql.NullBool

	// When the query started at.
	StartedAt time.Time

	// Time taken by the query.
	TimeTaken time.Duration

	// The caller file where the query was invoked.
	CallerFile string

	// The line in the caller file that invoked the query.
	CallerLine int

	// The name of the function where the query was invoked.
	CallerFunction string

	// The results from running the query (if it was provided).
	Results string
}

// LogSettings are the various log settings taken into account when producing
// the QueryStats.
type LogSettings struct {
	// Dispatch logging asynchronously (logs may arrive out of order which can be confusing, but it won't block function calls).
	LogAsynchronously bool

	// Include time taken by the query.
	IncludeTime bool

	// Include caller (filename and line number).
	IncludeCaller bool

	// Include fetched results.
	IncludeResults int
}

// SqLogger represents a logger for the sq package.
type SqLogger interface {
	// SqLogSettings should populate a LogSettings struct, which influences
	// what is added into the QueryStats.
	SqLogSettings(context.Context, *LogSettings)

	// SqLogQuery logs a query when for the given QueryStats.
	SqLogQuery(context.Context, QueryStats)
}

type sqLogger struct {
	logger *log.Logger
	config LoggerConfig
}

// LoggerConfig is the config used for the sq logger.
type LoggerConfig struct {
	// Dispatch logging asynchronously (logs may arrive out of order which can be confusing, but it won't block function calls).
	LogAsynchronously bool

	// Show time taken by the query.
	ShowTimeTaken bool

	// Show caller (filename and line number).
	ShowCaller bool

	// Show fetched results.
	ShowResults int

	// If true, logs are shown as plaintext (no color).
	NoColor bool

	// Verbose query interpolation, which shows the query before and after
	// interpolating query arguments. The logged query is interpolated by
	// default, InterpolateVerbose only controls whether the query before
	// interpolation is shown. To disable query interpolation entirely, look at
	// HideArgs.
	InterpolateVerbose bool

	// Explicitly hides arguments when logging the query (only the query
	// placeholders will be shown).
	HideArgs bool
}

var _ SqLogger = (*sqLogger)(nil)

var defaultLogger = NewLogger(os.Stdout, "", log.LstdFlags, LoggerConfig{
	ShowTimeTaken: true,
	ShowCaller:    true,
})

var verboseLogger = NewLogger(os.Stdout, "", log.LstdFlags, LoggerConfig{
	ShowTimeTaken:      true,
	ShowCaller:         true,
	ShowResults:        5,
	InterpolateVerbose: true,
})

// NewLogger returns a new SqLogger.
func NewLogger(w io.Writer, prefix string, flag int, config LoggerConfig) SqLogger {
	return &sqLogger{
		logger: log.New(w, prefix, flag),
		config: config,
	}
}

// SqLogSettings implements the SqLogger interface.
func (l *sqLogger) SqLogSettings(ctx context.Context, settings *LogSettings) {
	settings.LogAsynchronously = l.config.LogAsynchronously
	settings.IncludeTime = l.config.ShowTimeTaken
	settings.IncludeCaller = l.config.ShowCaller
	settings.IncludeResults = l.config.ShowResults
}

// SqLogQuery implements the SqLogger interface.
func (l *sqLogger) SqLogQuery(ctx context.Context, queryStats QueryStats) {
	var reset, red, green, blue, purple string
	envNoColor, _ := strconv.ParseBool(os.Getenv("NO_COLOR"))
	if !l.config.NoColor && !envNoColor {
		reset = colorReset
		red = colorRed
		green = colorGreen
		blue = colorBlue
		purple = colorPurple
	}
	buf := bufpool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufpool.Put(buf)
	if queryStats.Err == nil {
		buf.WriteString(green + "[OK]" + reset)
	} else {
		buf.WriteString(red + "[FAIL]" + reset)
	}
	if l.config.HideArgs {
		buf.WriteString(" " + queryStats.Query + ";")
	} else if !l.config.InterpolateVerbose {
		if queryStats.Err != nil {
			buf.WriteString(" " + queryStats.Query + ";")
			if len(queryStats.Args) > 0 {
				buf.WriteString(" [")
			}
			for i := 0; i < len(queryStats.Args); i++ {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fmt.Sprintf("%#v", queryStats.Args[i]))
			}
			if len(queryStats.Args) > 0 {
				buf.WriteString("]")
			}
		} else {
			query, err := Sprintf(queryStats.Dialect, queryStats.Query, queryStats.Args)
			if err != nil {
				query += " " + err.Error()
			}
			buf.WriteString(" " + query + ";")
		}
	}
	if queryStats.Err != nil {
		errStr := queryStats.Err.Error()
		if i := strings.IndexByte(errStr, '\n'); i < 0 {
			buf.WriteString(blue + " err" + reset + "={" + queryStats.Err.Error() + "}")
		}
	}
	if l.config.ShowTimeTaken {
		buf.WriteString(blue + " timeTaken" + reset + "=" + queryStats.TimeTaken.String())
	}
	if queryStats.RowCount.Valid {
		buf.WriteString(blue + " rowCount" + reset + "=" + strconv.FormatInt(queryStats.RowCount.Int64, 10))
	}
	if queryStats.RowsAffected.Valid {
		buf.WriteString(blue + " rowsAffected" + reset + "=" + strconv.FormatInt(queryStats.RowsAffected.Int64, 10))
	}
	if queryStats.LastInsertId.Valid {
		buf.WriteString(blue + " lastInsertId" + reset + "=" + strconv.FormatInt(queryStats.LastInsertId.Int64, 10))
	}
	if queryStats.Exists.Valid {
		buf.WriteString(blue + " exists" + reset + "=" + strconv.FormatBool(queryStats.Exists.Bool))
	}
	if l.config.ShowCaller {
		buf.WriteString(blue + " caller" + reset + "=" + queryStats.CallerFile + ":" + strconv.Itoa(queryStats.CallerLine) + ":" + filepath.Base(queryStats.CallerFunction))
	}
	if !l.config.HideArgs && l.config.InterpolateVerbose {
		buf.WriteString("\n" + purple + "----[ Executing query ]----" + reset)
		buf.WriteString("\n" + queryStats.Query + "; " + fmt.Sprintf("%#v", queryStats.Args))
		buf.WriteString("\n" + purple + "----[ with bind values ]----" + reset)
		query, err := Sprintf(queryStats.Dialect, queryStats.Query, queryStats.Args)
		query += ";"
		if err != nil {
			query += " " + err.Error()
		}
		buf.WriteString("\n" + query)
	}
	if l.config.ShowResults > 0 && queryStats.Err == nil {
		buf.WriteString("\n" + purple + "----[ Fetched result ]----" + reset)
		buf.WriteString(queryStats.Results)
		if queryStats.RowCount.Int64 > int64(l.config.ShowResults) {
			buf.WriteString("\n...\n(Fetched " + strconv.FormatInt(queryStats.RowCount.Int64, 10) + " rows)")
		}
	}
	if buf.Len() > 0 {
		l.logger.Println(buf.String())
	}
}

// Log wraps a DB and adds logging to it.
func Log(db DB) interface {
	DB
	SqLogger
} {
	return struct {
		DB
		SqLogger
	}{DB: db, SqLogger: defaultLogger}
}

// VerboseLog wraps a DB and adds verbose logging to it.
func VerboseLog(db DB) interface {
	DB
	SqLogger
} {
	return struct {
		DB
		SqLogger
	}{DB: db, SqLogger: verboseLogger}
}

var defaultLogSettings atomic.Value

// SetDefaultLogSettings sets the function to configure the default
// LogSettings. This value is not used unless SetDefaultLogQuery is also
// configured.
func SetDefaultLogSettings(logSettings func(context.Context, *LogSettings)) {
	defaultLogSettings.Store(logSettings)
}

var defaultLogQuery atomic.Value

// SetDefaultLogQuery sets the default logging function to call for all
// queries (if a logger is not explicitly passed in).
func SetDefaultLogQuery(logQuery func(context.Context, QueryStats)) {
	defaultLogQuery.Store(logQuery)
}

type sqLogStruct struct {
	logSettings func(context.Context, *LogSettings)
	logQuery    func(context.Context, QueryStats)
}

var _ SqLogger = (*sqLogStruct)(nil)

func (l *sqLogStruct) SqLogSettings(ctx context.Context, logSettings *LogSettings) {
	if l.logSettings == nil {
		return
	}
	l.logSettings(ctx, logSettings)
}

func (l *sqLogStruct) SqLogQuery(ctx context.Context, queryStats QueryStats) {
	if l.logQuery == nil {
		return
	}
	l.logQuery(ctx, queryStats)
}

const (
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[91m"
	colorGreen  = "\x1b[92m"
	colorYellow = "\x1b[93m"
	colorBlue   = "\x1b[94m"
	colorPurple = "\x1b[95m"
	colorCyan   = "\x1b[96m"
	colorGray   = "\x1b[97m"
	colorWhite  = "\x1b[97m"
)
