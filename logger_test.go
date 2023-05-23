package sq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
)

func TestLogger(t *testing.T) {
	type TT struct {
		description string
		ctx         context.Context
		stats       QueryStats
		config      LoggerConfig
		wantOutput  string
	}

	assert := func(t *testing.T, tt TT) {
		if tt.ctx == nil {
			tt.ctx = context.Background()
		}
		buf := &bytes.Buffer{}
		logger := sqLogger{
			logger: log.New(buf, "", 0),
			config: tt.config,
		}
		logger.SqLogQuery(tt.ctx, tt.stats)
		if diff := testutil.Diff(buf.String(), tt.wantOutput); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	}

	t.Run("Log VerboseLog", func(t *testing.T) {
		t.Parallel()
		var logSettings LogSettings
		Log(nil).SqLogSettings(context.Background(), &logSettings)
		diff := testutil.Diff(logSettings, LogSettings{
			LogAsynchronously: false,
			IncludeTime:       true,
			IncludeCaller:     true,
			IncludeResults:    0,
		})
		if diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		VerboseLog(nil).SqLogSettings(context.Background(), &logSettings)
		diff = testutil.Diff(logSettings, LogSettings{
			LogAsynchronously: false,
			IncludeTime:       true,
			IncludeCaller:     true,
			IncludeResults:    5,
		})
		if diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("ctx.Done", func(t *testing.T) {
		t.Parallel()
		var tt TT
		var cancel context.CancelFunc
		tt.ctx, cancel = context.WithTimeout(context.Background(), 0)
		defer cancel()
		tt.wantOutput = ""
		assert(t, tt)
	})

	t.Run("no color", func(t *testing.T) {
		var tt TT
		tt.config.NoColor = true
		tt.stats.Query = "SELECT 1"
		tt.wantOutput = "[OK] SELECT 1;\n"
		assert(t, tt)
	})

	tests := []TT{{
		description: "err",
		stats: QueryStats{
			Query: "SELECT 1",
			Err:   fmt.Errorf("lorem ipsum"),
		},
		wantOutput: "\x1b[91m[FAIL]\x1b[0m SELECT 1;\x1b[94m err\x1b[0m={lorem ipsum}\n",
	}, {
		description: "HideArgs",
		config:      LoggerConfig{HideArgs: true},
		stats: QueryStats{
			Query: "SELECT ?", Args: []any{1},
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT ?;\n",
	}, {
		description: "RowCount",
		stats: QueryStats{
			Query:    "SELECT 1",
			RowCount: sql.NullInt64{Valid: true, Int64: 3},
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT 1;\x1b[94m rowCount\x1b[0m=3\n",
	}, {
		description: "RowsAffected",
		stats: QueryStats{
			Query:        "SELECT 1",
			RowsAffected: sql.NullInt64{Valid: true, Int64: 5},
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT 1;\x1b[94m rowsAffected\x1b[0m=5\n",
	}, {
		description: "LastInsertId",
		stats: QueryStats{
			Query:        "SELECT 1",
			LastInsertId: sql.NullInt64{Valid: true, Int64: 7},
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT 1;\x1b[94m lastInsertId\x1b[0m=7\n",
	}, {
		description: "Exists",
		stats: QueryStats{
			Query:  "SELECT EXISTS (SELECT 1)",
			Exists: sql.NullBool{Valid: true, Bool: true},
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT EXISTS (SELECT 1);\x1b[94m exists\x1b[0m=true\n",
	}, {
		description: "ShowCaller",
		config:      LoggerConfig{ShowCaller: true},
		stats: QueryStats{
			Query:          "SELECT 1",
			CallerFile:     "file.go",
			CallerLine:     22,
			CallerFunction: "someFunc",
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT 1;\x1b[94m caller\x1b[0m=file.go:22:someFunc\n",
	}, {
		description: "Verbose",
		config:      LoggerConfig{InterpolateVerbose: true, ShowTimeTaken: true},
		stats: QueryStats{
			Query: "SELECT ?, ?", Args: []any{1, "bob"},
			TimeTaken: 300 * time.Millisecond,
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m\x1b[94m timeTaken\x1b[0m=300ms" +
			"\n\x1b[95m----[ Executing query ]----\x1b[0m" +
			"\nSELECT ?, ?; []interface {}{1, \"bob\"}" +
			"\n\x1b[95m----[ with bind values ]----\x1b[0m" +
			"\nSELECT 1, 'bob';\n",
	}, {
		description: "ShowResults",
		config:      LoggerConfig{ShowResults: 1},
		stats: QueryStats{
			Query:   "SELECT 1",
			Results: "\nlorem ipsum dolor sit amet",
		},
		wantOutput: "\x1b[92m[OK]\x1b[0m SELECT 1;" +
			"\n\x1b[95m----[ Fetched result ]----\x1b[0m" +
			"\nlorem ipsum dolor sit amet\n",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			assert(t, tt)
		})
	}
}
