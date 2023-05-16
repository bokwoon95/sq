package sq

import (
	"database/sql"
	"testing"

	"github.com/bokwoon95/sq/internal/testutil"
	"github.com/google/uuid"
)

func Test_preprocessValue(t *testing.T) {
	type TestTable struct {
		description string
		dialect     string
		input       any
		wantOutput  any
	}

	tests := []TestTable{{
		description: "empty",
		input:       nil,
		wantOutput:  nil,
	}, {
		description: "driver.Valuer",
		input:       uuid.MustParse("a4f952f1-4c45-4e63-bd4e-159ca33c8e20"),
		wantOutput:  "a4f952f1-4c45-4e63-bd4e-159ca33c8e20",
	}, {
		description: "Postgres DialectValuer",
		dialect:     DialectPostgres,
		input:       UUIDValue(uuid.MustParse("a4f952f1-4c45-4e63-bd4e-159ca33c8e20")),
		wantOutput:  "a4f952f1-4c45-4e63-bd4e-159ca33c8e20",
	}, {
		description: "MySQL DialectValuer",
		dialect:     DialectMySQL,
		input:       UUIDValue(uuid.MustParse("a4f952f1-4c45-4e63-bd4e-159ca33c8e20")),
		wantOutput:  []byte{0xa4, 0xf9, 0x52, 0xf1, 0x4c, 0x45, 0x4e, 0x63, 0xbd, 0x4e, 0x15, 0x9c, 0xa3, 0x3c, 0x8e, 0x20},
	}, {
		description: "[16]byte",
		input:       [16]byte{0xa4, 0xf9, 0x52, 0xf1, 0x4c, 0x45, 0x4e, 0x63, 0xbd, 0x4e, 0x15, 0x9c, 0xa3, 0x3c, 0x8e, 0x20},
		wantOutput:  []byte{0xa4, 0xf9, 0x52, 0xf1, 0x4c, 0x45, 0x4e, 0x63, 0xbd, 0x4e, 0x15, 0x9c, 0xa3, 0x3c, 0x8e, 0x20},
	}, {
		description: "Enumeration",
		input:       Monday,
		wantOutput:  "Monday",
	}, {
		description: "int",
		input:       42,
		wantOutput:  42,
	}, {
		description: "sql.NullString",
		input: sql.NullString{
			Valid:  false,
			String: "lorem ipsum dolor sit amet",
		},
		wantOutput: nil,
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			gotOutput, err := preprocessValue(tt.dialect, tt.input)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(gotOutput, tt.wantOutput); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}
