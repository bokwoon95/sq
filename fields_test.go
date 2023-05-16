package sq

import (
	"bytes"
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
	"github.com/google/uuid"
)

func TestTableStruct(t *testing.T) {
	t.Parallel()
	tbl := NewTableStruct("public", "users", "u")
	if diff := testutil.Diff(tbl.GetAlias(), "u"); diff != "" {
		t.Error(testutil.Callers(), diff)
	}
	gotQuery, _, err := ToSQL("", tbl, nil)
	if err != nil {
		t.Error(testutil.Callers(), err)
	}
	if diff := testutil.Diff(gotQuery, "public.users"); diff != "" {
		t.Error(testutil.Callers(), diff)
	}
}

func TestArrayField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewArrayField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("alias brackets removed", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "t (id, name, email)")
		f1 := NewArrayField("field", tbl)
		gotQuery, _, err := ToSQL("", f1, nil)
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
		if diff := testutil.Diff(tbl.GetAlias(), "t (id, name, email)"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if diff := testutil.Diff(gotQuery, "t.field"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewArrayField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Set", item: field.SetArray([]int{1, 2, 3}),
		wantQuery: "field = ?",
		wantArgs:  []any{`[1,2,3]`},
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestBinaryField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewBinaryField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewBinaryField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: field.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: field.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "EqBytes", item: field.EqBytes([]byte{0xff, 0xff}),
		wantQuery: "tbl.field = ?", wantArgs: []any{[]byte{0xff, 0xff}},
	}, {
		description: "NeBytes", item: field.NeBytes([]byte{0xff, 0xff}),
		wantQuery: "tbl.field <> ?", wantArgs: []any{[]byte{0xff, 0xff}},
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetBytes", item: field.SetBytes([]byte{0xff, 0xff}),
		wantQuery: "field = ?", wantArgs: []any{[]byte{0xff, 0xff}},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestBooleanField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewBooleanField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewBooleanField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: field.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: field.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "EqBytes", item: field.EqBool(true),
		wantQuery: "tbl.field = ?", wantArgs: []any{true},
	}, {
		description: "NeBytes", item: field.NeBool(true),
		wantQuery: "tbl.field <> ?", wantArgs: []any{true},
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetBool", item: field.SetBool(true),
		wantQuery: "field = ?", wantArgs: []any{true},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestCustomField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewAnyField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewAnyField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: field.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: field.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: field.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "NotIn", item: field.NotIn(RowValue{1, 2, 3}),
		wantQuery: "tbl.field NOT IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: field.Lt(field),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: field.Le(field),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: field.Gt(field),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: field.Ge(field),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "Expr", item: field.Expr("&& ARRAY[1, 2, 3]"),
		wantQuery: "tbl.field && ARRAY[1, 2, 3]",
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set Self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestEnumField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewEnumField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewEnumField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "In", item: field.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "NotIn", item: field.NotIn(RowValue{1, 2, 3}),
		wantQuery: "tbl.field NOT IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "EqEnum", item: field.Eq(Monday),
		wantQuery: "tbl.field = ?",
		wantArgs:  []any{"Monday"},
	}, {
		description: "NeEnum", item: field.Ne(Monday),
		wantQuery: "tbl.field <> ?",
		wantArgs:  []any{"Monday"},
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "SetEnum", item: field.Set(Monday),
		wantQuery: "field = ?",
		wantArgs:  []any{"Monday"},
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestJSONField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewJSONField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	type Data struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	field := NewJSONField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Set", item: field.SetJSON([]int{1, 2, 3}),
		wantQuery: "field = ?",
		wantArgs:  []any{`[1,2,3]`},
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestNumberField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewNumberField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewNumberField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: field.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: field.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: field.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "NotIn", item: field.NotIn(RowValue{1, 2, 3}),
		wantQuery: "tbl.field NOT IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: field.Lt(field),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: field.Le(field),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: field.Gt(field),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: field.Ge(field),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "EqInt", item: field.EqInt(3),
		wantQuery: "tbl.field = ?", wantArgs: []any{3},
	}, {
		description: "NeInt", item: field.NeInt(3),
		wantQuery: "tbl.field <> ?", wantArgs: []any{3},
	}, {
		description: "LtInt", item: field.LtInt(3),
		wantQuery: "tbl.field < ?", wantArgs: []any{3},
	}, {
		description: "LeInt", item: field.LeInt(3),
		wantQuery: "tbl.field <= ?", wantArgs: []any{3},
	}, {
		description: "GtInt", item: field.GtInt(3),
		wantQuery: "tbl.field > ?", wantArgs: []any{3},
	}, {
		description: "GeInt", item: field.GeInt(3),
		wantQuery: "tbl.field >= ?", wantArgs: []any{3},
	}, {
		description: "EqInt64", item: field.EqInt64(5),
		wantQuery: "tbl.field = ?", wantArgs: []any{int64(5)},
	}, {
		description: "NeInt64", item: field.NeInt64(5),
		wantQuery: "tbl.field <> ?", wantArgs: []any{int64(5)},
	}, {
		description: "LtInt64", item: field.LtInt64(5),
		wantQuery: "tbl.field < ?", wantArgs: []any{int64(5)},
	}, {
		description: "LeInt64", item: field.LeInt64(5),
		wantQuery: "tbl.field <= ?", wantArgs: []any{int64(5)},
	}, {
		description: "GtInt64", item: field.GtInt64(5),
		wantQuery: "tbl.field > ?", wantArgs: []any{int64(5)},
	}, {
		description: "GeInt64", item: field.GeInt64(5),
		wantQuery: "tbl.field >= ?", wantArgs: []any{int64(5)},
	}, {
		description: "EqFloat64", item: field.EqFloat64(7.11),
		wantQuery: "tbl.field = ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "NeFloat64", item: field.NeFloat64(7.11),
		wantQuery: "tbl.field <> ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "LtFloat64", item: field.LtFloat64(7.11),
		wantQuery: "tbl.field < ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "LeFloat64", item: field.LeFloat64(7.11),
		wantQuery: "tbl.field <= ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "GtFloat64", item: field.GtFloat64(7.11),
		wantQuery: "tbl.field > ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "GeFloat64", item: field.GeFloat64(7.11),
		wantQuery: "tbl.field >= ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetInt", item: field.SetInt(3),
		wantQuery: "field = ?", wantArgs: []any{3},
	}, {
		description: "SetInt64", item: field.SetInt64(5),
		wantQuery: "field = ?", wantArgs: []any{int64(5)},
	}, {
		description: "SetFloat64", item: field.SetFloat64(7.11),
		wantQuery: "field = ?", wantArgs: []any{float64(7.11)},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestStringField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewStringField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewStringField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: field.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: field.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: field.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "NotIn", item: field.NotIn(RowValue{1, 2, 3}),
		wantQuery: "tbl.field NOT IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: field.Lt(field),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: field.Le(field),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: field.Gt(field),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: field.Ge(field),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "EqString", item: field.EqString("lorem ipsum"),
		wantQuery: "tbl.field = ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "NeString", item: field.NeString("lorem ipsum"),
		wantQuery: "tbl.field <> ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "LtString", item: field.LtString("lorem ipsum"),
		wantQuery: "tbl.field < ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "LeString", item: field.LeString("lorem ipsum"),
		wantQuery: "tbl.field <= ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "GtString", item: field.GtString("lorem ipsum"),
		wantQuery: "tbl.field > ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "GeString", item: field.GeString("lorem ipsum"),
		wantQuery: "tbl.field >= ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "LikeString", item: field.LikeString("lorem%"),
		wantQuery: "tbl.field LIKE ?", wantArgs: []any{"lorem%"},
	}, {
		description: "NotLikeString", item: field.NotLikeString("lorem%"),
		wantQuery: "tbl.field NOT LIKE ?", wantArgs: []any{"lorem%"},
	}, {
		description: "ILikeString", item: field.ILikeString("lorem%"),
		wantQuery: "tbl.field ILIKE ?", wantArgs: []any{"lorem%"},
	}, {
		description: "NotILikeString", item: field.NotILikeString("lorem%"),
		wantQuery: "tbl.field NOT ILIKE ?", wantArgs: []any{"lorem%"},
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetString", item: field.SetString("lorem ipsum"),
		wantQuery: "field = ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "postgres Collate", item: field.Collate("c").LtString("lorem ipsum"),
		dialect:   DialectPostgres,
		wantQuery: `tbl.field COLLATE "c" < $1`, wantArgs: []any{"lorem ipsum"},
	}, {
		description: "mysql Collate", item: field.Collate("latin1_swedish_ci").LtString("lorem ipsum"),
		wantQuery: "tbl.field COLLATE latin1_swedish_ci < ?", wantArgs: []any{"lorem ipsum"},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestTimeField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewTimeField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewTimeField("field", NewTableStruct("", "tbl", ""))
	zeroTime := time.Unix(0, 0).UTC()
	tests := []TestTable{{
		description: "IsNull", item: field.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: field.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: field.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: field.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: field.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "NotIn", item: field.NotIn(RowValue{1, 2, 3}),
		wantQuery: "tbl.field NOT IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: field.Eq(field),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: field.Ne(field),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: field.Lt(field),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: field.Le(field),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: field.Gt(field),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: field.Ge(field),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "EqTime", item: field.EqTime(zeroTime),
		wantQuery: "tbl.field = ?", wantArgs: []any{zeroTime},
	}, {
		description: "NeTime", item: field.NeTime(zeroTime),
		wantQuery: "tbl.field <> ?", wantArgs: []any{zeroTime},
	}, {
		description: "LtTime", item: field.LtTime(zeroTime),
		wantQuery: "tbl.field < ?", wantArgs: []any{zeroTime},
	}, {
		description: "LeTime", item: field.LeTime(zeroTime),
		wantQuery: "tbl.field <= ?", wantArgs: []any{zeroTime},
	}, {
		description: "GtTime", item: field.GtTime(zeroTime),
		wantQuery: "tbl.field > ?", wantArgs: []any{zeroTime},
	}, {
		description: "GeTime", item: field.GeTime(zeroTime),
		wantQuery: "tbl.field >= ?", wantArgs: []any{zeroTime},
	}, {
		description: "Set", item: field.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: field.Set(field.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetTime", item: field.SetTime(zeroTime),
		wantQuery: "field = ?", wantArgs: []any{zeroTime},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

func TestTimestamp(t *testing.T) {
	t.Run("Value", func(t *testing.T) {
		type TestTable struct {
			description string
			timestamp   Timestamp
			wantValue   driver.Value
		}

		tests := []TestTable{{
			description: "empty",
			timestamp:   Timestamp{},
			wantValue:   nil,
		}, {
			description: "sqlite",
			timestamp: Timestamp{
				Valid:   true,
				Time:    time.Unix(1, 0),
				dialect: DialectSQLite,
			},
			wantValue: time.Unix(1, 0).Unix(),
		}, {
			description: "non-sqlite",
			timestamp: Timestamp{
				Valid:   true,
				Time:    time.Unix(1, 0),
				dialect: DialectPostgres,
			},
			wantValue: time.Unix(1, 0),
		}}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.description, func(t *testing.T) {
				t.Parallel()
				gotValue, err := tt.timestamp.Value()
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				if diff := testutil.Diff(gotValue, tt.wantValue); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})
		}
	})

	t.Run("Scan", func(t *testing.T) {
		type TestTable struct {
			description   string
			value         any
			wantTimestamp Timestamp
		}

		parseTime := func(value string) time.Time {
			value = strings.TrimSuffix(value, "Z")
			for _, format := range timestampFormats {
				if timeVal, err := time.ParseInLocation(format, value, time.UTC); err == nil {
					return timeVal
				}
			}
			t.Fatalf(testutil.Callers()+" could not convert %q into time", value)
			return time.Time{}
		}

		tests := []TestTable{{
			description:   "empty",
			value:         nil,
			wantTimestamp: Timestamp{},
		}, {
			description:   "empty string",
			value:         "",
			wantTimestamp: Timestamp{},
		}, {
			description:   "empty bytes",
			value:         []byte{},
			wantTimestamp: Timestamp{},
		}, {
			description:   "time.Time",
			value:         time.Unix(1, 0),
			wantTimestamp: NewTimestamp(time.Unix(1, 0)),
		}, {
			description:   "time.Time",
			value:         time.Unix(1, 0),
			wantTimestamp: NewTimestamp(time.Unix(1, 0)),
		}, {
			description:   "2006-01-02 15:04:05",
			value:         "2006-01-02 15:04:05",
			wantTimestamp: NewTimestamp(parseTime("2006-01-02 15:04:05")),
		}, {
			description:   "2006-01-02 15:04:05Z",
			value:         []byte("2006-01-02 15:04:05Z"),
			wantTimestamp: NewTimestamp(parseTime("2006-01-02 15:04:05Z")),
		}, {
			description:   "2006-01-02 15:04:05-07:00",
			value:         "2006-01-02 15:04:05-07:00",
			wantTimestamp: NewTimestamp(parseTime("2006-01-02 15:04:05-07:00")),
		}, {
			description:   "2006-01-02 15:04:05.9999",
			value:         []byte("2006-01-02 15:04:05.9999"),
			wantTimestamp: NewTimestamp(parseTime("2006-01-02 15:04:05.9999")),
		}, {
			description:   "int64 seconds",
			value:         int64(123456),
			wantTimestamp: NewTimestamp(time.Unix(123456, 0)),
		}, {
			description:   "int64 milliseconds",
			value:         int64(1e12 + 1),
			wantTimestamp: NewTimestamp(time.Unix(0, int64(1e12+1)*int64(time.Millisecond))),
		}}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.description, func(t *testing.T) {
				t.Parallel()
				var gotTimestamp Timestamp
				err := gotTimestamp.Scan(tt.value)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				if diff := testutil.Diff(gotTimestamp, tt.wantTimestamp); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})
		}
	})
}

func TestUUIDField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewUUIDField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	field := NewUUIDField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull",
		item:        field.IsNull(),
		wantQuery:   "tbl.field IS NULL",
	}, {
		description: "IsNotNull",
		item:        field.IsNotNull(),
		wantQuery:   "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast",
		item:        field.Asc().NullsLast(),
		wantQuery:   "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst",
		item:        field.Desc().NullsFirst(),
		wantQuery:   "tbl.field DESC NULLS FIRST",
	}, {
		description: "In",
		item:        field.In(RowValue{1, 2, 3}),
		wantQuery:   "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "NotIn",
		item:        field.NotIn(RowValue{1, 2, 3}),
		wantQuery:   "tbl.field NOT IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq",
		item:        field.Eq(field),
		wantQuery:   "tbl.field = tbl.field",
	}, {
		description: "Ne",
		item:        field.Ne(field),
		wantQuery:   "tbl.field <> tbl.field",
	}, {
		description: "EqUUID",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.EqUUID(id)
		}(),
		wantQuery: "tbl.field = ?",
		wantArgs:  []any{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}, {
		description: "NeUUID",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.NeUUID(id)
		}(),
		wantQuery: "tbl.field <> ?",
		wantArgs:  []any{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}, {
		description: "Eq id",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.Eq(id)
		}(),
		wantQuery: "tbl.field = ?",
		wantArgs:  []any{"ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}, {
		description: "Ne id",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.Ne(id)
		}(),
		wantQuery: "tbl.field <> ?",
		wantArgs:  []any{"ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}, {
		description: "Set",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.Set(id)
		}(),
		wantQuery: "field = ?",
		wantArgs:  []any{"ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}, {
		description: "SetUUID",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.SetUUID(id)
		}(),
		wantQuery: "field = ?",
		wantArgs:  []any{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}, {
		description: "Setf",
		item:        field.Setf("VALUES({})", field.WithPrefix("")),
		wantQuery:   "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED",
		item:        field.Set(field.WithPrefix("EXCLUDED")),
		wantQuery:   "field = EXCLUDED.field",
	}, {
		description: "Set self",
		item:        field.Set(field), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias",
		item:        field.Set(field.WithPrefix("new")),
		wantQuery:   "field = new.field",
	}, {
		description: "Set id",
		item: func() any {
			id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			return field.Set(id)
		}(),
		wantQuery: "field = ?",
		wantArgs:  []any{"ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}

type DummyTable struct{}

var _ Table = (*DummyTable)(nil)

func (t DummyTable) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return nil
}

func (t DummyTable) GetAlias() string { return "" }

func (t DummyTable) IsTable() {}

func TestNew(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		type USER struct {
			TableStruct
			USER_ID NumberField
			NAME    StringField
			EMAIL   StringField
		}
		u := New[USER]("u")
		TestTable{
			item:      Queryf("SELECT {} FROM {} AS {}", Fields{u.USER_ID, u.NAME, u.EMAIL}, u, Expr(u.GetAlias())),
			wantQuery: "SELECT u.user_id, u.name, u.email FROM user AS u",
		}.assert(t)
	})

	t.Run("name", func(t *testing.T) {
		type USER struct {
			TableStruct `sq:"User"`
			USER_ID     NumberField `sq:"UserId"`
			NAME        StringField `sq:"Name"`
			EMAIL       StringField `sq:"Email"`
			private     int
		}
		u := New[USER]("u")
		TestTable{
			item:      Queryf("SELECT {} FROM {} AS {}", Fields{u.USER_ID, u.NAME, u.EMAIL}, u, Expr(u.GetAlias())),
			wantQuery: `SELECT u."UserId", u."Name", u."Email" FROM "User" AS u`,
		}.assert(t)
	})

	t.Run("schema", func(t *testing.T) {
		type USER struct {
			TableStruct `sq:"public.user"`
			USER_ID     NumberField
			NAME        StringField
			EMAIL       StringField
			Public      int
		}
		u := New[USER]("u")
		TestTable{
			item:      Queryf("SELECT {} FROM {} AS {}", Fields{u.USER_ID, u.NAME, u.EMAIL}, u, Expr(u.GetAlias())),
			wantQuery: "SELECT u.user_id, u.name, u.email FROM public.user AS u",
		}.assert(t)
	})

	t.Run("first field not a struct", func(t *testing.T) {
		tbl := New[tmptable]("")
		if diff := testutil.Diff(tbl, tmptable("")); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("struct has no fields", func(t *testing.T) {
		tbl := New[DummyTable]("")
		if diff := testutil.Diff(tbl, DummyTable{}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("first field is unexported", func(t *testing.T) {
		tbl := New[Expression]("")
		if diff := testutil.Diff(tbl, Expression{}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	t.Run("first field is not TableStruct", func(t *testing.T) {
		tbl := New[struct{ DummyTable }]("")
		if diff := testutil.Diff(tbl, struct{ DummyTable }{}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})
}
