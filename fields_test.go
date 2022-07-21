package sq

import (
	"bytes"
	"context"
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

	f := NewArrayField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
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

	f := NewBinaryField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: f.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "EqBytes", item: f.EqBytes([]byte{0xff, 0xff}),
		wantQuery: "tbl.field = ?", wantArgs: []any{[]byte{0xff, 0xff}},
	}, {
		description: "NeBytes", item: f.NeBytes([]byte{0xff, 0xff}),
		wantQuery: "tbl.field <> ?", wantArgs: []any{[]byte{0xff, 0xff}},
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetBytes", item: f.SetBytes([]byte{0xff, 0xff}),
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

	f := NewBooleanField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "EqBytes", item: f.EqBool(true),
		wantQuery: "tbl.field = ?", wantArgs: []any{true},
	}, {
		description: "NeBytes", item: f.NeBool(true),
		wantQuery: "tbl.field <> ?", wantArgs: []any{true},
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetBool", item: f.SetBool(true),
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

	f := NewAnyField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: f.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: f.Lt(f),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: f.Le(f),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: f.Gt(f),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: f.Ge(f),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "Expr", item: f.Expr("&& ARRAY[1, 2, 3]"),
		wantQuery: "tbl.field && ARRAY[1, 2, 3]",
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set Self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
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

	f := NewEnumField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
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

	f := NewJSONField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
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

	f := NewNumberField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: f.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: f.Lt(f),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: f.Le(f),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: f.Gt(f),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: f.Ge(f),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "EqInt", item: f.EqInt(3),
		wantQuery: "tbl.field = ?", wantArgs: []any{3},
	}, {
		description: "NeInt", item: f.NeInt(3),
		wantQuery: "tbl.field <> ?", wantArgs: []any{3},
	}, {
		description: "LtInt", item: f.LtInt(3),
		wantQuery: "tbl.field < ?", wantArgs: []any{3},
	}, {
		description: "LeInt", item: f.LeInt(3),
		wantQuery: "tbl.field <= ?", wantArgs: []any{3},
	}, {
		description: "GtInt", item: f.GtInt(3),
		wantQuery: "tbl.field > ?", wantArgs: []any{3},
	}, {
		description: "GeInt", item: f.GeInt(3),
		wantQuery: "tbl.field >= ?", wantArgs: []any{3},
	}, {
		description: "EqInt64", item: f.EqInt64(5),
		wantQuery: "tbl.field = ?", wantArgs: []any{int64(5)},
	}, {
		description: "NeInt64", item: f.NeInt64(5),
		wantQuery: "tbl.field <> ?", wantArgs: []any{int64(5)},
	}, {
		description: "LtInt64", item: f.LtInt64(5),
		wantQuery: "tbl.field < ?", wantArgs: []any{int64(5)},
	}, {
		description: "LeInt64", item: f.LeInt64(5),
		wantQuery: "tbl.field <= ?", wantArgs: []any{int64(5)},
	}, {
		description: "GtInt64", item: f.GtInt64(5),
		wantQuery: "tbl.field > ?", wantArgs: []any{int64(5)},
	}, {
		description: "GeInt64", item: f.GeInt64(5),
		wantQuery: "tbl.field >= ?", wantArgs: []any{int64(5)},
	}, {
		description: "EqFloat64", item: f.EqFloat64(7.11),
		wantQuery: "tbl.field = ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "NeFloat64", item: f.NeFloat64(7.11),
		wantQuery: "tbl.field <> ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "LtFloat64", item: f.LtFloat64(7.11),
		wantQuery: "tbl.field < ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "LeFloat64", item: f.LeFloat64(7.11),
		wantQuery: "tbl.field <= ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "GtFloat64", item: f.GtFloat64(7.11),
		wantQuery: "tbl.field > ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "GeFloat64", item: f.GeFloat64(7.11),
		wantQuery: "tbl.field >= ?", wantArgs: []any{float64(7.11)},
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetInt", item: f.SetInt(3),
		wantQuery: "field = ?", wantArgs: []any{3},
	}, {
		description: "SetInt64", item: f.SetInt64(5),
		wantQuery: "field = ?", wantArgs: []any{int64(5)},
	}, {
		description: "SetFloat64", item: f.SetFloat64(7.11),
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

	f := NewStringField("field", NewTableStruct("", "tbl", ""))
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: f.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: f.Lt(f),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: f.Le(f),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: f.Gt(f),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: f.Ge(f),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "EqString", item: f.EqString("lorem ipsum"),
		wantQuery: "tbl.field = ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "NeString", item: f.NeString("lorem ipsum"),
		wantQuery: "tbl.field <> ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "LtString", item: f.LtString("lorem ipsum"),
		wantQuery: "tbl.field < ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "LeString", item: f.LeString("lorem ipsum"),
		wantQuery: "tbl.field <= ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "GtString", item: f.GtString("lorem ipsum"),
		wantQuery: "tbl.field > ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "GeString", item: f.GeString("lorem ipsum"),
		wantQuery: "tbl.field >= ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "LikeString", item: f.LikeString("lorem%"),
		wantQuery: "tbl.field LIKE ?", wantArgs: []any{"lorem%"},
	}, {
		description: "ILikeString", item: f.ILikeString("lorem%"),
		wantQuery: "tbl.field ILIKE ?", wantArgs: []any{"lorem%"},
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetString", item: f.SetString("lorem ipsum"),
		wantQuery: "field = ?", wantArgs: []any{"lorem ipsum"},
	}, {
		description: "postgres Collate", item: f.Collate("c").LtString("lorem ipsum"),
		dialect:   DialectPostgres,
		wantQuery: `tbl.field COLLATE "c" < $1`, wantArgs: []any{"lorem ipsum"},
	}, {
		description: "mysql Collate", item: f.Collate("latin1_swedish_ci").LtString("lorem ipsum"),
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

	f := NewTimeField("field", NewTableStruct("", "tbl", ""))
	zeroTime := time.Unix(0, 0).UTC()
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: f.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Lt", item: f.Lt(f),
		wantQuery: "tbl.field < tbl.field",
	}, {
		description: "Le", item: f.Le(f),
		wantQuery: "tbl.field <= tbl.field",
	}, {
		description: "Gt", item: f.Gt(f),
		wantQuery: "tbl.field > tbl.field",
	}, {
		description: "Ge", item: f.Ge(f),
		wantQuery: "tbl.field >= tbl.field",
	}, {
		description: "EqTime", item: f.EqTime(zeroTime),
		wantQuery: "tbl.field = ?", wantArgs: []any{zeroTime},
	}, {
		description: "NeTime", item: f.NeTime(zeroTime),
		wantQuery: "tbl.field <> ?", wantArgs: []any{zeroTime},
	}, {
		description: "LtTime", item: f.LtTime(zeroTime),
		wantQuery: "tbl.field < ?", wantArgs: []any{zeroTime},
	}, {
		description: "LeTime", item: f.LeTime(zeroTime),
		wantQuery: "tbl.field <= ?", wantArgs: []any{zeroTime},
	}, {
		description: "GtTime", item: f.GtTime(zeroTime),
		wantQuery: "tbl.field > ?", wantArgs: []any{zeroTime},
	}, {
		description: "GeTime", item: f.GeTime(zeroTime),
		wantQuery: "tbl.field >= ?", wantArgs: []any{zeroTime},
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "SetTime", item: f.SetTime(zeroTime),
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

func TestUUIDField(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tbl := NewTableStruct("", "tbl", "")
		f1 := NewUUIDField("field", tbl).As("f")
		if diff := testutil.Diff(f1.GetAlias(), "f"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	f := NewUUIDField("field", NewTableStruct("", "tbl", ""))
	id, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	if err != nil {
		t.Fatal(testutil.Callers(), err)
	}
	tests := []TestTable{{
		description: "IsNull", item: f.IsNull(),
		wantQuery: "tbl.field IS NULL",
	}, {
		description: "IsNotNull", item: f.IsNotNull(),
		wantQuery: "tbl.field IS NOT NULL",
	}, {
		description: "Asc NullsLast", item: f.Asc().NullsLast(),
		wantQuery: "tbl.field ASC NULLS LAST",
	}, {
		description: "Desc NullsFirst", item: f.Desc().NullsFirst(),
		wantQuery: "tbl.field DESC NULLS FIRST",
	}, {
		description: "In", item: f.In(RowValue{1, 2, 3}),
		wantQuery: "tbl.field IN (?, ?, ?)", wantArgs: []any{1, 2, 3},
	}, {
		description: "Eq", item: f.Eq(f),
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Ne", item: f.Ne(f),
		wantQuery: "tbl.field <> tbl.field",
	}, {
		description: "Eq id", item: f.Eq(id),
		wantQuery: "tbl.field = ?", wantArgs: []any{id},
	}, {
		description: "Ne id", item: f.Ne(id),
		wantQuery: "tbl.field <> ?", wantArgs: []any{id},
	}, {
		description: "Set", item: f.Set(Expr("NULL")),
		wantQuery: "field = NULL",
	}, {
		description: "Setf", item: f.Setf("VALUES({})", f.WithPrefix("")),
		wantQuery: "field = VALUES(field)",
	}, {
		description: "Set EXCLUDED", item: f.Set(f.WithPrefix("EXCLUDED")),
		wantQuery: "field = EXCLUDED.field",
	}, {
		description: "Set self", item: f.Set(f), dialect: DialectMySQL,
		wantQuery: "tbl.field = tbl.field",
	}, {
		description: "Set with alias", item: f.Set(f.WithPrefix("new")),
		wantQuery: "field = new.field",
	}, {
		description: "Set id", item: f.Set(id),
		wantQuery: "field = ?", wantArgs: []any{id},
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
