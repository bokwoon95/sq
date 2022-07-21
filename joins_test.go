package sq

import "testing"

func TestJoinTables(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("a")

	tests := []TestTable{{
		description: "JoinUsing",
		item:        JoinUsing(a, a.FIRST_NAME, a.LAST_NAME),
		wantQuery:   "JOIN actor AS a USING (first_name, last_name)",
	}, {
		description: "Join without operator",
		item:        CustomJoin("", a, a.ACTOR_ID.Eq(a.ACTOR_ID), a.FIRST_NAME.Ne(a.LAST_NAME)),
		wantQuery:   "JOIN actor AS a ON a.actor_id = a.actor_id AND a.first_name <> a.last_name",
	}, {
		description: "Join",
		item:        Join(a, a.ACTOR_ID.Eq(a.ACTOR_ID)),
		wantQuery:   "JOIN actor AS a ON a.actor_id = a.actor_id",
	}, {
		description: "LeftJoin",
		item:        LeftJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)),
		wantQuery:   "LEFT JOIN actor AS a ON a.actor_id = a.actor_id",
	}, {
		description: "Right Join",
		item:        JoinTable{JoinOperator: JoinRight, Table: a, OnPredicate: a.ACTOR_ID.Eq(a.ACTOR_ID)},
		wantQuery:   "RIGHT JOIN actor AS a ON a.actor_id = a.actor_id",
	}, {
		description: "FullJoin",
		item:        FullJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)),
		wantQuery:   "FULL JOIN actor AS a ON a.actor_id = a.actor_id",
	}, {
		description: "CrossJoin",
		item:        CrossJoin(a),
		wantQuery:   "CROSS JOIN actor AS a",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}

	notOKTests := []TestTable{{
		description: "full join has no predicate",
		item:        FullJoin(a),
	}, {
		description: "sqlite does not support full join",
		dialect:     DialectSQLite,
		item:        FullJoin(a, Expr("TRUE")),
	}, {
		description: "table is nil",
		item:        Join(nil, Expr("TRUE")),
	}, {
		description: "UsingField returns err",
		item:        JoinUsing(a, nil),
	}}

	for _, tt := range notOKTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertNotOK(t)
		})
	}

	errTests := []TestTable{{
		description: "table err",
		item:        Join(FaultySQL{}, a.ACTOR_ID.Eq(a.ACTOR_ID)),
	}, {
		description: "VariadicPredicate err",
		item:        Join(a, And(FaultySQL{})),
	}, {
		description: "predicate err",
		item:        Join(a, FaultySQL{}),
	}}

	for _, tt := range errTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertErr(t, ErrFaultySQL)
		})
	}
}
