package sq

import "testing"

func TestWindow(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		f1, f2, f3 := Expr("f1"), Expr("f2"), Expr("f3")
		TestTable{
			item:      PartitionBy(f1).OrderBy(f2, f3).Frame("RANGE UNBOUNDED PRECEDING"),
			wantQuery: "(PARTITION BY f1 ORDER BY f2, f3 RANGE UNBOUNDED PRECEDING)",
		}.assert(t)
		TestTable{
			item:      OrderBy(f1).PartitionBy(f2, f3).Frame("ROWS {} PRECEDING", 5),
			wantQuery: "(PARTITION BY f2, f3 ORDER BY f1 ROWS ? PRECEDING)",
			wantArgs:  []any{5},
		}.assert(t)
	})

	errTests := []TestTable{{
		description: "PartitionBy err", item: PartitionBy(FaultySQL{}),
	}, {
		description: "OrderBy err", item: OrderBy(FaultySQL{}),
	}, {
		description: "Frame err", item: OrderBy(Expr("f")).Frame("ROWS {} PRECEDING", FaultySQL{}),
	}, {
		description: "NamedWindows err", item: NamedWindows{{
			Name:       "w",
			Definition: OrderBy(Expr("f")).Frame("ROWS {} PRECEDING", FaultySQL{}),
		}},
	}}

	for _, tt := range errTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertErr(t, ErrFaultySQL)
		})
	}

	funcTests := []TestTable{{
		description: "CountOver", item: CountOver(Expr("f1"), WindowDefinition{}),
		wantQuery: "COUNT(f1) OVER ()",
	}, {
		description: "CountOver nil", item: CountOver(Expr("f1"), nil),
		wantQuery: "COUNT(f1) OVER ()",
	}, {
		description: "CountStarOver", item: CountStarOver(WindowDefinition{}),
		wantQuery: "COUNT(*) OVER ()",
	}, {
		description: "SumOver", item: SumOver(Expr("f1"), PartitionBy(Expr("f2"))),
		wantQuery: "SUM(f1) OVER (PARTITION BY f2)",
	}, {
		description: "AvgOver", item: AvgOver(Expr("f1"), PartitionBy(Expr("f2"))),
		wantQuery: "AVG(f1) OVER (PARTITION BY f2)",
	}, {
		description: "MinOver", item: MinOver(Expr("f1"), PartitionBy(Expr("f2"))),
		wantQuery: "MIN(f1) OVER (PARTITION BY f2)",
	}, {
		description: "MaxOver", item: MaxOver(Expr("f1"), PartitionBy(Expr("f2"))),
		wantQuery: "MAX(f1) OVER (PARTITION BY f2)",
	}, {
		description: "RowNumberOver", item: RowNumberOver(PartitionBy(Expr("f1"))),
		wantQuery: "ROW_NUMBER() OVER (PARTITION BY f1)",
	}, {
		description: "RankOver", item: RankOver(PartitionBy(Expr("f1"))),
		wantQuery: "RANK() OVER (PARTITION BY f1)",
	}, {
		description: "DenseRankOver", item: DenseRankOver(PartitionBy(Expr("f1"))),
		wantQuery: "DENSE_RANK() OVER (PARTITION BY f1)",
	}, {
		description: "CumeDistOver", item: CumeDistOver(PartitionBy(Expr("f1"))),
		wantQuery: "CUME_DIST() OVER (PARTITION BY f1)",
	}, {
		description: "FirstValueOver", item: FirstValueOver(Expr("f1"), PartitionBy(Expr("f2"))),
		wantQuery: "FIRST_VALUE(f1) OVER (PARTITION BY f2)",
	}, {
		description: "LastValueOver", item: LastValueOver(Expr("f1"), PartitionBy(Expr("f2"))),
		wantQuery: "LAST_VALUE(f1) OVER (PARTITION BY f2)",
	}, {
		description: "NamedWindow", item: CountStarOver(NamedWindow{Name: "w"}),
		wantQuery: "COUNT(*) OVER w",
	}, func() TestTable {
		var tt TestTable
		tt.description = "BaseWindow"
		w := NamedWindow{Name: "w", Definition: PartitionBy(Expr("f1"))}
		tt.item = CountStarOver(BaseWindow(w).Frame("ROWS UNBOUNDED PRECEDING"))
		tt.wantQuery = "COUNT(*) OVER (w ROWS UNBOUNDED PRECEDING)"
		return tt
	}(), func() TestTable {
		var tt TestTable
		tt.description = "NamedWindows"
		w1 := NamedWindow{Name: "w1", Definition: PartitionBy(Expr("f1"))}
		w2 := NamedWindow{Name: "w2", Definition: OrderBy(Expr("f2"))}
		w3 := NamedWindow{Name: "w3", Definition: OrderBy(Expr("f3")).Frame("ROWS UNBOUNDED PRECEDING")}
		tt.item = NamedWindows{w1, w2, w3}
		tt.wantQuery = "w1 AS (PARTITION BY f1)" +
			", w2 AS (ORDER BY f2)" +
			", w3 AS (ORDER BY f3 ROWS UNBOUNDED PRECEDING)"
		return tt
	}()}

	for _, tt := range funcTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assert(t)
		})
	}
}
