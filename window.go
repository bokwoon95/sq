package sq

import (
	"bytes"
	"context"
	"fmt"
)

// NamedWindow represents an SQL named window.
type NamedWindow struct {
	Name       string
	Definition Window
}

var _ Window = (*NamedWindow)(nil)

// WriteSQL implements the SQLWriter interface.
func (w NamedWindow) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	buf.WriteString(w.Name)
	return nil
}

// IsWindow implements the Window interface.
func (w NamedWindow) IsWindow() {}

// WindowDefinition represents an SQL window definition.
type WindowDefinition struct {
	BaseWindowName    string
	PartitionByFields []Field
	OrderByFields     []Field
	FrameSpec         string
	FrameValues       []any
}

var _ Window = (*WindowDefinition)(nil)

// BaseWindow creates a new WindowDefinition based off an existing NamedWindow.
func BaseWindow(w NamedWindow) WindowDefinition {
	return WindowDefinition{BaseWindowName: w.Name}
}

// PartitionBy returns a new WindowDefinition with the PARTITION BY clause.
func PartitionBy(fields ...Field) WindowDefinition {
	return WindowDefinition{PartitionByFields: fields}
}

// PartitionBy returns a new WindowDefinition with the ORDER BY clause.
func OrderBy(fields ...Field) WindowDefinition {
	return WindowDefinition{OrderByFields: fields}
}

// WriteSQL implements the SQLWriter interface.
func (w WindowDefinition) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	var written bool
	buf.WriteString("(")
	if w.BaseWindowName != "" {
		buf.WriteString(w.BaseWindowName + " ")
	}
	if len(w.PartitionByFields) > 0 {
		written = true
		buf.WriteString("PARTITION BY ")
		err = writeFields(ctx, dialect, buf, args, params, w.PartitionByFields, false)
		if err != nil {
			return fmt.Errorf("Window PARTITION BY: %w", err)
		}
	}
	if len(w.OrderByFields) > 0 {
		if written {
			buf.WriteString(" ")
		}
		written = true
		buf.WriteString("ORDER BY ")
		err = writeFields(ctx, dialect, buf, args, params, w.OrderByFields, false)
		if err != nil {
			return fmt.Errorf("Window ORDER BY: %w", err)
		}
	}
	if w.FrameSpec != "" {
		if written {
			buf.WriteString(" ")
		}
		written = true
		err = Writef(ctx, dialect, buf, args, params, w.FrameSpec, w.FrameValues)
		if err != nil {
			return fmt.Errorf("Window FRAME: %w", err)
		}
	}
	buf.WriteString(")")
	return nil
}

// PartitionBy returns a new WindowDefinition with the PARTITION BY clause.
func (w WindowDefinition) PartitionBy(fields ...Field) WindowDefinition {
	w.PartitionByFields = fields
	return w
}

// OrderBy returns a new WindowDefinition with the ORDER BY clause.
func (w WindowDefinition) OrderBy(fields ...Field) WindowDefinition {
	w.OrderByFields = fields
	return w
}

// Frame returns a new WindowDefinition with the frame specification set.
func (w WindowDefinition) Frame(frameSpec string, frameValues ...any) WindowDefinition {
	w.FrameSpec = frameSpec
	w.FrameValues = frameValues
	return w
}

// IsWindow implements the Window interface.
func (w WindowDefinition) IsWindow() {}

// NamedWindows represents a slice of NamedWindows.
type NamedWindows []NamedWindow

// WriteSQL imeplements the SQLWriter interface.
func (ws NamedWindows) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	var err error
	for i, window := range ws {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(window.Name + " AS ")
		err = window.Definition.WriteSQL(ctx, dialect, buf, args, params)
		if err != nil {
			return fmt.Errorf("window #%d: %w", i+1, err)
		}
	}
	return nil
}

// CountOver represents the COUNT(<field>) OVER (<window>) window function.
func CountOver(field Field, window Window) Expression {
	if window == nil {
		return Expr("COUNT({}) OVER ()", field)
	}
	return Expr("COUNT({}) OVER {}", field, window)
}

// CountStarOver represents the COUNT(*) OVER (<window>) window function.
func CountStarOver(window Window) Expression {
	if window == nil {
		return Expr("COUNT(*) OVER ()")
	}
	return Expr("COUNT(*) OVER {}", window)
}

// SumOver represents the SUM(<num>) OVER (<window>) window function.
func SumOver(num Number, window Window) Expression {
	if window == nil {
		return Expr("SUM({}) OVER ()", num)
	}
	return Expr("SUM({}) OVER {}", num, window)
}

// AvgOver represents the AVG(<num>) OVER (<window>) window function.
func AvgOver(num Number, window Window) Expression {
	if window == nil {
		return Expr("AVG({}) OVER ()", num)
	}
	return Expr("AVG({}) OVER {}", num, window)
}

// MinOver represents the MIN(<field>) OVER (<window>) window function.
func MinOver(field Field, window Window) Expression {
	if window == nil {
		return Expr("MIN({}) OVER ()", field)
	}
	return Expr("MIN({}) OVER {}", field, window)
}

// MaxOver represents the MAX(<field>) OVER (<window>) window function.
func MaxOver(field Field, window Window) Expression {
	if window == nil {
		return Expr("MAX({}) OVER ()", field)
	}
	return Expr("MAX({}) OVER {}", field, window)
}

// RowNumberOver represents the ROW_NUMBER() OVER (<window>) window function.
func RowNumberOver(window Window) Expression {
	if window == nil {
		return Expr("ROW_NUMBER() OVER ()")
	}
	return Expr("ROW_NUMBER() OVER {}", window)
}

// RankOver represents the RANK() OVER (<window>) window function.
func RankOver(window Window) Expression {
	if window == nil {
		return Expr("RANK() OVER ()")
	}
	return Expr("RANK() OVER {}", window)
}

// DenseRankOver represents the DENSE_RANK() OVER (<window>) window function.
func DenseRankOver(window Window) Expression {
	if window == nil {
		return Expr("DENSE_RANK() OVER ()")
	}
	return Expr("DENSE_RANK() OVER {}", window)
}

// CumeDistOver represents the CUME_DIST() OVER (<window>) window function.
func CumeDistOver(window Window) Expression {
	if window == nil {
		return Expr("CUME_DIST() OVER ()")
	}
	return Expr("CUME_DIST() OVER {}", window)
}

// FirstValueOver represents the FIRST_VALUE(<field>) OVER (<window>) window function.
func FirstValueOver(field Field, window Window) Expression {
	if window == nil {
		return Expr("FIRST_VALUE({}) OVER ()", field)
	}
	return Expr("FIRST_VALUE({}) OVER {}", field, window)
}

// LastValueOver represents the LAST_VALUE(<field>) OVER (<window>) window
// function.
func LastValueOver(field Field, window Window) Expression {
	if window == nil {
		return Expr("LAST_VALUE({}) OVER ()", field)
	}
	return Expr("LAST_VALUE({}) OVER {}", field, window)
}
