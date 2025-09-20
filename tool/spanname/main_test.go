package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis"
)

func runAnalyzer(t *testing.T, src string) []analysis.Diagnostic {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "a.go", src, parser.ParseComments)
	require.NoError(t, err)

	var diags []analysis.Diagnostic
	pass := &analysis.Pass{
		Fset:  fset,
		Files: []*ast.File{file},
		Report: func(d analysis.Diagnostic) {
			diags = append(diags, d)
		},
	}
	_, err = Analyzer.Run(pass)
	require.NoError(t, err)
	return diags
}

func TestAnalyzer(t *testing.T) {
	t.Parallel()
	src := `package a
import "context"
var tracer struct{ Start func(context.Context, string) }

func Good() { tracer.Start(context.Background(), "Good") }

func Bad() { tracer.Start(context.Background(), "Nope") }

type S struct{}
func (s *S) MethodPointerGood() { tracer.Start(context.Background(), "S.MethodPointerGood") }
func (s *S) MethodPointerBad() { tracer.Start(context.Background(), "MethodPointerBad") }

type T struct{}
func (t T) MethodValueGood() { tracer.Start(context.Background(), "T.MethodValueGood") }
func (t T) MethodValueBad() { tracer.Start(context.Background(), "MethodValueBad") }

// spanname:ignore
func Ignored() { tracer.Start(context.Background(), "Whatever") }
`
	diags := runAnalyzer(t, src)
	require.Len(t, diags, 3)
	want := []string{
		"span name \"Nope\" does not match function \"Bad\"",
		"span name \"MethodPointerBad\" does not match function \"S.MethodPointerBad\"",
		"span name \"MethodValueBad\" does not match function \"T.MethodValueBad\"",
	}
	got := make([]string, 0, len(diags))
	for _, d := range diags {
		got = append(got, d.Message)
	}
	require.ElementsMatch(t, want, got)
}
