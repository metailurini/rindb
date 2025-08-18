package main

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/singlechecker"
	"golang.org/x/tools/go/ast/inspector"
)

var Analyzer = &analysis.Analyzer{
	Name: "spanname",
	Doc:  "checks that tracer.Start(ctx, \"<func>\") matches the enclosing function name",
	Run: func(pass *analysis.Pass) (any, error) {
		ins := inspector.New(pass.Files)
		nodeFilter := []ast.Node{(*ast.FuncDecl)(nil), (*ast.CallExpr)(nil)}

		var currentFunc *ast.FuncDecl

		ins.Nodes(nodeFilter, func(n ast.Node, push bool) bool {
			switch x := n.(type) {
			case *ast.FuncDecl:
				if push {
					currentFunc = x
				} else {
					currentFunc = nil
				}
			case *ast.CallExpr:
				if !push || currentFunc == nil {
					return true
				}
				// opt-out via comment
				if currentFunc.Doc != nil {
					for _, cg := range currentFunc.Doc.List {
						if strings.Contains(cg.Text, "spanname:ignore") {
							return true
						}
					}
				}
				// match tracer.Start(...)
				sel, ok := x.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel == nil || sel.Sel.Name != "Start" {
					return true
				}
				// (optional) check qualifier package name if you want:
				//   pkgIdent, ok := sel.X.(*ast.Ident); ok && pkgIdent.Name == "tracer"

				if len(x.Args) < 2 {
					return true
				}
				lit, ok := x.Args[1].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				want := currentFunc.Name.Name
				if currentFunc.Recv != nil && len(currentFunc.Recv.List) > 0 {
					recvType := currentFunc.Recv.List[0].Type
					var structName string
					switch t := recvType.(type) {
					case *ast.StarExpr: // Pointer receiver, e.g., *MyStruct
						if ident, ok := t.X.(*ast.Ident); ok {
							structName = ident.Name
						}
					case *ast.Ident: // Value receiver, e.g., MyStruct
						structName = t.Name
					}
					if structName != "" {
						want = structName + "." + currentFunc.Name.Name
					}
				}

				// choose your policy; this example expects just the bare name
				got, _ := strconv.Unquote(lit.Value)
				if got != want {
					pass.Reportf(x.Pos(), "span name %q does not match function %q", got, want)
				}
			}
			return true
		})
		return nil, nil
	},
}

func main() { singlechecker.Main(Analyzer) }
