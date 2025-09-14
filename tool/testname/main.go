package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"regexp"
	"strings"
)

var testNameRe = regexp.MustCompile(`^Test[A-Z][a-zA-Z0-9]*_[a-zA-Z0-9]+$`)

func main() {
	flag.Parse()
	dirs := flag.Args()
	if len(dirs) == 0 {
		dirs = []string{"."}
	}
	var failed bool
	for _, dir := range dirs {
		names, err := checkDir(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			failed = true
			continue
		}
		for _, name := range names {
			fmt.Fprintf(os.Stderr, "%s does not follow Test<Subject>_<Behavior>\n", name)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

func checkDir(dir string) ([]string, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
		return strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		return nil, fmt.Errorf("parse dir %s: %w", dir, err)
	}
	var bad []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv != nil {
					continue
				}
				if strings.HasPrefix(fn.Name.Name, "Test") && !testNameRe.MatchString(fn.Name.Name) {
					pos := fset.Position(fn.Pos())
					bad = append(bad, fmt.Sprintf("%s:%d: %s", pos.Filename, pos.Line, fn.Name.Name))
				}
			}
		}
	}
	return bad, nil
}
