package core

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// forbiddenImports are the packages the core must never import (spec §3).
// A path is forbidden if it equals an entry or lies under it ("net/http").
var forbiddenImports = []string{
	"time", "math/rand", "math/rand/v2", "crypto/rand",
	"sync", "sync/atomic", "net", "os",
}

func forbidden(path string) bool {
	for _, f := range forbiddenImports {
		if path == f || strings.HasPrefix(path, f+"/") {
			return true
		}
	}
	return false
}

// TestCoreImportsAndGoroutines is test C0: the core stays deterministic. Only
// non-test files are checked; tests may import whatever they need.
func TestCoreImportsAndGoroutines(t *testing.T) {
	fset := token.NewFileSet()
	for _, f := range parseCoreFiles(t, fset) {
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if forbidden(path) {
				t.Errorf("%s: core must not import %q", fset.Position(imp.Pos()), path)
			}
		}
		ast.Inspect(f, func(node ast.Node) bool {
			if g, ok := node.(*ast.GoStmt); ok {
				t.Errorf("%s: core must not start goroutines", fset.Position(g.Pos()))
			}
			return true
		})
	}
}

// TestCoreNeverRangesOverMap enforces "never range over a map" (spec §4.1):
// Go map order is random, so ranging over one would break determinism. It
// type-checks the non-test files and fails on any range whose operand is a
// map (including named map types).
func TestCoreNeverRangesOverMap(t *testing.T) {
	fset := token.NewFileSet()
	files := parseCoreFiles(t, fset)
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	if _, err := conf.Check("core", fset, files, info); err != nil {
		t.Fatalf("type-checking the core package: %v", err)
	}
	ranges := 0
	for _, f := range files {
		ast.Inspect(f, func(node ast.Node) bool {
			r, ok := node.(*ast.RangeStmt)
			if !ok {
				return true
			}
			ranges++
			tv, ok := info.Types[r.X]
			if !ok {
				t.Errorf("%s: no type for range operand", fset.Position(r.X.Pos()))
				return true
			}
			if _, isMap := tv.Type.Underlying().(*types.Map); isMap {
				t.Errorf("%s: core must not range over a map (%s); loop over n.peers instead",
					fset.Position(r.Pos()), tv.Type)
			}
			return true
		})
	}
	if ranges == 0 {
		t.Fatal("found no range statements; the check is not looking at the core files")
	}
}

// parseCoreFiles parses the package's non-test .go files.
func parseCoreFiles(t *testing.T, fset *token.FileSet) []*ast.File {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatal("no non-test .go files found in the core package")
	}
	return files
}

func TestForbiddenMatchesSubpackages(t *testing.T) {
	for _, p := range []string{"time", "net/http", "os/exec", "sync/atomic", "math/rand/v2"} {
		if !forbidden(p) {
			t.Errorf("forbidden(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"sort", "strconv", "fmt", "osx", "network", "timeutil"} {
		if forbidden(p) {
			t.Errorf("forbidden(%q) = true, want false", p)
		}
	}
}
