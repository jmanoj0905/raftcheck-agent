package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
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
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
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
	if checked == 0 {
		t.Fatal("no non-test .go files found in the core package")
	}
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
