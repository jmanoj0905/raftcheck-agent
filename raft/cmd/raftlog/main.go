// Command raftlog prints a JSONL trace (a file argument, or stdin) as one
// readable line per record: [n2 t5 LEADER] become_leader votes=[1 2]
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// FormatLine renders one record. Detail keys are sorted.
func FormatLine(r tracelog.Record) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[n%d t%d %s] %s", r.Node, r.Term, strings.ToUpper(r.State.Role), r.Event)
	keys := make([]string, 0, len(r.Details))
	for k := range r.Details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%s", k, formatValue(r.Details[k]))
	}
	return b.String()
}

func formatValue(v any) string {
	switch x := v.(type) {
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = formatValue(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	default:
		return fmt.Sprint(v)
	}
}

func run(in io.Reader, out io.Writer) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r tracelog.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return fmt.Errorf("line %d: %w", n, err)
		}
		if _, err := fmt.Fprintln(out, FormatLine(r)); err != nil {
			return err
		}
	}
	return sc.Err()
}

func main() {
	in := io.Reader(os.Stdin)
	if len(os.Args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: raftlog [file.jsonl]")
		os.Exit(2)
	}
	if len(os.Args) == 2 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "raftlog:", err)
			os.Exit(1)
		}
		defer f.Close()
		in = f
	}
	if err := run(in, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "raftlog:", err)
		os.Exit(1)
	}
}
