// Package schemacheck validates JSONL records against tracelog's schema.
package schemacheck

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	once   sync.Once
	schema *jsonschema.Schema
	errCmp error
)

func compiled() (*jsonschema.Schema, error) {
	once.Do(func() {
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(tracelog.SchemaJSON))
		if err != nil {
			errCmp = err
			return
		}
		c := jsonschema.NewCompiler()
		if err := c.AddResource("schema.json", doc); err != nil {
			errCmp = err
			return
		}
		schema, errCmp = c.Compile("schema.json")
	})
	return schema, errCmp
}

// ValidateLine checks one JSON record.
func ValidateLine(line []byte) error {
	s, err := compiled()
	if err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(line))
	if err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	return s.Validate(inst)
}

// ValidateJSONL checks every non-empty line; errors name the 1-based line.
func ValidateJSONL(data []byte) error {
	for i, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if err := ValidateLine(line); err != nil {
			return fmt.Errorf("line %d: %w", i+1, err)
		}
	}
	return nil
}
