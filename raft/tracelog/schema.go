package tracelog

import _ "embed"

// SchemaJSON is the JSON Schema (draft 2020-12) for one record.
//
//go:embed schema.json
var SchemaJSON []byte
