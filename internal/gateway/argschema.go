package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// validateArguments validates invocation arguments against the pinned
// descriptor's JSON Schema before the side-effect boundary (REQ-010/ARCH-014).
// It enforces, in order: the inline size limit, JSON syntax, and the schema.
// An empty/absent schema (nil or `{}`) is treated as "any JSON object" — the
// descriptor is still required to declare one at registration, but a permissive
// schema does not block v1 tools that have no field constraints.
//
// Schema validation is part of the v1 security boundary: schema-invalid or
// forbidden business fields must not reach the runner (REQ-010).
func validateArguments(args []byte, schema []byte, limit int) error {
	if len(args) > limit {
		return fmt.Errorf("arguments exceed inline limit %d (use artifact refs)", limit)
	}
	if len(args) == 0 {
		// No arguments. Allowed only if the schema does not require fields; an
		// empty object is the canonical form, so normalize.
		args = []byte("{}")
	}
	var v interface{}
	if err := json.Unmarshal(args, &v); err != nil {
		return fmt.Errorf("arguments are not valid JSON: %w", err)
	}
	if len(schema) == 0 || bytes.Equal(bytes.TrimSpace(schema), []byte("{}")) {
		return nil // permissive schema; syntax + size already checked
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("input.json", bytes.NewReader(schema)); err != nil {
		return fmt.Errorf("load input_schema: %w", err)
	}
	sch, err := c.Compile("input.json")
	if err != nil {
		// A descriptor with an un-compilable schema is a registration defect;
		// fail closed rather than dispatch unvalidated arguments.
		return fmt.Errorf("compile input_schema: %w", err)
	}
	if err := sch.Validate(v); err != nil {
		// santhosh-tekuri returns a *jsonschema.ValidationError; flatten its
		// message for the caller. The full tree is preserved in the cause.
		return fmt.Errorf("arguments fail input_schema validation: %w", err)
	}
	return nil
}
