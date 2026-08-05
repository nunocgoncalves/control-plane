package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

const (
	NodeAgentTask = "agent_task"
	NodeHumanGate = "human_gate"
)

// ValidateGraph validates the immutable graph contract used both at reconcile
// and attempt resolution. Cycles are legal; unreachable nodes, uncovered
// outcomes, and graphs with no possible terminal path fail closed.
//
//nolint:gocyclo // Graph validation reports precise operator errors across the complete contract.
func ValidateGraph(spec CanonicalSpec) error {
	g := spec.Graph
	if g.EntryNode == "" {
		return fmt.Errorf("graph.entryNode is required")
	}
	if g.MaxTransitions < 1 || g.MaxTransitions > 10000 {
		return fmt.Errorf("graph.maxTransitions must be between 1 and 10000")
	}
	if len(g.Nodes) == 0 {
		return fmt.Errorf("graph.nodes must not be empty")
	}
	if len(g.TerminalOutcomes) == 0 {
		return fmt.Errorf("graph.terminalOutcomes must not be empty")
	}

	skills := make(map[string]struct{}, len(spec.Skills))
	for _, s := range spec.Skills {
		skills[s.Name] = struct{}{}
	}
	caps := make(map[string]struct{}, len(spec.RequestedCapabilities))
	for _, c := range spec.RequestedCapabilities {
		if c.Tool == "complete_step" {
			return fmt.Errorf("requested capability complete_step is reserved by the platform")
		}
		caps[c.Tool] = struct{}{}
	}

	nodes := make(map[string]CanonicalNode, len(g.Nodes))
	outcomes := make(map[string]map[string]struct{}, len(g.Nodes))
	for i, n := range g.Nodes {
		if n.Key == "" {
			return fmt.Errorf("graph.nodes[%d].key is required", i)
		}
		if _, exists := nodes[n.Key]; exists {
			return fmt.Errorf("graph.nodes[%d].key %q is duplicated", i, n.Key)
		}
		if n.Label.EN == "" || n.Label.PT == "" {
			return fmt.Errorf("graph.nodes[%d].label requires en and pt text", i)
		}
		if len(n.Outcomes) == 0 {
			return fmt.Errorf("graph.nodes[%d].outcomes must not be empty", i)
		}
		out := make(map[string]struct{}, len(n.Outcomes))
		for j, name := range n.Outcomes {
			if name == "" {
				return fmt.Errorf("graph.nodes[%d].outcomes[%d] is empty", i, j)
			}
			if _, exists := out[name]; exists {
				return fmt.Errorf("graph.nodes[%d].outcome %q is duplicated", i, name)
			}
			out[name] = struct{}{}
		}
		outcomes[n.Key] = out

		switch n.Kind {
		case NodeAgentTask:
			if n.Prompt == "" {
				return fmt.Errorf("graph.nodes[%d].prompt is required for agent_task", i)
			}
			if n.HumanGate != nil {
				return fmt.Errorf("graph.nodes[%d].humanGate is forbidden for agent_task", i)
			}
			if n.ModelRef == "" && spec.DefaultModelRef == "" {
				return fmt.Errorf("graph.nodes[%d] requires modelRef or spec.defaultModelRef", i)
			}
			seenNodeSkills := map[string]struct{}{}
			for _, name := range n.Skills {
				if _, duplicate := seenNodeSkills[name]; duplicate {
					return fmt.Errorf("graph.nodes[%d].skill %q is duplicated", i, name)
				}
				seenNodeSkills[name] = struct{}{}
				if _, ok := skills[name]; !ok {
					return fmt.Errorf("graph.nodes[%d].skill %q is not declared by the workflow", i, name)
				}
			}
			seenNodeCaps := map[string]struct{}{}
			for _, name := range n.Capabilities {
				if _, duplicate := seenNodeCaps[name]; duplicate {
					return fmt.Errorf("graph.nodes[%d].capability %q is duplicated", i, name)
				}
				seenNodeCaps[name] = struct{}{}
				if _, ok := caps[name]; !ok {
					return fmt.Errorf("graph.nodes[%d].capability %q is not requested by the workflow", i, name)
				}
			}
			if n.Timeout != "" {
				if timeout, err := time.ParseDuration(n.Timeout); err != nil || timeout <= 0 {
					return fmt.Errorf("graph.nodes[%d].timeout must be a positive duration", i)
				}
			}
			if err := compileSchema(n.OutputSchema); err != nil {
				return fmt.Errorf("graph.nodes[%d].outputSchema: %w", i, err)
			}
		case NodeHumanGate:
			if n.Prompt != "" || n.ModelRef != "" || len(n.Skills) > 0 || len(n.Capabilities) > 0 || n.WorkspaceTools {
				return fmt.Errorf("graph.nodes[%d] human_gate cannot declare agent prompt/model/skills/capabilities/workspaceTools", i)
			}
			if n.OutputSchema != nil {
				return fmt.Errorf("graph.nodes[%d].outputSchema is forbidden for human_gate; use humanGate.responseSchema", i)
			}
			if n.HumanGate == nil {
				return fmt.Errorf("graph.nodes[%d].humanGate is required for human_gate", i)
			}
			switch n.HumanGate.Type {
			case "information", "decision", "approval", "artifact":
			default:
				return fmt.Errorf("graph.nodes[%d].humanGate.type %q is invalid", i, n.HumanGate.Type)
			}
			if n.HumanGate.Title.EN == "" || n.HumanGate.Title.PT == "" {
				return fmt.Errorf("graph.nodes[%d].humanGate.title requires en and pt text", i)
			}
			if n.HumanGate.Description.EN == "" || n.HumanGate.Description.PT == "" {
				return fmt.Errorf("graph.nodes[%d].humanGate.description requires en and pt text", i)
			}
			if err := compileSchema(n.HumanGate.ResponseSchema); err != nil {
				return fmt.Errorf("graph.nodes[%d].humanGate.responseSchema: %w", i, err)
			}
			if err := validateDashboardResponseSchema(n.HumanGate.ResponseSchema); err != nil {
				return fmt.Errorf("graph.nodes[%d].humanGate.responseSchema: %w", i, err)
			}
			if err := validateHumanGatePresentation(n); err != nil {
				return fmt.Errorf("graph.nodes[%d].humanGate.presentation: %w", i, err)
			}
		default:
			return fmt.Errorf("graph.nodes[%d].kind %q is unknown", i, n.Kind)
		}
		nodes[n.Key] = n
	}
	if _, ok := nodes[g.EntryNode]; !ok {
		return fmt.Errorf("graph.entryNode %q does not reference a node", g.EntryNode)
	}

	// A route is uniquely identified by (node,outcome). Every declared outcome
	// must have exactly one edge or terminal route.
	type route struct{ node, outcome string }
	routes := make(map[route]string)
	adj := make(map[string][]string, len(nodes))
	reverse := make(map[string][]string, len(nodes))
	for i, e := range g.Edges {
		if _, ok := nodes[e.From]; !ok {
			return fmt.Errorf("graph.edges[%d].from %q does not reference a node", i, e.From)
		}
		if _, ok := nodes[e.To]; !ok {
			return fmt.Errorf("graph.edges[%d].to %q does not reference a node", i, e.To)
		}
		if _, ok := outcomes[e.From][e.Outcome]; !ok {
			return fmt.Errorf("graph.edges[%d] outcome %q is not declared by node %q", i, e.Outcome, e.From)
		}
		r := route{e.From, e.Outcome}
		if previous, exists := routes[r]; exists {
			return fmt.Errorf("graph outcome %s/%s has multiple routes (%s and %s)", e.From, e.Outcome, previous, e.To)
		}
		routes[r] = e.To
		adj[e.From] = append(adj[e.From], e.To)
		reverse[e.To] = append(reverse[e.To], e.From)
	}
	terminalNodes := make(map[string]struct{})
	for i, t := range g.TerminalOutcomes {
		if _, ok := nodes[t.Node]; !ok {
			return fmt.Errorf("graph.terminalOutcomes[%d].node %q does not reference a node", i, t.Node)
		}
		if _, ok := outcomes[t.Node][t.Outcome]; !ok {
			return fmt.Errorf("graph.terminalOutcomes[%d] outcome %q is not declared by node %q", i, t.Outcome, t.Node)
		}
		r := route{t.Node, t.Outcome}
		if previous, exists := routes[r]; exists {
			return fmt.Errorf("graph outcome %s/%s is both terminal and routed to %s", t.Node, t.Outcome, previous)
		}
		routes[r] = "$terminal"
		terminalNodes[t.Node] = struct{}{}
	}
	for node, declared := range outcomes {
		for outcome := range declared {
			if _, ok := routes[route{node, outcome}]; !ok {
				return fmt.Errorf("graph outcome %s/%s has no edge or terminal declaration", node, outcome)
			}
		}
	}

	reachable := walk(g.EntryNode, adj)
	for key := range nodes {
		if _, ok := reachable[key]; !ok {
			return fmt.Errorf("graph node %q is unreachable from entryNode", key)
		}
	}
	// Reverse-walk from every terminal node. A cycle with no exit is rejected
	// even though the runtime also enforces maxTransitions.
	canTerminate := make(map[string]struct{})
	for key := range terminalNodes {
		for visited := range walk(key, reverse) {
			canTerminate[visited] = struct{}{}
		}
	}
	for key := range nodes {
		if _, ok := canTerminate[key]; !ok {
			return fmt.Errorf("graph node %q has no path to a terminal outcome", key)
		}
	}
	return nil
}

func walk(start string, adj map[string][]string) map[string]struct{} {
	seen := map[string]struct{}{start: {}}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return seen
}

//nolint:gocyclo // The fail-closed Dashboard schema subset validates each allowed shape explicitly.
func validateDashboardResponseSchema(raw json.RawMessage) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("{}")) {
		return nil
	}
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &schema); err != nil {
		return fmt.Errorf("must be a JSON object: %w", err)
	}
	allowed := map[string]struct{}{
		"$comment": {}, "$id": {}, "$schema": {}, "additionalProperties": {},
		"default": {}, "deprecated": {}, "description": {}, "examples": {},
		"maxProperties": {}, "minProperties": {}, "properties": {}, "readOnly": {},
		"required": {}, "title": {}, "type": {}, "writeOnly": {},
	}
	for keyword := range schema {
		if _, ok := allowed[keyword]; !ok {
			return fmt.Errorf("keyword %q is not supported by the v1 Dashboard form", keyword)
		}
	}
	var schemaType string
	if err := json.Unmarshal(schema["type"], &schemaType); err != nil || schemaType != "object" {
		return fmt.Errorf("must declare type object for the v1 Dashboard form")
	}
	properties, err := responseSchemaProperties(raw)
	if err != nil {
		return err
	}
	var required []string
	if value := schema["required"]; len(value) > 0 {
		if err := json.Unmarshal(value, &required); err != nil {
			return fmt.Errorf("required must be an array of property names")
		}
	}
	for _, key := range required {
		if _, ok := properties[key]; !ok {
			return fmt.Errorf("required property %q must be declared directly in properties", key)
		}
	}
	if value := schema["minProperties"]; len(value) > 0 {
		var minimum int
		if err := json.Unmarshal(value, &minimum); err != nil || minimum > len(properties) {
			return fmt.Errorf("minProperties cannot exceed the directly declared property count")
		}
	}
	if value := schema["maxProperties"]; len(value) > 0 {
		var maximum int
		if err := json.Unmarshal(value, &maximum); err != nil || maximum < len(required) {
			return fmt.Errorf("maxProperties cannot be smaller than the required property count")
		}
	}
	for key, property := range properties {
		if err := validateDashboardResponseProperty(key, property); err != nil {
			return err
		}
	}
	return nil
}

func validateDashboardResponseProperty(key string, raw json.RawMessage) error {
	var property map[string]json.RawMessage
	if err := json.Unmarshal(raw, &property); err != nil {
		return fmt.Errorf("property %q must be a JSON object", key)
	}
	for _, keyword := range []string{"$ref", "allOf", "anyOf", "if", "not", "oneOf"} {
		if _, exists := property[keyword]; exists {
			return fmt.Errorf("property %q keyword %q is not supported by the v1 Dashboard form", key, keyword)
		}
	}
	if enum := property["enum"]; len(enum) > 0 {
		var values []json.RawMessage
		if err := json.Unmarshal(enum, &values); err != nil || len(values) == 0 {
			return fmt.Errorf("property %q enum must contain at least one value", key)
		}
		return nil
	}
	var propertyType string
	if err := json.Unmarshal(property["type"], &propertyType); err != nil {
		return fmt.Errorf("property %q must declare a direct type or enum", key)
	}
	switch propertyType {
	case "string", "boolean", "number", "integer", "object", "array":
		return nil
	default:
		return fmt.Errorf("property %q type %q is not supported by the v1 Dashboard form", key, propertyType)
	}
}

func validateHumanGatePresentation(node CanonicalNode) error {
	presentation := node.HumanGate.Presentation
	if len(presentation.Outcomes) != len(node.Outcomes) {
		return fmt.Errorf("outcomes must provide one localized label for each declared outcome")
	}
	if err := validateLocalizedLabels(presentation.Outcomes, "outcomes"); err != nil {
		return err
	}
	properties, err := responseSchemaProperties(node.HumanGate.ResponseSchema)
	if err != nil {
		return err
	}
	fields, err := indexHumanGateFields(presentation.Fields)
	if err != nil {
		return err
	}
	for key := range fields {
		if _, exists := properties[key]; !exists {
			return fmt.Errorf("field %q does not exist in responseSchema.properties", key)
		}
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		field, exists := fields[key]
		if !exists {
			return fmt.Errorf("responseSchema property %q requires localized presentation", key)
		}
		if err := validateHumanGateFieldOptions(key, properties[key], field.Options); err != nil {
			return err
		}
	}
	return nil
}

func validateLocalizedLabels(labels []CanonicalLocalizedText, path string) error {
	for i, label := range labels {
		if label.EN == "" || label.PT == "" {
			return fmt.Errorf("%s[%d] requires en and pt text", path, i)
		}
	}
	return nil
}

func responseSchemaProperties(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("{}")) {
		return nil, nil
	}
	if err := json.Unmarshal(trimmed, &schema); err != nil {
		return nil, fmt.Errorf("read response-schema properties: %w", err)
	}
	return schema.Properties, nil
}

func indexHumanGateFields(fields []CanonicalHumanGateFieldPresentation) (map[string]CanonicalHumanGateFieldPresentation, error) {
	indexed := make(map[string]CanonicalHumanGateFieldPresentation, len(fields))
	for i, field := range fields {
		if field.Key == "" {
			return nil, fmt.Errorf("fields[%d].key is required", i)
		}
		if _, exists := indexed[field.Key]; exists {
			return nil, fmt.Errorf("field %q is duplicated", field.Key)
		}
		if field.Label.EN == "" || field.Label.PT == "" {
			return nil, fmt.Errorf("field %q label requires en and pt text", field.Key)
		}
		indexed[field.Key] = field
	}
	return indexed, nil
}

func validateHumanGateFieldOptions(key string, raw json.RawMessage, labels []CanonicalLocalizedText) error {
	var property struct {
		Enum []json.RawMessage `json:"enum"`
	}
	if err := json.Unmarshal(raw, &property); err != nil {
		return fmt.Errorf("responseSchema property %q must be an object", key)
	}
	if len(labels) != len(property.Enum) {
		return fmt.Errorf("field %q options must provide one localized label for each enum value", key)
	}
	return validateLocalizedLabels(labels, fmt.Sprintf("field %q options", key))
}

func compileSchema(raw []byte) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("{}")) {
		return nil
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", bytes.NewReader(raw)); err != nil {
		return fmt.Errorf("load JSON Schema: %w", err)
	}
	if _, err := compiler.Compile("schema.json"); err != nil {
		return fmt.Errorf("compile JSON Schema: %w", err)
	}
	return nil
}
