package work

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/nunocgoncalves/control-plane/internal/workflow"
)

// GetAssignmentContext resolves the exact immutable context for one graph-node
// execution. No prompt/model/tool scope is inferred by dispatch.
func (s *Store) GetAssignmentContext(ctx context.Context, nodeExecutionID string) (AssignmentContext, error) {
	var out AssignmentContext
	var graphJSON, specJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT a.work_item_id::text, a.id::text, wr.scope_identity_id::text,
		       d.pool_key, d.spec_json, a.graph_snapshot
		FROM runtime.node_executions ne
		JOIN work.attempts a ON a.id=ne.attempt_id
		JOIN runtime.workflow_runs wr ON wr.id=a.id
		JOIN workflow.definitions d ON d.id=a.definition_id
		WHERE ne.id=$1`, nodeExecutionID).
		Scan(&out.WorkItemID, &out.AttemptID, &out.ScopeIdentityID, &out.AgentPoolKey, &specJSON, &graphJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return AssignmentContext{}, ErrNotFound
	}
	if err != nil {
		return AssignmentContext{}, err
	}
	out.Node, err = scanNodeExecution(s.pool.QueryRow(ctx, nodeExecutionSelect+` WHERE id=$1`, nodeExecutionID))
	if err != nil {
		return AssignmentContext{}, err
	}
	var spec workflow.CanonicalSpec
	var graph workflow.CanonicalGraph
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return AssignmentContext{}, err
	}
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return AssignmentContext{}, err
	}
	out.Persona = spec.Presentation.PersonaName
	def, ok := findNode(graph, out.Node.NodeKey)
	if !ok {
		return AssignmentContext{}, ErrNotFound
	}
	out.AllowedOutcomes = def.Outcomes
	out.OutputSchema = def.OutputSchema
	if len(out.OutputSchema) == 0 {
		out.OutputSchema = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(out.Node.SkillsSnapshot, &out.Skills); err != nil {
		return AssignmentContext{}, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tool_name,tool_version_digest FROM toolgateway.attempt_tool_pins
		WHERE attempt_id=$1 ORDER BY tool_name`, out.AttemptID)
	if err != nil {
		return AssignmentContext{}, err
	}
	defer rows.Close()
	pins := make([]map[string]string, 0)
	for rows.Next() {
		var name, digest string
		if err := rows.Scan(&name, &digest); err != nil {
			return AssignmentContext{}, err
		}
		pins = append(pins, map[string]string{"tool": name, "digest": digest})
	}
	out.ToolPins, _ = json.Marshal(pins)
	return out, rows.Err()
}
