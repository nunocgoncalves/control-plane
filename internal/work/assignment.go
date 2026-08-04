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
//
//nolint:gocyclo // Assignment assembly validates each immutable snapshot and canonical artifact input fail-closed.
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
	pins := make([]map[string]string, 0)
	for rows.Next() {
		var name, digest string
		if err := rows.Scan(&name, &digest); err != nil {
			rows.Close()
			return AssignmentContext{}, err
		}
		pins = append(pins, map[string]string{"tool": name, "digest": digest})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AssignmentContext{}, err
	}
	rows.Close()
	out.ToolPins, _ = json.Marshal(pins)

	artifactRows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ar.id::text,ar.mime_type,ar.size_bytes,ar.digest
		FROM work.artifact_links l
		JOIN artifact.artifacts ar ON ar.id=l.artifact_id
		WHERE l.attempt_id=$1 AND ar.state='available'
		ORDER BY ar.id::text`, out.AttemptID)
	if err != nil {
		return AssignmentContext{}, err
	}
	defer artifactRows.Close()
	for artifactRows.Next() {
		var m ArtifactMaterialization
		if err := artifactRows.Scan(&m.ArtifactID, &m.MIMEType, &m.SizeBytes, &m.Digest); err != nil {
			return AssignmentContext{}, err
		}
		// Artifact ids are UUIDs, so this deterministic destination contains no
		// customer-controlled path segment.
		m.RelativePath = "inputs/" + m.ArtifactID
		out.Materializations = append(out.Materializations, m)
	}
	return out, artifactRows.Err()
}
