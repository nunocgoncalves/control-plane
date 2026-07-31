package gateway

import (
	"encoding/json"
	"time"

	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// --- effect class ---

func effectClassToProto(c EffectClass) v1.EffectClass {
	switch c {
	case EffectReadOnly:
		return v1.EffectClass_EFFECT_CLASS_READ_ONLY
	case EffectIdempotentWrite:
		return v1.EffectClass_EFFECT_CLASS_IDEMPOTENT_WRITE
	case EffectNonIdempotentWrite:
		return v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE
	}
	return v1.EffectClass_EFFECT_CLASS_UNSPECIFIED
}

func effectClassFromProto(c v1.EffectClass) EffectClass {
	switch c {
	case v1.EffectClass_EFFECT_CLASS_READ_ONLY:
		return EffectReadOnly
	case v1.EffectClass_EFFECT_CLASS_IDEMPOTENT_WRITE:
		return EffectIdempotentWrite
	case v1.EffectClass_EFFECT_CLASS_NON_IDEMPOTENT_WRITE:
		return EffectNonIdempotentWrite
	}
	return EffectReadOnly // default; registration should set explicitly
}

// --- caller scope ---

func callerScopeFromProto(c v1.CallerScope) CallerScope {
	switch c {
	case v1.CallerScope_CALLER_SCOPE_TURN:
		return CallerScopeTurn
	case v1.CallerScope_CALLER_SCOPE_WORKFLOW_STEP:
		return CallerScopeWorkflowStep
	}
	return CallerScopeTurn
}

// --- invocation state ---

func invocationStateToProto(s InvocationState) v1.InvokeState {
	switch s {
	case InvocationDispatching:
		return v1.InvokeState_INVOKE_STATE_DISPATCHING
	case InvocationRunning:
		return v1.InvokeState_INVOKE_STATE_RUNNING
	case InvocationSucceeded:
		return v1.InvokeState_INVOKE_STATE_SUCCEEDED
	case InvocationFailed:
		return v1.InvokeState_INVOKE_STATE_FAILED
	case InvocationOutcomeUnknown:
		return v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN
	}
	return v1.InvokeState_INVOKE_STATE_UNSPECIFIED
}

// --- descriptor <-> tool version ---

// toolVersionToDescriptor maps a stored ToolVersion to a proto ToolDescriptor.
// Credential slots, artifact capabilities, and idempotency proof are
// gateway-internal (used for authorization/credential resolution and retry
// classification) and are NOT sent to callers or back to the runner — the
// runner already declared them at registration, and the child must not see them.
func toolVersionToDescriptor(tv ToolVersion) *v1.ToolDescriptor {
	return &v1.ToolDescriptor{
		Name:        tv.Name,
		Version:     tv.Version,
		Digest:      tv.Digest,
		Description: tv.Description,
		InputSchema: tv.InputSchema,
		EffectClass: effectClassToProto(tv.EffectClass),
		Timeout:     durationpb.New(time.Duration(tv.TimeoutMS) * time.Millisecond),
	}
}

// descriptorToToolVersion maps a runner-supplied descriptor to a stored
// ToolVersion, marshalling structured fields to JSONB for storage.
func descriptorToToolVersion(d *v1.ToolDescriptor) ToolVersion {
	tv := ToolVersion{
		Name:        d.Name,
		Version:     d.Version,
		Digest:      d.Digest,
		Description: d.Description,
		InputSchema: d.InputSchema,
		EffectClass: effectClassFromProto(d.EffectClass),
	}
	if d.Timeout != nil {
		tv.TimeoutMS = d.Timeout.AsDuration().Milliseconds()
	}
	if tv.TimeoutMS == 0 {
		tv.TimeoutMS = 60000 // default 60s when not set
	}
	if len(d.CredentialSlots) > 0 {
		slots := make([]map[string]any, 0, len(d.CredentialSlots))
		for _, s := range d.CredentialSlots {
			slots = append(slots, map[string]any{
				"name":           s.Name,
				"scheme":         schemeToStorage(s.Scheme),
				"binding_schema": s.BindingSchema,
				"required":       s.Required,
			})
		}
		tv.CredentialSlots, _ = marshalJSON(slots)
	} else {
		tv.CredentialSlots = []byte("[]")
	}
	if d.ArtifactCapabilities != nil {
		tv.ArtifactCapabs, _ = marshalJSON(map[string]any{
			"reads":               d.ArtifactCapabilities.ReadsArtifacts,
			"writes":              d.ArtifactCapabilities.WritesArtifacts,
			"accepted_mime_types": d.ArtifactCapabilities.AcceptedMimeTypes,
		})
	} else {
		tv.ArtifactCapabs = []byte("{}")
	}
	if d.IdempotencyProof != nil {
		tv.IdempotencyProof, _ = marshalJSON(map[string]any{
			"strategy":            d.IdempotencyProof.Strategy,
			"description":         d.IdempotencyProof.Description,
			"upstream_key_header": d.IdempotencyProof.UpstreamKeyHeader,
		})
	}
	return tv
}

func schemeToStorage(s v1.CredentialScheme) CredentialScheme {
	if s == v1.CredentialScheme_CREDENTIAL_SCHEME_OAUTH_CLIENT_CREDENTIALS {
		return CredOAuthClientCredentials
	}
	return CredBearer
}

// --- invocation <-> response ---

// invocationToResponse maps a ledger row to a caller response. A non-terminal
// invocation (dispatching/running) is reported as in_progress with the existing
// invocation id (ARCH-014 duplicate handling).
func invocationToResponse(inv Invocation) *v1.InvokeResponse {
	resp := &v1.InvokeResponse{InvocationId: inv.ID, ArtifactOutputRefs: parseArtifactRefs(inv.ArtifactOutputRefs)}
	switch inv.State {
	case InvocationSucceeded:
		resp.State = v1.InvokeState_INVOKE_STATE_SUCCEEDED
		resp.ResultJson = inv.ResultJSON
	case InvocationFailed:
		resp.State = v1.InvokeState_INVOKE_STATE_FAILED
		resp.Error = parseError(inv.Error)
	case InvocationOutcomeUnknown:
		resp.State = v1.InvokeState_INVOKE_STATE_OUTCOME_UNKNOWN
		resp.Error = parseError(inv.Error)
	default: // dispatching / running
		resp.State = v1.InvokeState_INVOKE_STATE_IN_PROGRESS
		resp.ExistingInvocationId = inv.ID
	}
	return resp
}

// invokeResultToDispatch maps a runner-reported InvokeResult to a dispatch
// result for the waiting dispatcher.
func invokeResultToDispatch(r *v1.InvokeResult) dispatchResult {
	out := dispatchResult{artifactRefs: marshalArtifactRefs(r.ArtifactOutputRefs)}
	switch r.State {
	case v1.InvokeState_INVOKE_STATE_SUCCEEDED:
		out.state = InvocationSucceeded
		out.resultJSON = r.ResultJson
	case v1.InvokeState_INVOKE_STATE_FAILED:
		out.state = InvocationFailed
		out.errorDetail = mustMarshalError(r.Error)
	default:
		// The runner reported a non-terminal state as a result; treat as failed
		// with an ambiguous error rather than hanging the dispatcher.
		out.state = InvocationFailed
		out.errorDetail = mustMarshalError(&v1.ErrorDetail{Code: "unexpected_state", Message: "runner reported non-terminal result state"})
	}
	return out
}

// --- artifact refs / error ---

func parseArtifactRefs(b []byte) []*v1.ArtifactRef {
	if len(b) == 0 {
		return nil
	}
	var raw []struct {
		ArtifactID string `json:"artifact_id"`
		MimeType   string `json:"mime_type"`
		SizeBytes  int64  `json:"size_bytes"`
		Digest     string `json:"digest"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil
	}
	out := make([]*v1.ArtifactRef, 0, len(raw))
	for _, r := range raw {
		out = append(out, &v1.ArtifactRef{ArtifactId: r.ArtifactID, MimeType: r.MimeType, SizeBytes: r.SizeBytes, Digest: r.Digest})
	}
	return out
}

func marshalArtifactRefs(refs []*v1.ArtifactRef) []byte {
	if len(refs) == 0 {
		return []byte("[]")
	}
	type ref struct {
		ArtifactID string `json:"artifact_id"`
		MimeType   string `json:"mime_type"`
		SizeBytes  int64  `json:"size_bytes"`
		Digest     string `json:"digest"`
	}
	out := make([]ref, 0, len(refs))
	for _, r := range refs {
		out = append(out, ref{ArtifactID: r.ArtifactId, MimeType: r.MimeType, SizeBytes: r.SizeBytes, Digest: r.Digest})
	}
	b, _ := marshalJSON(out)
	return b
}

func parseError(b []byte) *v1.ErrorDetail {
	if len(b) == 0 {
		return nil
	}
	var e struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		Retryability string `json:"retryability"`
		DetailsJSON  string `json:"details_json"`
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return nil
	}
	return &v1.ErrorDetail{Code: e.Code, Message: e.Message, Retryability: parseRetryability(e.Retryability), DetailsJson: e.DetailsJSON}
}

func mustMarshalError(e *v1.ErrorDetail) []byte {
	if e == nil {
		return []byte("{}")
	}
	b, _ := marshalJSON(map[string]any{
		"code":         e.Code,
		"message":      e.Message,
		"retryability": retryabilityToStorage(e.Retryability),
		"details_json": e.DetailsJson,
	})
	return b
}

func parseRetryability(s string) v1.Retryability {
	switch s {
	case "retryable":
		return v1.Retryability_RETRYABILITY_RETRYABLE
	case "non_retryable":
		return v1.Retryability_RETRYABILITY_NON_RETRYABLE
	case "unknown":
		return v1.Retryability_RETRYABILITY_UNKNOWN
	}
	return v1.Retryability_RETRYABILITY_UNSPECIFIED
}

func retryabilityToStorage(r v1.Retryability) string {
	switch r {
	case v1.Retryability_RETRYABILITY_RETRYABLE:
		return "retryable"
	case v1.Retryability_RETRYABILITY_NON_RETRYABLE:
		return "non_retryable"
	case v1.Retryability_RETRYABILITY_UNKNOWN:
		return "unknown"
	}
	return ""
}
