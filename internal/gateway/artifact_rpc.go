package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	connect "connectrpc.com/connect"
	artifactstore "github.com/nunocgoncalves/control-plane/internal/artifact"
	v1 "github.com/nunocgoncalves/control-plane/internal/gatewayrpc/iterabase/gateway/v1"
	"github.com/nunocgoncalves/control-plane/internal/spiffe"
)

// PutArtifact streams one immutable object. The first message must be init;
// later messages must be chunks. Authorization is resolved before MinIO sees a
// byte and the artifact remains pending until the complete stream verifies.
func (s *Service) PutArtifact(ctx context.Context, st *connect.ClientStream[v1.PutArtifactRequest]) (*connect.Response[v1.PutArtifactResponse], error) {
	if s.artifacts == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("artifact service not configured"))
	}
	if !st.Receive() {
		if err := st.Err(); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("artifact stream is empty"))
	}
	first := st.Msg()
	init := first.GetInit()
	if init == nil || init.Context == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("first artifact message must be init with context"))
	}
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	auth, err := s.authorizeArtifactCaller(ctx, id, init.Context, "")
	if err != nil {
		return nil, mapArtifactErr(err)
	}
	expectedSize := init.ExpectedSizeBytes
	a, err := s.artifacts.Upload(ctx, artifactstore.UploadInput{
		SourceType: auth.sourceType, SourceRef: auth.sourceRef,
		CreatedByIdentityID: auth.creatorIdentityID, MIMEType: init.MimeType,
		ExpectedSize: expectedSize, ExpectedDigest: init.ExpectedDigest,
		Scope: &auth.scope,
	}, &artifactChunkReader{stream: st})
	if err != nil {
		return nil, mapArtifactErr(err)
	}
	return connect.NewResponse(&v1.PutArtifactResponse{Metadata: artifactMetadata(a)}), nil
}

func (s *Service) GetArtifact(ctx context.Context, req *connect.Request[v1.GetArtifactRequest], stream *connect.ServerStream[v1.GetArtifactResponse]) error {
	if s.artifacts == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("artifact service not configured"))
	}
	if req.Msg.Context == nil || req.Msg.ArtifactId == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("context and artifact_id are required"))
	}
	id, ok := identityFromContext(ctx)
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if _, err := s.authorizeArtifactCaller(ctx, id, req.Msg.Context, req.Msg.ArtifactId); err != nil {
		return mapArtifactErr(err)
	}
	a, body, err := s.artifacts.Open(ctx, req.Msg.ArtifactId)
	if err != nil {
		return mapArtifactErr(err)
	}
	defer body.Close()
	if err := stream.Send(&v1.GetArtifactResponse{Kind: &v1.GetArtifactResponse_Metadata{Metadata: artifactMetadata(a)}}); err != nil {
		return err
	}
	buf := make([]byte, 64*1024)
	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if err := stream.Send(&v1.GetArtifactResponse{Kind: &v1.GetArtifactResponse_Chunk{Chunk: chunk}}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return connect.NewError(connect.CodeDataLoss, readErr)
		}
	}
}

func (s *Service) StatArtifact(ctx context.Context, req *connect.Request[v1.StatArtifactRequest]) (*connect.Response[v1.StatArtifactResponse], error) {
	if s.artifacts == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("artifact service not configured"))
	}
	if req.Msg.Context == nil || req.Msg.ArtifactId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("context and artifact_id are required"))
	}
	id, ok := identityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no caller identity"))
	}
	if _, err := s.authorizeArtifactCaller(ctx, id, req.Msg.Context, req.Msg.ArtifactId); err != nil {
		return nil, mapArtifactErr(err)
	}
	a, err := s.artifacts.Stat(ctx, req.Msg.ArtifactId)
	if err != nil {
		return nil, mapArtifactErr(err)
	}
	return connect.NewResponse(&v1.StatArtifactResponse{Metadata: artifactMetadata(a)}), nil
}

type artifactAuthorization struct {
	scope             artifactstore.Scope
	creatorIdentityID string
	sourceType        string
	sourceRef         string
}

//nolint:gocyclo // Three authenticated caller classes intentionally share one fail-closed authorization gate.
func (s *Service) authorizeArtifactCaller(ctx context.Context, id spiffe.Identity, c *v1.ArtifactCallerContext, artifactID string) (artifactAuthorization, error) {
	var auth artifactAuthorization
	if id.Kind == spiffe.KindRunner {
		if c.InvocationId == "" || !s.pool.runnerOwnsInvocation(id.RunnerID, c.InvocationId) {
			return auth, artifactstore.ErrUnauthorized
		}
		inv, err := s.store.GetInvocation(ctx, c.InvocationId)
		if err != nil || inv.State != InvocationRunning {
			return auth, artifactstore.ErrUnauthorized
		}
		if artifactID == "" {
			// PutArtifact is an output capability of the exact immutable tool
			// descriptor pinned on this live invocation, not merely a property of
			// runner ownership.
			tv, err := s.store.GetToolVersion(ctx, inv.ToolName, inv.ToolVersionDigest)
			if err != nil || !toolWritesArtifacts(tv) {
				return auth, artifactstore.ErrUnauthorized
			}
		}
		if artifactID != "" && !containsArtifactRef(inv.ArtifactInputRefs, artifactID) {
			return auth, artifactstore.ErrUnauthorized
		}
		scope, creator, err := s.artifacts.Store().ExecutionScope(ctx, inv.AttemptID, string(inv.CallerScope), inv.CallerScopeID)
		if err != nil {
			return auth, err
		}
		if artifactID != "" {
			ok, err := s.artifacts.Store().LinkedToAttempt(ctx, artifactID, scope.AttemptID, scope.NodeExecutionID)
			if err != nil || !ok {
				return auth, artifactstore.ErrUnauthorized
			}
		}
		scope.Role = "output"
		return artifactAuthorization{scope: scope, creatorIdentityID: creator, sourceType: artifactstore.SourceToolOutput, sourceRef: inv.ID}, nil
	}

	res, err := s.resolveCallerScope(ctx, id, &v1.DiscoverRequest{
		AttemptId: c.AttemptId, CallerScope: c.CallerScope, CallerScopeId: c.CallerScopeId,
		FencingGeneration: c.FencingGeneration,
	})
	if err != nil {
		return auth, err
	}
	scope, creator, err := s.artifacts.Store().ExecutionScope(ctx, res.AttemptID, string(res.CallerScope), res.CallerScopeID)
	if err != nil {
		return auth, err
	}
	if artifactID != "" {
		ok, err := s.artifacts.Store().LinkedToAttempt(ctx, artifactID, scope.AttemptID, scope.NodeExecutionID)
		if err != nil || !ok {
			return auth, artifactstore.ErrUnauthorized
		}
	}
	scope.Role = "output"
	source := artifactstore.SourceWorkflow
	if id.Kind == spiffe.KindSupervisor {
		source = artifactstore.SourceSandboxPublish
	}
	return artifactAuthorization{scope: scope, creatorIdentityID: creator, sourceType: source, sourceRef: res.CallerScopeID}, nil
}

func toolWritesArtifacts(tv ToolVersion) bool {
	var capabilities struct {
		Writes bool `json:"writes"`
	}
	return json.Unmarshal(tv.ArtifactCapabs, &capabilities) == nil && capabilities.Writes
}

func containsArtifactRef(raw []byte, id string) bool {
	var refs []struct {
		ArtifactID string `json:"artifact_id"`
	}
	if json.Unmarshal(raw, &refs) != nil {
		return false
	}
	for _, ref := range refs {
		if ref.ArtifactID == id {
			return true
		}
	}
	return false
}

func artifactMetadata(a artifactstore.Artifact) *v1.ArtifactMetadata {
	m := &v1.ArtifactMetadata{Source: a.SourceType, State: a.State, CreatedAtUnixMs: a.CreatedAt.UnixMilli()}
	if a.SizeBytes != nil && a.Digest != nil {
		m.Ref = &v1.ArtifactRef{ArtifactId: a.ID, MimeType: a.MIMEType, SizeBytes: *a.SizeBytes, Digest: *a.Digest}
	}
	if a.RetentionUntil != nil {
		v := a.RetentionUntil.UnixMilli()
		m.RetentionUntilUnixMs = &v
	}
	if a.DeletedAt != nil {
		v := a.DeletedAt.UnixMilli()
		m.DeletedAtUnixMs = &v
	}
	return m
}

func mapArtifactErr(err error) error {
	switch {
	case errors.Is(err, artifactstore.ErrUnauthorized):
		return connect.NewError(connect.CodePermissionDenied, err)
	case errors.Is(err, artifactstore.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, artifactstore.ErrNotAvailable):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, artifactstore.ErrInvalidInput), errors.Is(err, artifactstore.ErrDigest), errors.Is(err, artifactstore.ErrSize):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, artifactstore.ErrTooLarge):
		return connect.NewError(connect.CodeResourceExhausted, err)
	default:
		return connect.NewError(connect.CodeInternal, fmt.Errorf("artifact operation: %w", err))
	}
}

type artifactChunkReader struct {
	stream *connect.ClientStream[v1.PutArtifactRequest]
	buf    []byte
	done   bool
}

func (r *artifactChunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		if !r.stream.Receive() {
			r.done = true
			if err := r.stream.Err(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		msg := r.stream.Msg()
		chunk, ok := msg.Kind.(*v1.PutArtifactRequest_Chunk)
		if !ok {
			return 0, errors.New("artifact init may appear only as the first stream message")
		}
		r.buf = chunk.Chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
