package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	artifactstore "github.com/nunocgoncalves/control-plane/internal/artifact"
	"github.com/nunocgoncalves/control-plane/internal/identity"
)

func (h *Handler) artifactActor(r *http.Request) (identity.Identity, bool) {
	v, ok := r.Context().Value(ctxIdentity).(identity.Identity)
	return v, ok
}

func (h *Handler) authorizeArtifact(w http.ResponseWriter, r *http.Request, action string) bool {
	if h.artifacts == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "artifact service not configured"})
		return false
	}
	actor, ok := h.artifactActor(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "missing identity"})
		return false
	}
	if h.perms == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "permissions not configured"})
		return false
	}
	caps, err := h.perms.EffectiveCapabilities(r.Context(), actor.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "evaluating permission"})
		return false
	}
	for _, c := range caps {
		if (c.Resource == "*" || c.Resource == "artifact") && (c.Action == "*" || c.Action == action) {
			return true
		}
	}
	writeJSON(w, http.StatusForbidden, errorBody{Error: "artifact:" + action + " permission required"})
	return false
}

// uploadArtifact accepts a raw streaming body. Content-Type is required;
// Content-Length and X-Artifact-SHA256 are optional integrity claims.
func (h *Handler) uploadArtifact(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeArtifact(w, r, "write") {
		return
	}
	actor, _ := h.artifactActor(r)
	mimeType := r.Header.Get("Content-Type")
	var expectedSize *int64
	if r.ContentLength >= 0 {
		n := r.ContentLength
		expectedSize = &n
	}
	a, err := h.artifacts.Upload(r.Context(), artifactstore.UploadInput{
		SourceType: artifactstore.SourceUserUpload, SourceRef: r.Header.Get("X-Artifact-Source"),
		CreatedByIdentityID: actor.ID, MIMEType: mimeType, ExpectedSize: expectedSize,
		ExpectedDigest: r.Header.Get("X-Artifact-SHA256"),
	}, r.Body)
	if err != nil {
		writeArtifactError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (h *Handler) getArtifact(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeArtifact(w, r, "read") {
		return
	}
	a, body, err := h.artifacts.Open(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeArtifactError(w, err)
		return
	}
	defer body.Close()
	setArtifactHeaders(w, a)
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, body); err != nil {
		// Response status is already committed; cancellation/truncation is the
		// only safe signal. Never replace a partial artifact with JSON.
		return
	}
}

func (h *Handler) headArtifact(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeArtifact(w, r, "read") {
		return
	}
	a, err := h.artifacts.Stat(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeArtifactError(w, err)
		return
	}
	if a.State != artifactstore.StateAvailable {
		writeArtifactError(w, artifactstore.ErrNotAvailable)
		return
	}
	setArtifactHeaders(w, a)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteArtifact(w http.ResponseWriter, r *http.Request) {
	if h.artifacts == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "artifact service not configured"})
		return
	}
	if _, err := h.artifacts.Delete(r.Context(), chi.URLParam(r, "id"), "admin_explicit"); err != nil {
		writeArtifactError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func setArtifactHeaders(w http.ResponseWriter, a artifactstore.Artifact) {
	w.Header().Set("Content-Type", a.MIMEType)
	w.Header().Set("X-Artifact-ID", a.ID)
	w.Header().Set("X-Artifact-State", a.State)
	if a.SizeBytes != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*a.SizeBytes, 10))
	}
	if a.Digest != nil {
		w.Header().Set("X-Artifact-SHA256", *a.Digest)
	}
	if a.RetentionUntil != nil {
		w.Header().Set("X-Artifact-Retention-Until", a.RetentionUntil.Format("2006-01-02T15:04:05Z07:00"))
	}
}

func writeArtifactError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	message := "artifact operation failed"
	switch {
	case errors.Is(err, artifactstore.ErrNotFound):
		status, message = http.StatusNotFound, "artifact not found"
	case errors.Is(err, artifactstore.ErrNotAvailable):
		status, message = http.StatusGone, "artifact is not available"
	case errors.Is(err, artifactstore.ErrUnauthorized):
		status, message = http.StatusForbidden, "artifact access denied"
	case errors.Is(err, artifactstore.ErrInvalidInput), errors.Is(err, artifactstore.ErrDigest), errors.Is(err, artifactstore.ErrSize):
		status, message = http.StatusBadRequest, err.Error()
	case errors.Is(err, artifactstore.ErrTooLarge):
		status, message = http.StatusRequestEntityTooLarge, err.Error()
	}
	writeJSON(w, status, errorBody{Error: fmt.Sprint(message)})
}
