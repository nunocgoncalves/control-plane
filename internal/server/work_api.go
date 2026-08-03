package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/nunocgoncalves/control-plane/internal/identity"
	workstore "github.com/nunocgoncalves/control-plane/internal/work"
)

func (h *Handler) workReady(w http.ResponseWriter) bool {
	if h.work == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "work service not configured"})
		return false
	}
	return true
}

func (h *Handler) workActor(r *http.Request) (identity.Identity, bool) {
	v, ok := r.Context().Value(ctxIdentity).(identity.Identity)
	return v, ok
}

func (h *Handler) authorizeWork(w http.ResponseWriter, r *http.Request, action string) bool {
	if !h.workReady(w) {
		return false
	}
	actor, ok := h.workActor(r)
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
		if (c.Resource == "*" || c.Resource == "work") && (c.Action == "*" || c.Action == action) {
			return true
		}
	}
	writeJSON(w, http.StatusForbidden, errorBody{Error: "work:" + action + " permission required"})
	return false
}

type startWorkRequest struct {
	WorkflowKey     string                  `json:"workflowKey"`
	WorkflowVersion string                  `json:"workflowVersion,omitempty"`
	Title           string                  `json:"title"`
	Source          json.RawMessage         `json:"source"`
	ArtifactRefs    []workstore.ArtifactRef `json:"artifactRefs,omitempty"`
}

func (h *Handler) startWorkItem(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "start") {
		return
	}
	actor, _ := h.workActor(r)
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "Idempotency-Key is required"})
		return
	}
	var req startWorkRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	item, created, err := h.work.Start(r.Context(), workstore.StartInput{ActorIdentityID: actor.ID, WorkflowKey: req.WorkflowKey, WorkflowVersion: req.WorkflowVersion, IdempotencyKey: key, Title: req.Title, Source: req.Source, ArtifactRefs: req.ArtifactRefs})
	if err != nil {
		writeWorkError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, item)
}

func (h *Handler) listWorkItems(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := h.work.ListWorkItems(r.Context(), r.URL.Query().Get("state"), r.URL.Query().Get("search"), limit)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (h *Handler) getWorkItem(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	item, err := h.work.GetWorkItem(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}
func (h *Handler) listWorkAttempts(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	items, err := h.work.ListAttempts(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (h *Handler) listWorkNodes(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	nodes, err := h.work.ListNodeExecutions(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}
func (h *Handler) listWorkTimeline(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	events, err := h.work.ListTimeline(r.Context(), chi.URLParam(r, "id"), after, limit)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}
func (h *Handler) getWorkConsequences(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	consequences, err := h.work.ConsequencesForItem(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, consequences)
}

func (h *Handler) getWorkBlocker(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	blocker, err := h.work.OpenBlockerForItem(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, blocker)
}

type blockerResponseRequest struct {
	Outcome                string          `json:"outcome"`
	Response               json.RawMessage `json:"response"`
	ConfirmedInvocationIDs []string        `json:"confirmedInvocationIds,omitempty"`
}

func (h *Handler) respondWorkBlocker(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "respond") {
		return
	}
	actor, _ := h.workActor(r)
	var req blockerResponseRequest
	if decodeJSON(r, &req) != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	b, err := h.work.RespondBlocker(r.Context(), workstore.BlockerResponseInput{BlockerID: chi.URLParam(r, "id"), ActorIdentityID: actor.ID, Outcome: req.Outcome, Response: req.Response, ConfirmedInvocationIDs: req.ConfirmedInvocationIDs})
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (h *Handler) listWorkFeedback(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	feedback, err := h.work.ListFeedback(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, feedback)
}

func (h *Handler) getWorkFeedback(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	feedback, err := h.work.GetFeedback(r.Context(), chi.URLParam(r, "id"), chi.URLParam(r, "feedbackID"))
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, feedback)
}

type feedbackRequest struct {
	AttemptID       string          `json:"attemptId"`
	Category        string          `json:"category"`
	Explanation     string          `json:"explanation,omitempty"`
	CorrectedResult json.RawMessage `json:"correctedResult,omitempty"`
}

func (h *Handler) saveWorkFeedback(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "feedback") {
		return
	}
	actor, _ := h.workActor(r)
	var req feedbackRequest
	if decodeJSON(r, &req) != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	f, err := h.work.SaveFeedback(r.Context(), workstore.FeedbackInput{WorkItemID: chi.URLParam(r, "id"), AttemptID: req.AttemptID, ActorIdentityID: actor.ID, Category: req.Category, Explanation: req.Explanation, CorrectedResult: req.CorrectedResult})
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, f)
}

type revisionRequest struct {
	FeedbackID             string   `json:"feedbackId"`
	ActionableGuidance     string   `json:"actionableGuidance"`
	ConfirmedInvocationIDs []string `json:"confirmedInvocationIds,omitempty"`
}

func (h *Handler) createWorkRevision(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "retry") {
		return
	}
	actor, _ := h.workActor(r)
	var req revisionRequest
	if decodeJSON(r, &req) != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	item, err := h.work.CreateRevision(r.Context(), workstore.RevisionInput{WorkItemID: chi.URLParam(r, "id"), ActorIdentityID: actor.ID, FeedbackID: req.FeedbackID, ActionableGuidance: req.ActionableGuidance, ConfirmedInvocationIDs: req.ConfirmedInvocationIDs})
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, item)
}

func (h *Handler) getWorkDashboard(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	now := time.Now().UTC()
	from := now.AddDate(0, 0, -7)
	to := now.Add(time.Second)
	if v := r.URL.Query().Get("from"); v != "" {
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			from = parsed
		} else {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "from must be RFC3339"})
			return
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			to = parsed
		} else {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "to must be RFC3339"})
			return
		}
	}
	summary, err := h.work.Dashboard(r.Context(), from, to)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h *Handler) createValueModel(w http.ResponseWriter, r *http.Request) {
	if !h.workReady(w) {
		return
	}
	var req workstore.ValueModelInput
	if decodeJSON(r, &req) != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid request body"})
		return
	}
	model, err := h.work.CreateValueModel(r.Context(), req)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, model)
}

// streamWorkEvents is a resumable customer-safe SSE stream. Last-Event-ID is
// the durable timeline cursor; polling is a fallback over the same contract.
func (h *Handler) streamWorkEvents(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWork(w, r, "read") {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "streaming unsupported"})
		return
	}
	cursor, _ := strconv.ParseInt(strings.TrimSpace(r.Header.Get("Last-Event-ID")), 10, 64)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	poll := time.NewTicker(time.Second)
	heartbeat := time.NewTicker(15 * time.Second)
	defer poll.Stop()
	defer heartbeat.Stop()
	send := func() error {
		for {
			events, err := h.work.TimelineSince(r.Context(), cursor, 500)
			if err != nil {
				return err
			}
			for _, event := range events {
				data, _ := json.Marshal(event)
				if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Cursor, event.Code, data); err != nil {
					return err
				}
				cursor = event.Cursor
			}
			if len(events) < 500 {
				break
			}
		}
		flusher.Flush()
		return nil
	}
	if err := send(); err != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-poll.C:
			if send() != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeWorkError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workstore.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{Error: err.Error()})
	case errors.Is(err, workstore.ErrConflict), errors.Is(err, workstore.ErrConfirmation):
		writeJSON(w, http.StatusConflict, errorBody{Error: err.Error()})
	case errors.Is(err, workstore.ErrInvalidInput), errors.Is(err, workstore.ErrInvalidTransition):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "work operation failed"})
	}
}
