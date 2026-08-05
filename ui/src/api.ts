import type { Blocker, Consequence, DashboardSummary, DetailData, Feedback, NodeExecution, Attempt, TimelineEvent, WorkArtifact, WorkItem } from "./types";

export class APIError extends Error {
  constructor(public status: number, message: string) { super(message); }
}

async function request<T>(token: string, path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, ...(init.body && !(init.body instanceof Blob) ? { "Content-Type": "application/json" } : {}), ...init.headers },
  });
  if (!response.ok) {
    let message = response.statusText;
    try { message = (await response.json() as { error?: string }).error || message; } catch { /* empty response */ }
    throw new APIError(response.status, message);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export interface Period { from: Date; to: Date }
const periodParams = (period: Period) => new URLSearchParams({ from: period.from.toISOString(), to: period.to.toISOString() });

export async function loadDashboard(token: string, period: Period, search = ""): Promise<{ summary: DashboardSummary; items: WorkItem[] }> {
  const summaryParams = periodParams(period);
  const itemParams = periodParams(period);
  itemParams.set("limit", "200");
  if (search.trim()) itemParams.set("search", search.trim());
  const [summary, items] = await Promise.all([
    request<DashboardSummary>(token, `/v1/work-dashboard?${summaryParams}`),
    request<WorkItem[]>(token, `/v1/work-items?${itemParams}`),
  ]);
  return { summary, items };
}

async function optional<T>(promise: Promise<T>): Promise<T | null> {
  try { return await promise; } catch (error) { if (error instanceof APIError && error.status === 404) return null; throw error; }
}

export async function loadDetail(token: string, item: WorkItem): Promise<DetailData> {
  const id = encodeURIComponent(item.id);
  const [fresh, attempts, timeline, blocker, feedback, consequences, artifacts] = await Promise.all([
    request<WorkItem>(token, `/v1/work-items/${id}`),
    request<Attempt[]>(token, `/v1/work-items/${id}/attempts`),
    request<TimelineEvent[]>(token, `/v1/work-items/${id}/timeline?limit=1000`),
    optional(request<Blocker>(token, `/v1/work-items/${id}/blocker`)),
    request<Feedback[]>(token, `/v1/work-items/${id}/feedback`),
    request<Consequence[]>(token, `/v1/work-items/${id}/consequences`),
    request<WorkArtifact[]>(token, `/v1/work-items/${id}/artifacts`),
  ]);
  const nodeGroups = await Promise.all(attempts.map((attempt) => request<NodeExecution[]>(token, `/v1/work-attempts/${encodeURIComponent(attempt.id)}/nodes`)));
  return { item: fresh, attempts, timeline, blocker, feedback, consequences, artifacts, nodes: nodeGroups.flat() };
}

export async function respondToBlocker(token: string, blocker: Blocker, response: Record<string, unknown>, artifactRefs: Array<{ artifactId: string; metadata?: unknown }> = []): Promise<void> {
  await request(token, `/v1/work-blockers/${encodeURIComponent(blocker.id)}/responses`, {
    method: "POST",
    body: JSON.stringify({
      outcome: blocker.allowedOutcomes[0], response, artifactRefs,
      confirmedInvocationIds: blocker.kind === "consequence_confirmation" ? (blocker.requiredConsequences || []).map((c) => c.invocationId) : undefined,
    }),
  });
}

export function saveFeedback(token: string, item: WorkItem, category: string, explanation: string): Promise<Feedback> {
  return request(token, `/v1/work-items/${encodeURIComponent(item.id)}/feedback`, {
    method: "POST", body: JSON.stringify({ attemptId: item.currentAttemptId, category, explanation: explanation || undefined }),
  });
}

export function createRevision(token: string, item: WorkItem, feedbackId: string, guidance: string, consequences: Consequence[]): Promise<WorkItem> {
  return request(token, `/v1/work-items/${encodeURIComponent(item.id)}/revisions`, {
    method: "POST", body: JSON.stringify({ feedbackId, actionableGuidance: guidance, confirmedInvocationIds: consequences.map((c) => c.invocationId) }),
  });
}

export async function uploadArtifact(token: string, file: File): Promise<{ id: string }> {
  const response = await fetch("/v1/artifacts", { method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": file.type || "application/octet-stream", "X-Artifact-Source": file.name }, body: file });
  if (!response.ok) throw new APIError(response.status, (await response.text()) || response.statusText);
  return response.json() as Promise<{ id: string }>;
}

export async function downloadArtifact(token: string, artifact: WorkArtifact): Promise<void> {
  const response = await fetch(`/v1/artifacts/${encodeURIComponent(artifact.artifactId)}`, { headers: { Authorization: `Bearer ${token}` } });
  if (!response.ok) throw new APIError(response.status, response.statusText);
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = typeof artifact.metadata?.name === "string" ? artifact.metadata.name : `artifact-${artifact.artifactId}`;
  link.click();
  URL.revokeObjectURL(url);
}

// EventSource cannot carry the work bearer key. This fetch-based SSE reader
// preserves the existing resumable contract without putting credentials in a
// URL or browser storage.
export function subscribe(token: string, onEvent: () => void): () => void {
  const controller = new AbortController();
  let cursor = "";
  const run = async () => {
    while (!controller.signal.aborted) {
      try {
        const response = await fetch("/v1/work-events", { headers: { Authorization: `Bearer ${token}`, ...(cursor ? { "Last-Event-ID": cursor } : {}) }, signal: controller.signal });
        if (!response.ok || !response.body) throw new APIError(response.status, response.statusText);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (!controller.signal.aborted) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let boundary = buffer.indexOf("\n\n");
          while (boundary >= 0) {
            const frame = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2);
            const id = frame.split("\n").find((line) => line.startsWith("id: "))?.slice(4);
            if (id) { cursor = id; onEvent(); }
            boundary = buffer.indexOf("\n\n");
          }
        }
      } catch (error) {
        if (controller.signal.aborted) return;
        await new Promise((resolve) => setTimeout(resolve, 1500));
      }
    }
  };
  void run();
  return () => controller.abort();
}
