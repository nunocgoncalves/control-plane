import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import axe from "axe-core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";

const item = {
  id: "item-1", workflowKey: "walter/quotation", title: "Quotation request — ACME", currentAttemptId: "attempt-1", state: "done",
  source: { kind: "outlook", title: "Quote — ACME", subtitle: "buyer@acme.example", evidence: [{ label: { en: "Customer", pt: "Cliente" }, value: "ACME" }] },
  presentation: { workflowTitle: "Quotation processing", personaName: "Marco", locale: "en" }, createdAt: "2026-08-05T09:00:00Z", updatedAt: "2026-08-05T09:05:00Z", finishedAt: "2026-08-05T09:05:00Z",
  valueConfigured: true, estimatedValue: "10.00", valueCurrency: "EUR", valueDisputed: false,
};
const summary = { counts: { todo: 0, in_progress: 0, blocked: 0, done: 1, failed: 0 }, value: { configured: true, estimated: true, totals: [{ amount: "10", currency: "EUR" }], models: [{ formula: "labor_time_saved", currency: "EUR", baselineSeconds: 1200, loadedHourlyCost: "30", explanation: { en: "Customer assumptions", pt: "Pressupostos do cliente" } }] }, trend: [{ date: "2026-08-05", amount: "10", currency: "EUR" }] };

function json(data: unknown, status = 200) { return Promise.resolve(new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } })); }
function fetchMock(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const url = String(input);
  if (url.startsWith("/v1/work-events")) return new Promise((_resolve, reject) => init?.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError"))));
  if (url.startsWith("/v1/work-dashboard")) return json(summary);
  if (url.startsWith("/v1/work-items?") ) return json([item]);
  if (url === "/v1/work-items/item-1") return json(item);
  if (url.endsWith("/attempts")) return json([{ id: "attempt-1", workItemId: "item-1", number: 1, definitionKey: "walter/quotation", definitionVersion: "1", createdAt: item.createdAt, finishedAt: item.finishedAt }]);
  if (url.includes("/work-attempts/")) return json([{ id: "node-1", attemptId: "attempt-1", nodeKey: "process", businessLabel: { en: "Processing quotation", pt: "A processar cotação" }, visit: 1, executionSeq: 1, kind: "agent_task", state: "succeeded", summary: "Quotation processed", output: { classification: "pricing" }, createdAt: item.createdAt }]);
  if (url.endsWith("/timeline?limit=1000")) return json([{ cursor: 1, id: "event-1", workItemId: "item-1", attemptId: "attempt-1", nodeExecutionId: "node-1", code: "work_completed", params: {}, createdAt: item.finishedAt }]);
  if (url.endsWith("/blocker")) return json({ error: "not found" }, 404);
  if (url.endsWith("/feedback") && init?.method !== "POST") return json([]);
  if (url.endsWith("/feedback") && init?.method === "POST") return json({ id: "feedback-1", workItemId: "item-1", attemptId: "attempt-1", category: "poor_output", createdAt: new Date().toISOString() }, 201);
  if (url.endsWith("/revisions") && init?.method === "POST") return json({ ...item, state: "in_progress" }, 201);
  if (url.endsWith("/consequences")) return json([]);
  if (url.endsWith("/artifacts")) return json([]);
  throw new Error(`unexpected fetch ${url}`);
}

async function connect() {
  const user = userEvent.setup();
  render(<App />);
  await user.type(screen.getByLabelText("Work API key"), "work_secret");
  await user.click(screen.getByRole("button", { name: "Connect" }));
  await screen.findByText("Quotation request — ACME");
  return user;
}

describe("Platform v1 Dashboard", () => {
  beforeEach(() => { vi.stubGlobal("fetch", vi.fn(fetchMock)); });
  afterEach(() => vi.unstubAllGlobals());

  it("keeps the key in memory, renders the single-workflow board, and excludes deferred navigation", async () => {
    await connect();
    expect(screen.getByText("Estimated business value created")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "To do" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Done" })).toBeInTheDocument();
    expect(screen.queryByText("Workflows")).not.toBeInTheDocument();
    expect(document.cookie).not.toContain("work_secret");
    const calls = vi.mocked(fetch).mock.calls;
    expect(calls.some(([, init]) => (init?.headers as Record<string, string>)?.Authorization === "Bearer work_secret")).toBe(true);
  });

  it("presents Portuguese customer labels", async () => {
    const user = userEvent.setup(); render(<App />);
    await user.click(screen.getByRole("button", { name: "PT" }));
    expect(screen.getByRole("heading", { name: "Ligar ao Painel" })).toBeInTheDocument();
    expect(screen.getByLabelText("Chave da API de trabalho")).toBeInTheDocument();
  });

  it("inspects a completed outcome and saves feedback without starting a revision", async () => {
    const user = await connect();
    await user.click(screen.getByText("Quotation request — ACME"));
    expect(await screen.findByText("Quotation processed")).toBeInTheDocument();
    expect(screen.getByText("pricing")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Result needs improvement/ }));
    await user.click(screen.getByRole("button", { name: "Poor output" }));
    await user.type(screen.getByPlaceholderText(/Explanation/), "Wrong extracted total");
    await user.click(screen.getByRole("button", { name: "Save feedback" }));
    await waitFor(() => expect(screen.getByText(/without starting new work/)).toBeInTheDocument());
    const post = vi.mocked(fetch).mock.calls.find(([url, init]) => String(url).endsWith("/feedback") && init?.method === "POST");
    expect(post).toBeTruthy();
    expect(vi.mocked(fetch).mock.calls.some(([url]) => String(url).endsWith("/revisions"))).toBe(false);
  });

  it("requires exact consequence confirmation before starting a revised attempt", async () => {
    const consequence = { invocationId: "invocation-1", summary: { en: "Send the quotation email again", pt: "Enviar novamente o email da cotação" }, state: "succeeded" };
    vi.mocked(fetch).mockImplementation((input, init) => String(input).endsWith("/consequences") ? json([consequence]) : fetchMock(input, init));
    const user = await connect();
    await user.click(screen.getByText("Quotation request — ACME"));
    await screen.findByText("Quotation processed");
    await user.click(screen.getByRole("button", { name: /Result needs improvement/ }));
    await user.click(screen.getByRole("button", { name: "Try again with feedback" }));
    await user.type(screen.getByPlaceholderText(/Actionable guidance/), "Use the corrected classification rule");
    const start = screen.getByRole("button", { name: "Start revised attempt" });
    expect(start).toBeDisabled();
    await user.click(screen.getByText("Send the quotation email again"));
    expect(start).toBeEnabled();
    await user.click(start);
    await waitFor(() => expect(screen.getByText(/original history is preserved/)).toBeInTheDocument());
    const revision = vi.mocked(fetch).mock.calls.find(([url, init]) => String(url).endsWith("/revisions") && init?.method === "POST");
    expect(revision?.[1]?.body).toContain("invocation-1");
  });

  it("uploads and attaches a file when resolving an artifact blocker", async () => {
    const blocked = { ...item, id: "item-blocked", currentAttemptId: "attempt-blocked", state: "blocked", blocker: { id: "blocker-1", kind: "artifact", title: { en: "Signed agreement required", pt: "Acordo assinado necessário" } } };
    const blocker = { id: "blocker-1", workItemId: blocked.id, attemptId: blocked.currentAttemptId, kind: "artifact", title: blocked.blocker.title, description: { en: "Upload the signed agreement", pt: "Carregue o acordo assinado" }, responseSchema: { type: "object", properties: { note: { type: "string", title: "Note" } } }, allowedOutcomes: ["provided"], state: "open" };
    vi.mocked(fetch).mockImplementation((input, init) => {
      const url = String(input);
      if (url.startsWith("/v1/work-items?") ) return json([blocked]);
      if (url === "/v1/work-items/item-blocked") return json(blocked);
      if (url.includes("attempt-blocked") && url.includes("/nodes")) return json([]);
      if (url.endsWith("/blocker")) return json(blocker);
      if (url === "/v1/artifacts" && init?.method === "POST") return json({ id: "artifact-1" }, 201);
      if (url.includes("/work-blockers/blocker-1/responses")) return json({ ...blocker, state: "resolved" });
      return fetchMock(input, init);
    });
    const user = await connect();
    await user.click(screen.getByText("Quotation request — ACME"));
    expect(await screen.findByRole("heading", { name: "Signed agreement required" })).toBeInTheDocument();
    const file = new File(["signed"], "agreement.pdf", { type: "application/pdf" });
    await user.upload(screen.getByLabelText("Choose the required artifact"), file);
    const send = screen.getByRole("button", { name: "Send response" });
    await waitFor(() => expect(send).toBeEnabled());
    await user.click(send);
    await waitFor(() => expect(vi.mocked(fetch).mock.calls.some(([url]) => String(url).includes("/work-blockers/blocker-1/responses"))).toBe(true));
    const response = vi.mocked(fetch).mock.calls.find(([url]) => String(url).includes("/work-blockers/blocker-1/responses"));
    expect(response?.[1]?.body).toContain("artifact-1");
  });

  it("has no automatically detectable accessibility violations on the Dashboard", async () => {
    await connect();
    const results = await axe.run(document.body, { rules: { "color-contrast": { enabled: false } } });
    expect(results.violations).toEqual([]);
  });
});
