import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import axe from "axe-core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";

const item = {
  id: "item-1",
  workflowKey: "walter/quotation",
  title: "Quotation request — ACME",
  currentAttemptId: "attempt-1",
  state: "done",
  source: {
    kind: "outlook",
    title: "Quote — ACME",
    subtitle: "buyer@acme.example",
    evidence: [{ label: { en: "Customer", pt: "Cliente" }, value: "ACME" }],
  },
  presentation: {
    workflowTitle: "Quotation processing",
    personaName: "Marco",
    locale: "en",
  },
  createdAt: "2026-08-05T09:00:00Z",
  updatedAt: "2026-08-05T09:05:00Z",
  finishedAt: "2026-08-05T09:05:00Z",
  valueConfigured: true,
  valueModel: {
    formula: "labor_time_saved",
    currency: "EUR",
    baselineSeconds: 1200,
    loadedHourlyCost: "30",
    assumptions: { source: "Customer baseline" },
    explanation: {
      en: "Customer assumptions",
      pt: "Pressupostos do cliente",
    },
  },
  estimatedValue: "10.00",
  valueCurrency: "EUR",
  valueDisputed: false,
};
const summary = {
  counts: { todo: 0, in_progress: 0, blocked: 0, done: 1, failed: 0 },
  value: {
    configured: true,
    estimated: true,
    totals: [{ amount: "10", currency: "EUR" }],
    models: [
      {
        formula: "labor_time_saved",
        currency: "EUR",
        baselineSeconds: 1200,
        loadedHourlyCost: "30",
        explanation: {
          en: "Customer assumptions",
          pt: "Pressupostos do cliente",
        },
      },
    ],
  },
  trend: [{ date: "2026-08-05", amount: "10", currency: "EUR" }],
};

function json(data: unknown, status = 200) {
  return Promise.resolve(
    new Response(JSON.stringify(data), {
      status,
      headers: { "Content-Type": "application/json" },
    }),
  );
}
function fetchMock(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = String(input);
  if (url.startsWith("/v1/work-events"))
    return new Promise((_resolve, reject) =>
      init?.signal?.addEventListener("abort", () =>
        reject(new DOMException("Aborted", "AbortError")),
      ),
    );
  if (url.startsWith("/v1/work-dashboard")) return json(summary);
  if (url.startsWith("/v1/work-items?")) return json([item]);
  if (url === "/v1/work-items/item-1") return json(item);
  if (url.endsWith("/attempts"))
    return json([
      {
        id: "attempt-1",
        workItemId: "item-1",
        number: 1,
        definitionKey: "walter/quotation",
        definitionVersion: "1",
        createdAt: item.createdAt,
        finishedAt: item.finishedAt,
      },
    ]);
  if (url.includes("/work-attempts/"))
    return json([
      {
        id: "node-1",
        attemptId: "attempt-1",
        nodeKey: "process",
        businessLabel: {
          en: "Processing quotation",
          pt: "A processar cotação",
        },
        visit: 1,
        executionSeq: 1,
        kind: "agent_task",
        state: "succeeded",
        summary: "Quotation processed",
        output: { classification: "pricing" },
        createdAt: item.createdAt,
      },
    ]);
  if (url.endsWith("/timeline?limit=1000"))
    return json([
      {
        cursor: 1,
        id: "event-1",
        workItemId: "item-1",
        attemptId: "attempt-1",
        nodeExecutionId: "node-1",
        code: "work_completed",
        params: {},
        createdAt: item.finishedAt,
      },
    ]);
  if (url.endsWith("/blocker")) return json({ error: "not found" }, 404);
  if (url.endsWith("/feedback") && init?.method !== "POST") return json([]);
  if (url.endsWith("/feedback") && init?.method === "POST")
    return json(
      {
        id: "feedback-1",
        workItemId: "item-1",
        attemptId: "attempt-1",
        category: "poor_output",
        createdAt: new Date().toISOString(),
      },
      201,
    );
  if (url.endsWith("/revisions") && init?.method === "POST")
    return json({ ...item, state: "in_progress" }, 201);
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
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn(fetchMock));
  });
  afterEach(() => vi.unstubAllGlobals());

  it("keeps the key in memory, renders the single-workflow board, and excludes deferred navigation", async () => {
    await connect();
    expect(
      screen.getByText("Estimated business value created"),
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "To do" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Done" })).toBeInTheDocument();
    expect(screen.queryByText("Workflows")).not.toBeInTheDocument();
    expect(document.cookie).not.toContain("work_secret");
    const calls = vi.mocked(fetch).mock.calls;
    expect(
      calls.some(
        ([, init]) =>
          (init?.headers as Record<string, string>)?.Authorization ===
          "Bearer work_secret",
      ),
    ).toBe(true);
  });

  it("recovers from an invalid key when the operator retries with a valid key", async () => {
    vi.mocked(fetch).mockImplementation((input, init) => {
      const authorization = (init?.headers as Record<string, string>)
        ?.Authorization;
      if (
        String(input).startsWith("/v1/work-dashboard") &&
        authorization === "Bearer invalid"
      )
        return json({ error: "unauthorized" }, 401);
      return fetchMock(input, init);
    });
    const user = userEvent.setup();
    render(<App />);
    const key = screen.getByLabelText("Work API key");
    await user.type(key, "invalid");
    await user.click(screen.getByRole("button", { name: "Connect" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "The key could not open the Dashboard.",
    );
    await user.clear(key);
    await user.type(key, "work_secret");
    await user.click(screen.getByRole("button", { name: "Connect" }));
    expect(
      await screen.findByText("Quotation request — ACME"),
    ).toBeInTheDocument();
  });

  it("keeps active business steps visible and failed work outside the four columns when value is unconfigured", async () => {
    const active = {
      ...item,
      id: "item-active",
      state: "in_progress",
      finishedAt: undefined,
      startedAt: "2026-08-05T09:01:00Z",
      estimatedValue: undefined,
      valueConfigured: false,
      currentStep: {
        key: "process",
        label: {
          en: "Checking quotation details",
          pt: "A verificar detalhes da cotação",
        },
        state: "running",
      },
    };
    const failed = {
      ...item,
      id: "item-failed",
      title: "Quotation request — Could not parse",
      state: "failed",
      estimatedValue: undefined,
      valueConfigured: false,
      failureSummary: { message: "The quotation could not be processed." },
    };
    const unconfigured = {
      ...summary,
      counts: { todo: 0, in_progress: 1, blocked: 0, done: 0, failed: 1 },
      value: { configured: false, estimated: false, totals: [], models: [] },
      trend: [],
    };
    vi.mocked(fetch).mockImplementation((input, init) => {
      const url = String(input);
      if (url.startsWith("/v1/work-dashboard")) return json(unconfigured);
      if (url.startsWith("/v1/work-items?")) return json([active, failed]);
      if (url === "/v1/work-items/item-active") return json(active);
      return fetchMock(input, init);
    });
    const user = await connect();
    expect(screen.getByText("Value model not configured")).toBeInTheDocument();
    expect(screen.getByText("Checking quotation details")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: /Could not complete/ }),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Quotation request — Could not parse"),
    ).toBeInTheDocument();
    await user.click(screen.getByText("Quotation request — ACME"));
    expect(await screen.findByText("Processing quotation")).toBeInTheDocument();
    expect(
      screen.queryByText(
        /model messages|worker details|tool calls?|system prompts?/i,
      ),
    ).not.toBeInTheDocument();
  });

  it("presents Portuguese customer labels", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(screen.getByRole("button", { name: "PT" }));
    expect(
      screen.getByRole("heading", { name: "Ligar ao Painel" }),
    ).toBeInTheDocument();
    expect(
      screen.getByLabelText("Chave da API de trabalho"),
    ).toBeInTheDocument();
  });

  it("inspects a completed outcome and saves feedback without starting a revision", async () => {
    const user = await connect();
    await user.click(screen.getByText("Quotation request — ACME"));
    expect(await screen.findByText("Quotation processed")).toBeInTheDocument();
    expect(screen.getByText("pricing")).toBeInTheDocument();
    const itemValueExplanation = screen
      .getAllByText("How this is estimated")
      .at(-1);
    expect(itemValueExplanation).toBeDefined();
    await user.click(itemValueExplanation!);
    expect(screen.getByText("Customer assumptions")).toBeInTheDocument();
    expect(screen.getByText("Manual handling time saved")).toBeInTheDocument();
    expect(screen.getByText(/€30\.00\/hour/)).toBeInTheDocument();
    expect(screen.getByText("Customer baseline")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "PT" }));
    expect(
      screen.getByText("Tempo de trabalho manual poupado"),
    ).toBeInTheDocument();
    expect(screen.getByText(/\/hora/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "EN" }));
    expect(
      screen.queryByText(/successful|correct|approved/i),
    ).not.toBeInTheDocument();
    await user.click(
      screen.getByRole("button", { name: /Result needs improvement/ }),
    );
    await user.click(screen.getByRole("button", { name: "Poor output" }));
    await user.type(
      screen.getByPlaceholderText(/Explanation/),
      "Wrong extracted total",
    );
    await user.click(screen.getByRole("button", { name: "Save feedback" }));
    await waitFor(() =>
      expect(screen.getByText(/without starting new work/)).toBeInTheDocument(),
    );
    const post = vi
      .mocked(fetch)
      .mock.calls.find(
        ([url, init]) =>
          String(url).endsWith("/feedback") && init?.method === "POST",
      );
    expect(post).toBeTruthy();
    expect(
      vi
        .mocked(fetch)
        .mock.calls.some(([url]) => String(url).endsWith("/revisions")),
    ).toBe(false);
  });

  it("requires exact consequence confirmation before starting a revised attempt", async () => {
    const consequence = {
      invocationId: "invocation-1",
      summary: {
        en: "Send the quotation email again",
        pt: "Enviar novamente o email da cotação",
      },
      state: "succeeded",
    };
    vi.mocked(fetch).mockImplementation((input, init) =>
      String(input).endsWith("/consequences")
        ? json([consequence])
        : fetchMock(input, init),
    );
    const user = await connect();
    await user.click(screen.getByText("Quotation request — ACME"));
    await screen.findByText("Quotation processed");
    await user.click(
      screen.getByRole("button", { name: /Result needs improvement/ }),
    );
    await user.click(
      screen.getByRole("button", { name: "Try again with feedback" }),
    );
    await user.type(
      screen.getByPlaceholderText(/Actionable guidance/),
      "Use the corrected classification rule",
    );
    const start = screen.getByRole("button", { name: "Start revised attempt" });
    expect(start).toBeDisabled();
    await user.click(screen.getByText("Send the quotation email again"));
    expect(start).toBeEnabled();
    await user.click(start);
    await waitFor(() =>
      expect(
        screen.getByText(/original history is preserved/),
      ).toBeInTheDocument(),
    );
    const revision = vi
      .mocked(fetch)
      .mock.calls.find(
        ([url, init]) =>
          String(url).endsWith("/revisions") && init?.method === "POST",
      );
    expect(revision?.[1]?.body).toContain("invocation-1");
  });

  it("uploads and attaches a file when resolving an artifact blocker", async () => {
    const blocked = {
      ...item,
      id: "item-blocked",
      currentAttemptId: "attempt-blocked",
      state: "blocked",
      blocker: {
        id: "blocker-1",
        kind: "artifact",
        title: {
          en: "Signed agreement required",
          pt: "Acordo assinado necessário",
        },
      },
    };
    const blocker = {
      id: "blocker-1",
      workItemId: blocked.id,
      attemptId: blocked.currentAttemptId,
      kind: "artifact",
      title: blocked.blocker.title,
      description: {
        en: "Upload the signed agreement",
        pt: "Carregue o acordo assinado",
      },
      responseSchema: {
        type: "object",
        properties: { note: { type: "string", title: "Note" } },
      },
      allowedOutcomes: ["provided"],
      state: "open",
    };
    vi.mocked(fetch).mockImplementation((input, init) => {
      const url = String(input);
      if (url.startsWith("/v1/work-items?")) return json([blocked]);
      if (url === "/v1/work-items/item-blocked") return json(blocked);
      if (url.includes("attempt-blocked") && url.includes("/nodes"))
        return json([]);
      if (url.endsWith("/blocker")) return json(blocker);
      if (url === "/v1/artifacts" && init?.method === "POST")
        return json({ id: "artifact-1" }, 201);
      if (url.includes("/work-blockers/blocker-1/responses"))
        return json({ ...blocker, state: "resolved" });
      return fetchMock(input, init);
    });
    const user = await connect();
    await user.click(screen.getByText("Quotation request — ACME"));
    expect(
      await screen.findByRole("heading", { name: "Signed agreement required" }),
    ).toBeInTheDocument();
    const file = new File(["signed"], "agreement.pdf", {
      type: "application/pdf",
    });
    await user.upload(
      screen.getByLabelText("Choose the required artifact"),
      file,
    );
    const send = screen.getByRole("button", { name: "Send response" });
    await waitFor(() => expect(send).toBeEnabled());
    await user.click(send);
    await waitFor(() =>
      expect(
        vi
          .mocked(fetch)
          .mock.calls.some(([url]) =>
            String(url).includes("/work-blockers/blocker-1/responses"),
          ),
      ).toBe(true),
    );
    const response = vi
      .mocked(fetch)
      .mock.calls.find(([url]) =>
        String(url).includes("/work-blockers/blocker-1/responses"),
      );
    expect(response?.[1]?.body).toContain("artifact-1");
  });

  it("submits the selected outcome and correctly typed JSON-Schema fields", async () => {
    const blocked = {
      ...item,
      id: "item-decision",
      currentAttemptId: "attempt-decision",
      state: "blocked",
      blocker: {
        id: "blocker-decision",
        kind: "decision",
        title: { en: "Choose the delivery path", pt: "Escolha a entrega" },
      },
    };
    const blocker = {
      id: "blocker-decision",
      workItemId: blocked.id,
      attemptId: blocked.currentAttemptId,
      kind: "decision",
      title: blocked.blocker.title,
      description: {
        en: "Choose and supply the decision details.",
        pt: "Escolha e forneça os detalhes da decisão.",
      },
      responseSchema: {
        type: "object",
        required: ["amount", "quantity", "details", "items"],
        properties: {
          amount: { type: "number", title: "Amount" },
          quantity: { type: "integer", title: "Quantity" },
          details: { type: "object", title: "Details" },
          items: { type: "array", title: "Items" },
        },
      },
      allowedOutcomes: ["approved", "rejected"],
      state: "open",
    };
    vi.mocked(fetch).mockImplementation((input, init) => {
      const url = String(input);
      if (url.startsWith("/v1/work-items?")) return json([blocked]);
      if (url === "/v1/work-items/item-decision") return json(blocked);
      if (url.endsWith("/attempts")) return json([]);
      if (url.endsWith("/blocker")) return json(blocker);
      if (url.includes("/work-blockers/blocker-decision/responses"))
        return json({ ...blocker, state: "resolved" });
      return fetchMock(input, init);
    });
    const user = await connect();
    await user.click(screen.getByRole("button", { name: "PT" }));
    await user.click(screen.getByText("Quotation request — ACME"));
    await screen.findByRole("heading", { name: "Escolha a entrega" });
    const outcome = screen.getByLabelText("Decisão solicitada");
    expect(
      screen.getByRole("option", { name: "Aprovado" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "Rejeitado" }),
    ).toBeInTheDocument();
    await user.selectOptions(outcome, "rejected");
    await user.type(screen.getByLabelText("Montante"), "12.5");
    await user.type(screen.getByLabelText("Quantidade"), "3");
    await user.click(screen.getByLabelText("Detalhes"));
    await user.paste('{"priority":"high"}');
    await user.click(screen.getByLabelText("Itens"));
    await user.paste('["quote"]');
    await user.click(screen.getByRole("button", { name: "Enviar resposta" }));
    await waitFor(() =>
      expect(
        vi
          .mocked(fetch)
          .mock.calls.some(([url]) =>
            String(url).includes("/work-blockers/blocker-decision/responses"),
          ),
      ).toBe(true),
    );
    const response = vi
      .mocked(fetch)
      .mock.calls.find(([url]) =>
        String(url).includes("/work-blockers/blocker-decision/responses"),
      );
    const body = JSON.parse(String(response?.[1]?.body));
    expect(body.outcome).toBe("rejected");
    expect(body.response).toEqual({
      amount: 12.5,
      quantity: 3,
      details: { priority: "high" },
      items: ["quote"],
    });
  });

  it("requires exact confirmation before resolving a consequence blocker and localizes it in Portuguese", async () => {
    const blocked = {
      ...item,
      id: "item-confirmation",
      currentAttemptId: "attempt-confirmation",
      state: "blocked",
      source: { ...item.source, kind: "schedule" },
      blocker: {
        id: "blocker-confirmation",
        kind: "consequence_confirmation",
        title: { en: "Confirm repeated action", pt: "Confirmar ação repetida" },
      },
    };
    const blocker = {
      id: "blocker-confirmation",
      workItemId: blocked.id,
      attemptId: blocked.currentAttemptId,
      kind: "consequence_confirmation",
      title: blocked.blocker.title,
      description: {
        en: "Review the possible action.",
        pt: "Reveja a ação possível.",
      },
      responseSchema: { type: "object" },
      allowedOutcomes: ["confirmed"],
      requiredConsequences: [
        {
          invocationId: "invocation-1",
          summary: {
            en: "Send the quotation email again",
            pt: "Enviar novamente o email da cotação",
          },
          state: "succeeded",
        },
      ],
      state: "open",
    };
    vi.mocked(fetch).mockImplementation((input, init) => {
      const url = String(input);
      if (url.startsWith("/v1/work-items?")) return json([blocked]);
      if (url === "/v1/work-items/item-confirmation") return json(blocked);
      if (url.endsWith("/attempts")) return json([]);
      if (url.endsWith("/blocker")) return json(blocker);
      if (url.includes("/work-blockers/blocker-confirmation/responses"))
        return json({ ...blocker, state: "resolved" });
      return fetchMock(input, init);
    });
    const user = await connect();
    await user.click(screen.getByRole("button", { name: "PT" }));
    await user.click(screen.getByText("Quotation request — ACME"));
    expect(
      await screen.findAllByText("Confirmação de ação consequente"),
    ).toHaveLength(2);
    expect(screen.getAllByText("Agendado").length).toBeGreaterThan(0);
    const send = screen.getByRole("button", { name: "Enviar resposta" });
    expect(send).toBeDisabled();
    await user.click(screen.getByText("Enviar novamente o email da cotação"));
    expect(send).toBeEnabled();
    await user.click(send);
    const response = await waitFor(() => {
      const call = vi
        .mocked(fetch)
        .mock.calls.find(([url]) =>
          String(url).includes("/work-blockers/blocker-confirmation/responses"),
        );
      expect(call).toBeTruthy();
      return call;
    });
    const body = JSON.parse(String(response?.[1]?.body));
    expect(body.confirmedInvocationIds).toEqual(["invocation-1"]);
  });

  it("has no automatically detectable accessibility violations on the Dashboard", async () => {
    await connect();
    const results = await axe.run(document.body, {
      rules: { "color-contrast": { enabled: false } },
    });
    expect(results.violations).toEqual([]);
  });
});
