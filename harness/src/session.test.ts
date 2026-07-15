import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createSession } from "./session.js";
import type { HarnessConfig } from "./config.js";

// Exercises the real pi createAgentSession wiring (HOR-351) hermetically: the
// model provider is registered + selected (never invoked — no prompt), the
// session is created/resumed by session.id, the persona is applied, and the
// tool option disables built-in coding tools. The prompt->event flow is
// covered by server.test.ts (Connect contract) + the forge e2e (HOR-365).
//
// pi materializes the session file lazily (only on a real prompt, which needs a
// model), so the resume test pre-writes a valid session file to exercise the
// findSessionFile -> SessionManager.open path; in prod the first Prompt writes it.
function cfg(sessionDir: string, id: string): HarnessConfig {
  return {
    persona: "You are a test email-triage agent.",
    model: { id: "mock-model", endpoint: "https://localhost:8444/v1", api: "openai-completions", contextWindow: 8192 },
    toolAllowList: "*",
    session: { id, dir: sessionDir },
    piDirs: [],
    egressProxyUrl: "https://localhost:8444",
    tls: { cert: "/tmp/x", key: "/tmp/x", ca: "/tmp/x" },
    server: { port: 8443, healthPort: 8081 },
  };
}

describe("createSession", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "harness-sess-"));
  });
  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("creates a new session scoped by session.id, selects the model, applies the persona, disables built-in coding tools", async () => {
    const { session, isNew } = await createSession(cfg(dir, "wf-1"));
    try {
      expect(isNew).toBe(true);
      expect(session.model?.id).toBe("mock-model"); // registerProvider -> find -> setModel
      expect(session.sessionId).toBeTruthy();
      expect(session.sessionFile?.startsWith(join(dir, "wf-1"))).toBe(true); // scoped by session.id
      expect(existsSync(join(dir, "wf-1"))).toBe(true); // harness mkdir'd the id dir
      expect(session.state.systemPrompt).toContain("test email-triage"); // persona via systemPromptOverride
      // broad-default ("*") -> noTools:"builtin": no read/bash/edit/write
      expect(session.state.tools.filter((t) => ["read", "bash", "edit", "write"].includes(t.name))).toHaveLength(0);
    } finally {
      session.dispose();
    }
  });

  it("resumes an existing session file for the same session.id (findSessionFile -> SessionManager.open)", async () => {
    const idDir = join(dir, "wf-2");
    const existing = join(idDir, "2026-01-01T00-00-00-000Z_00000000-0000-7000-8000-000000000000.jsonl");
    mkdirSync(idDir, { recursive: true });
    writeFileSync(
      existing,
      JSON.stringify({
        type: "session",
        version: 3,
        id: "00000000-0000-7000-8000-000000000000",
        timestamp: "2026-01-01T00:00:00.000Z",
        cwd: idDir,
      }) + "\n",
    );

    const { session, isNew } = await createSession(cfg(dir, "wf-2"));
    try {
      expect(isNew).toBe(false);
      expect(session.sessionFile).toBe(existing); // resumed the existing file, did not create a new one
    } finally {
      session.dispose();
    }
  });
});
