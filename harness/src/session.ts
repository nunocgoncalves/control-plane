// pi session wiring (HOR-351). Embeds pi via the SDK (createAgentSession) —
// the harness IS the agent. One pod = one session, selected by config.session.id
// (control-plane-owned mapping, HOR-249): resume (SessionManager.open) if a
// session exists for the id, else create (scoped to a per-id dir). Built-in
// coding tools are OFF (tools: []); the agent's tools come from the overlay
// pi/ tree. Model traffic routes to the egress proxy with a placeholder key.
//
// SKELETON: requires `make proto` (Connect stubs) + `npm install` to compile.

import {
  createAgentSession,
  DefaultResourceLoader,
  SessionManager,
  type AgentSession,
} from "@earendil-works/pi-coding-agent";
import { join } from "node:path";
import { existsSync, readdirSync } from "node:fs";
import type { HarnessConfig } from "./config.js";
import { filterTools } from "./enforcement.js";

export interface SessionHandle {
  session: AgentSession;
  isNew: boolean; // true if created (vs resumed) — useful for the control-plane's mapping
}

// Build the pi session from config. The persona is pod-level (systemPromptOverride);
// per-task instructions ride in each Prompt message.
export async function createSession(cfg: HarnessConfig): Promise<SessionHandle> {
  const sessionDir = join(cfg.session.dir, cfg.session.id); // scope by id; never scan "most recent"
  const existing = findSessionFile(sessionDir);

  const resourceLoader = new DefaultResourceLoader({
    cwd: sessionDir,
    additionalExtensionPaths: cfg.piDirs, // product first, then client (last-wins precedence)
    systemPromptOverride: () => cfg.persona,
  });
  await resourceLoader.reload();

  const sessionManager = existing
    ? SessionManager.open(existing, sessionDir) // resume
    : SessionManager.create(sessionDir, sessionDir); // create new, scoped to the per-id dir

  const { session } = await createAgentSession({
    cwd: sessionDir,
    resourceLoader,
    sessionManager,
    tools: [], // no built-in coding tools; tools come from the overlay pi/ tree
    // model: TODO register the egress-proxy provider via an inline extension:
    //   pi.registerProvider("iterabase-inference", {
    //     baseUrl: cfg.model.endpoint, api: cfg.model.api, apiKey: "placeholder",
    //     models: [{ id: cfg.model.id, contextWindow: cfg.model.contextWindow, ... }],
    //   });
    // then select cfg.model.id. The proxy substitutes the placeholder for the
    // real gateway key on the wire (HOR-244).
  });

  // Load-time allow-list enforcement (in-process). Broad-default ("*") passes
  // all registered tools through; a list filters to it. Runtime tool_call
  // interception (per-action) is deferred to HOR-283.
  filterTools(session, cfg.toolAllowList);

  return { session, isNew: !existing };
}

// A session dir holds at most one active session file (the harness never forks
// in v1). If present, resume it; else create new.
function findSessionFile(sessionDir: string): string | undefined {
  if (!existsSync(sessionDir)) return undefined;
  const files = readdirSync(sessionDir).filter((f) => f.endsWith(".jsonl"));
  if (files.length === 0) return undefined;
  if (files.length > 1) {
    // Unexpected in v1 (no forking); resume the most recent in THIS id's dir.
    // This is NOT cross-session "most recent" — it is scoped to session.id.
  }
  return join(sessionDir, files.sort().at(-1)!);
}
