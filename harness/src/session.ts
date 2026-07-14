// pi session wiring (HOR-351). Embeds pi via the SDK (createAgentSession) —
// the harness IS the agent. One pod = one session, selected by config.session.id
// (control-plane-owned mapping, HOR-249): resume (SessionManager.open) if a
// session exists for the id, else create (scoped to a per-id dir). Built-in
// coding tools are off; the agent's tools come from the overlay pi/ tree.
// Model traffic routes to the egress proxy with a placeholder key (the proxy
// substitutes the real gateway key — HOR-244); the harness holds no real creds.

import {
  AuthStorage,
  createAgentSession,
  DefaultResourceLoader,
  ModelRegistry,
  SessionManager,
  type AgentSession,
  type ExtensionFactory,
  type ProviderConfig,
} from "@earendil-works/pi-coding-agent";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import type { HarnessConfig } from "./config.js";
import { toolOptions } from "./enforcement.js";

const PROVIDER = "iterabase-inference";

export interface SessionHandle {
  session: AgentSession;
  isNew: boolean;
}

export async function createSession(cfg: HarnessConfig): Promise<SessionHandle> {
  const sessionDir = join(cfg.session.dir, cfg.session.id);
  const existing = findSessionFile(sessionDir);

  const authStorage = AuthStorage.create(join(sessionDir, "auth.json"));
  const modelRegistry = ModelRegistry.create(authStorage);

  // Register the egress-proxy-backed model provider. The placeholder key is
  // what the per-sandbox egress proxy swaps for the real gateway key on the
  // wire (HOR-244). pi.registerProvider -> modelRegistry.registerProvider, so
  // modelRegistry.find() sees it after the factory runs (during createAgentSession).
  const providerFactory: ExtensionFactory = (pi) => {
    const provider: ProviderConfig = {
      baseUrl: cfg.model.endpoint,
      apiKey: "placeholder",
      api: cfg.model.api as unknown as ProviderConfig["api"],
      models: [
        {
          id: cfg.model.id,
          name: cfg.model.id,
          reasoning: false,
          input: ["text"],
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          contextWindow: cfg.model.contextWindow,
          maxTokens: 4096,
        },
      ],
    };
    pi.registerProvider(PROVIDER, provider);
  };

  const resourceLoader = new DefaultResourceLoader({
    cwd: sessionDir,
    agentDir: sessionDir, // scope discovery to the session (no global ~/.pi)
    additionalExtensionPaths: cfg.piDirs, // product first, then client (last-wins precedence)
    extensionFactories: [providerFactory],
    systemPromptOverride: () => cfg.persona,
  });
  await resourceLoader.reload();

  const sessionManager = existing
    ? SessionManager.open(existing, sessionDir, sessionDir) // resume
    : SessionManager.create(sessionDir, sessionDir); // create, scoped to the per-id dir

  const { session } = await createAgentSession({
    cwd: sessionDir,
    authStorage,
    modelRegistry,
    resourceLoader,
    sessionManager,
    ...toolOptions(cfg.toolAllowList),
  });

  const model = modelRegistry.find(PROVIDER, cfg.model.id);
  if (!model) throw new Error(`harness: model not found after provider registration: ${cfg.model.id}`);
  await session.setModel(model);

  return { session, isNew: !existing };
}

// A session dir holds at most one active session file in v1 (no forking).
// Scoped to config.session.id — never a cross-session "most recent" scan.
function findSessionFile(sessionDir: string): string | undefined {
  if (!existsSync(sessionDir)) return undefined;
  const files = readdirSync(sessionDir).filter((f) => f.endsWith(".jsonl"));
  if (files.length === 0) return undefined;
  return join(sessionDir, files.sort().at(-1)!);
}
