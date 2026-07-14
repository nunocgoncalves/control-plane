// Tool allow-list enforcement (HOR-351). In-process, load-time: filter the
// session's registered tools to the allow-list so disallowed tools are never
// exposed to the LLM (enforcement by construction). Broad-default ("*") passes
// all through (v1). Fine-grained per-action gating is deferred to HOR-283
// (a tool_call interception extension layered on top).

import type { AgentSession } from "@earendil-works/pi-coding-agent";
import type { ToolAllowList } from "./config.js";

export function filterTools(session: AgentSession, allow: ToolAllowList): void {
  if (allow === "*") return; // broad-default: nothing to filter
  const allowed = new Set(allow);
  const tools = session.agent.state.tools;
  session.agent.state.tools = tools.filter((t) => allowed.has(t.name));
}
