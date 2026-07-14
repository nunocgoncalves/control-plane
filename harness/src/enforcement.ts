// Tool allow-list enforcement (HOR-351). In-process, via pi's own tool
// allowlist: broad-default ("*") disables the built-in coding tools and keeps
// all overlay extension tools enabled; a specific list enables only those
// names. Enforcement by construction — disallowed tools are never exposed to
// the LLM. Fine-grained per-action gating (a tool_call interception extension)
// is deferred to HOR-283.

import type { CreateAgentSessionOptions } from "@earendil-works/pi-coding-agent";
import type { ToolAllowList } from "./config.js";

export function toolOptions(
  allow: ToolAllowList,
): Pick<CreateAgentSessionOptions, "tools" | "noTools"> {
  if (allow === "*") return { noTools: "builtin" };
  return { tools: allow };
}
