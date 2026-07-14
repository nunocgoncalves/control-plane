// pi event -> proto Event mapping (HOR-351). Translates pi's
// AgentSessionEvent stream into the curated 5-kind Event oneof the
// control-plane records (HOR-249). v1 forwards only the semantic events:
// TurnStarted, AssistantMessage, ToolResult, Error, Settled. Deferred:
// message_update deltas (live token streaming), queue_update, compaction/retry.
//
// SKELETON: the generated Event types come from `make proto`
// (./gen/harness/v1/harness_pb.ts + connect-es). The mapping below is the
// intended shape; wire it to session.subscribe() in server.ts.

import type { AgentSessionEvent } from "@earendil-works/pi-coding-agent";
// TODO after `make proto`:
// import { create, toBinary } from "@bufbuild/protobuf";
// import * as pb from "./gen/harness/v1/harness_pb.js";

export type HarnessEvent =
  | { $case: "turn_started"; model: string; thinkingLevel: string }
  | { $case: "assistant_message"; text: string; toolCalls: ToolCall[]; usage: Usage; stopReason: string }
  | { $case: "tool_result"; toolCallId: string; toolName: string; argumentsJson: string; resultText: string; isError: boolean }
  | { $case: "error"; source: string; message: string }
  | { $case: "settled"; reason: "completed" | "aborted" | "failed"; messageCount: number };

interface ToolCall {
  id: string;
  name: string;
  argumentsJson: string;
}
interface Usage {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  costUsd: number;
}

// Map a pi event to zero or one HarnessEvent. pi emits many event types; v1
// keeps only the semantic ones. Returns undefined to drop (e.g. deltas).
export function mapEvent(_e: AgentSessionEvent): HarnessEvent | undefined {
  // TODO: switch on _e.type:
  //   turn_start        -> { $case: "turn_started", model, thinkingLevel }
  //   message_end       -> { $case: "assistant_message", ... } (role=assistant only)
  //   tool_execution_end-> { $case: "tool_result", ... }
  //   extension_error   -> { $case: "error", source: "extension", ... }
  //   agent_settled     -> { $case: "settled", reason: "completed" }
  // (agent_end with willRetry / auto_retry_end final failure -> error + settled{failed})
  return undefined;
}
