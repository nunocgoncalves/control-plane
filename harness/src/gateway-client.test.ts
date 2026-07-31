import { describe, it, expect } from "vitest";
import { createRouterTransport } from "@connectrpc/connect";
import { createGatewayClient, GatewayClientError } from "./gateway-client.js";
import { GatewayService, EffectClass, CallerScope, InvokeState } from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { HarnessConfig } from "./config.js";
import type { Transport } from "@connectrpc/connect";

function cfg(): HarnessConfig {
  return {
    controlPlane: { url: "", serverName: "" },
    worker: { workerId: "pod-1", poolId: "pool-1" },
    tls: { cert: "", key: "", ca: "" },
    sandboxRoot: "",
    piDirs: [],
    toolGateway: { url: "https://localhost:8442", serverName: "tool-gateway" },
    inferenceGateway: { url: "https://localhost:8443", serverName: "inference-gateway" },
    walDir: "",
    probe: { port: 0 },
    transport: { http2PingIntervalMs: 30000, http2PingTimeoutMs: 10000 },
    reconnect: { initialBackoffMs: 1, maxBackoffMs: 2, resetAfterMs: 1000 },
    child: { livenessIntervalMs: 1000, abortGraceMs: 1000 },
    outbox: { bound: 4096 },
    modelRetry: { maxAttempts: 3 },
    tokenDelta: { sendBufferBytes: 1048576 },
  } as HarnessConfig;
}

describe("createGatewayClient", () => {
  it("discover returns non-secret descriptors stamped with the turn scope", async () => {
    const transport = createRouterTransport((router) => {
      router.service(GatewayService, {
        discoverEffectiveTools: async (req) => ({
          descriptors: [
            {
              name: "graph.read_mail",
              version: "1.0.0",
              digest: "sha256:abc",
              description: "read mail",
              inputSchema: new TextEncoder().encode(JSON.stringify({ type: "object" })),
              effectClass: EffectClass.READ_ONLY,
              credentialSlots: [],
              artifactCapabilities: { readsArtifacts: false, writesArtifacts: false, acceptedMimeTypes: [] },
              timeout: { seconds: 30n, nanos: 0 },
              idempotencyProof: undefined,
            },
          ],
        }),
        invokeTool: async () => ({ invocationId: "inv-1", state: InvokeState.SUCCEEDED, resultJson: new TextEncoder().encode('{"ok":true}'), artifactOutputRefs: [], error: undefined, existingInvocationId: "" }),
        cancelInvocation: async () => ({ state: InvokeState.FAILED }),
      });
    });
    const client = createGatewayClient(cfg(), () => transport as Transport);
    const descriptors = await client.discover({ turnId: "turn-1", runId: "run-1" });
    expect(descriptors).toHaveLength(1);
    expect(descriptors[0]).toEqual({
      name: "graph.read_mail",
      version: "1.0.0",
      digest: "sha256:abc",
      description: "read mail",
      inputSchema: { type: "object" },
      effectClass: "read_only",
      timeoutMs: 30_000,
    });
  });

  it("invokeTool stamps attempt_id=runId + caller_scope_id=turnId and decodes the result", async () => {
    let seen: { attemptId: string; callerScope: number; callerScopeId: string; toolCallId: string } | undefined;
    const transport = createRouterTransport((router) => {
      router.service(GatewayService, {
        discoverEffectiveTools: async () => ({ descriptors: [] }),
        invokeTool: async (req) => {
          seen = { attemptId: req.attemptId, callerScope: req.callerScope, callerScopeId: req.callerScopeId, toolCallId: req.toolCallId };
          return { invocationId: "inv-2", state: InvokeState.SUCCEEDED, resultJson: new TextEncoder().encode('{"ok":true}'), artifactOutputRefs: [], error: undefined, existingInvocationId: "" };
        },
        cancelInvocation: async () => ({ state: InvokeState.FAILED }),
      });
    });
    const client = createGatewayClient(cfg(), () => transport as Transport);
    const resp = await client.invokeTool(
      { turnId: "turn-1", runId: "run-1" },
      { toolCallId: "tc-1", toolName: "graph.read_mail", toolVersionDigest: "sha256:abc", argumentsJson: '{"limit":5}', idempotencyKey: "tc-1" },
      undefined,
    );
    expect(resp.state).toBe(InvokeState.SUCCEEDED);
    expect(new TextDecoder().decode(resp.resultJson)).toBe('{"ok":true}');
    expect(seen).toEqual({ attemptId: "run-1", callerScope: CallerScope.TURN, callerScopeId: "turn-1", toolCallId: "tc-1" });
  });

  it("wraps a transport error as GatewayClientError", async () => {
    const transport = createRouterTransport((router) => {
      router.service(GatewayService, {
        discoverEffectiveTools: async () => {
          throw new Error("boom");
        },
        invokeTool: async () => ({ invocationId: "", state: InvokeState.FAILED, resultJson: new Uint8Array(), artifactOutputRefs: [], error: undefined, existingInvocationId: "" }),
        cancelInvocation: async () => ({ state: InvokeState.FAILED }),
      });
    });
    const client = createGatewayClient(cfg(), () => transport as Transport);
    await expect(client.discover({ turnId: "t", runId: "r" })).rejects.toBeInstanceOf(GatewayClientError);
  });
});
