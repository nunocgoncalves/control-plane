import { describe, it, expect } from "vitest";
import { createRouterTransport } from "@connectrpc/connect";
import { createGatewayClient, GatewayClientError } from "./gateway-client.js";
import { GatewayService, ArtifactService, EffectClass, CallerScope, InvokeState } from "./gen/iterabase/gateway/v1/gateway_pb.js";
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
      readsArtifacts: false,
      acceptedArtifactMimeTypes: [],
      timeoutMs: 30_000,
    });
  });

  it("invokeTool stamps attempt_id=runId + caller_scope_id=turnId and decodes the result", async () => {
    let seen: { attemptId: string; callerScope: number; callerScopeId: string; toolCallId: string; artifactIds: string[] } | undefined;
    const transport = createRouterTransport((router) => {
      router.service(GatewayService, {
        discoverEffectiveTools: async () => ({ descriptors: [] }),
        invokeTool: async (req) => {
          seen = {
            attemptId: req.attemptId, callerScope: req.callerScope, callerScopeId: req.callerScopeId,
            toolCallId: req.toolCallId, artifactIds: req.artifactInputRefs.map((ref) => ref.artifactId),
          };
          return { invocationId: "inv-2", state: InvokeState.SUCCEEDED, resultJson: new TextEncoder().encode('{"ok":true}'), artifactOutputRefs: [], error: undefined, existingInvocationId: "" };
        },
        cancelInvocation: async () => ({ state: InvokeState.FAILED }),
      });
    });
    const client = createGatewayClient(cfg(), () => transport as Transport);
    const resp = await client.invokeTool(
      { turnId: "turn-1", runId: "run-1" },
      {
        toolCallId: "tc-1", toolName: "graph.read_mail", toolVersionDigest: "sha256:abc", argumentsJson: '{"limit":5}',
        artifactInputRefs: [{ artifactId: "artifact-1", mimeType: "text/plain", sizeBytes: "4", digest: "sha256:test" }],
        idempotencyKey: "tc-1",
      },
      undefined,
    );
    expect(resp.state).toBe(InvokeState.SUCCEEDED);
    expect(new TextDecoder().decode(resp.resultJson)).toBe('{"ok":true}');
    expect(seen).toEqual({ attemptId: "run-1", callerScope: CallerScope.TURN, callerScopeId: "turn-1", toolCallId: "tc-1", artifactIds: ["artifact-1"] });
  });

  it("streams artifact bytes with authenticated turn context", async () => {
    let uploaded = "";
    const transport = createRouterTransport((router) => {
      router.service(ArtifactService, {
        putArtifact: async (requests) => {
          for await (const request of requests) {
            if (request.kind.case === "chunk") uploaded += new TextDecoder().decode(request.kind.value);
          }
          return { metadata: { ref: { artifactId: "a1", mimeType: "text/plain", sizeBytes: 4n, digest: "sha256:test" }, source: "sandbox_publish", state: "available", createdAtUnixMs: 0n } };
        },
        getArtifact: async function* (request) {
          yield { kind: { case: "metadata" as const, value: { ref: { artifactId: request.artifactId, mimeType: "text/plain", sizeBytes: 4n, digest: "sha256:test" }, source: "user_upload", state: "available", createdAtUnixMs: 0n } } };
          yield { kind: { case: "chunk" as const, value: new TextEncoder().encode("data") } };
        },
        statArtifact: async (request) => ({ metadata: { ref: { artifactId: request.artifactId, mimeType: "text/plain", sizeBytes: 4n, digest: "sha256:test" }, source: "user_upload", state: "available", createdAtUnixMs: 0n } }),
      });
    });
    const client = createGatewayClient(cfg(), () => transport as Transport);
    const scope = { turnId: "turn-1", runId: "run-1", fencingGeneration: 2n };
    const metadata = await client.putArtifact!(scope, { mimeType: "text/plain", expectedSizeBytes: 4n, expectedDigest: "sha256:test", chunks: (async function* () { yield new TextEncoder().encode("data"); })() });
    expect(metadata.ref?.artifactId).toBe("a1");
    expect(uploaded).toBe("data");
    let downloaded = "";
    for await (const response of client.getArtifact!(scope, "a1")) {
      if (response.kind.case === "chunk") downloaded += new TextDecoder().decode(response.kind.value);
    }
    expect(downloaded).toBe("data");
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
