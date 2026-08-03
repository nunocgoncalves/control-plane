// The supervisor's tool-gateway client (HOR-395, ARCH-004/010). The trusted
// supervisor authenticates to the tool gateway with its SPIFFE-bound worker
// mTLS cert and calls GatewayService (DiscoverEffectiveTools/InvokeTool/
// CancelInvocation). The disposable child never sees this client or the cert.
//
// The supervisor stamps durable caller context from the active assignment
// (attempt_id = run id, caller_scope = TURN, caller_scope_id = turn id) — the
// child supplies only business arguments + tool-call id (ARCH-004). The gateway
// re-resolves scope from durable state and validates it; caller-supplied fields
// are validated, never trusted as scope.
//
// One lazy gRPC Connect transport, reused across discover/invoke/cancel within
// the process. Certs are re-read on first use so a freshly-rotated file is
// picked up without a restart.

import { readFileSync } from "node:fs";
import { createClient, type Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  GatewayService,
  EffectClass,
  CallerScope,
  type ToolDescriptor,
  type InvokeResponse,
  type CancelResponse,
  type DiscoverResponse,
} from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { HarnessConfig } from "./config.js";
import type { GatewayToolDescriptor } from "./ipc.js";

export interface AssignmentScope {
  turnId: string;
  runId: string;
  /** The supervisor's current Welcome fencing generation (HOR-249/DEC-041).
   * Sent on every gateway request so the gateway can deny a fenced/old-
   * generation caller by comparing against the active assignment. */
  fencingGeneration: bigint;
}

export class GatewayClientError extends Error {}

/** Load + memoize the mTLS gRPC transport for the tool gateway. */
export function createGatewayTransport(cfg: HarnessConfig): Transport {
  const tls = cfg.tls;
  const ca = readFileSync(tls.ca);
  const cert = readFileSync(tls.cert);
  const key = readFileSync(tls.key);
  return createGrpcTransport({
    baseUrl: cfg.toolGateway.url,
    nodeOptions: {
      ca,
      key,
      cert,
      rejectUnauthorized: true,
      servername: cfg.toolGateway.serverName,
    } as Record<string, unknown>,
  });
}

export interface GatewayClient {
  /** Discover the effective gateway tools for an active turn (ARCH-006). */
  discover(scope: AssignmentScope, signal?: AbortSignal): Promise<GatewayToolDescriptor[]>;
  /** Invoke a gateway tool (ARCH-014). Returns the committed result + state. */
  invokeTool(
    scope: AssignmentScope,
    call: { toolCallId: string; toolName: string; toolVersionDigest: string; argumentsJson: string; idempotencyKey?: string },
    signal: AbortSignal | undefined,
  ): Promise<InvokeResponse>;
  /** Cancel an in-flight invocation (ARCH-014 — cannot undo an effect). */
  cancelInvocation(invocationId: string): Promise<CancelResponse>;
  /** Drop the memoized mTLS transport so the next call re-reads the cert/key/CA
   * files (certificate rotation). Called by the supervisor on reconnect. */
  resetTransport(): void;
}

/** Build a GatewayClient over a lazy transport. */
export function createGatewayClient(cfg: HarnessConfig, transportFactory: () => Transport = () => createGatewayTransport(cfg)): GatewayClient {
  let transport: Transport | null = null;
  const getTransport = (): Transport => {
    if (!transport) transport = transportFactory();
    return transport;
  };
  return {
    async discover(scope: AssignmentScope, signal?: AbortSignal): Promise<GatewayToolDescriptor[]> {
      const client = createClient(GatewayService, getTransport());
      const req = {
        attemptId: scope.runId,
        callerScope: CallerScope.TURN,
        callerScopeId: scope.turnId,
        fencingGeneration: scope.fencingGeneration,
      };
      let resp: DiscoverResponse;
      try {
        resp = await client.discoverEffectiveTools(req, signal ? { signal } : undefined);
      } catch (err) {
        throw new GatewayClientError(`discover failed: ${err instanceof Error ? err.message : String(err)}`);
      }
      return (resp.descriptors ?? []).map(descriptorToNonSecret);
    },

    async invokeTool(
      scope: AssignmentScope,
      call: { toolCallId: string; toolName: string; toolVersionDigest: string; argumentsJson: string; idempotencyKey?: string },
      signal: AbortSignal | undefined,
    ): Promise<InvokeResponse> {
      const client = createClient(GatewayService, getTransport());
      const req = {
        attemptId: scope.runId,
        callerScope: CallerScope.TURN,
        callerScopeId: scope.turnId,
        toolCallId: call.toolCallId,
        toolName: call.toolName,
        toolVersionDigest: call.toolVersionDigest,
        argumentsJson: new TextEncoder().encode(call.argumentsJson),
        fencingGeneration: scope.fencingGeneration,
        ...(call.idempotencyKey !== undefined ? { idempotencyKey: call.idempotencyKey } : {}),
      };
      try {
        return await client.invokeTool(req, signal ? { signal } : undefined);
      } catch (err) {
        throw new GatewayClientError(`invoke ${call.toolName} failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    },

    async cancelInvocation(invocationId: string): Promise<CancelResponse> {
      const client = createClient(GatewayService, getTransport());
      const req = { invocationId };
      try {
        return await client.cancelInvocation(req);
      } catch (err) {
        throw new GatewayClientError(`cancel ${invocationId} failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    },

    resetTransport(): void {
      // Drop the memoized transport so the next discover/invoke/cancel call
      // rebuilds it from the current cert/key/CA files (rotation). The
      // process-wide memo otherwise keeps stale credentials until restart.
      transport = null;
    },
  };
}

/** Reduce a full ToolDescriptor to the non-secret shape passed to the child (ARCH-006). */
function descriptorToNonSecret(d: ToolDescriptor): GatewayToolDescriptor {
  const desc: GatewayToolDescriptor = {
    name: d.name,
    version: d.version,
    digest: d.digest,
    description: d.description,
    inputSchema: d.inputSchema.length ? JSON.parse(new TextDecoder().decode(d.inputSchema)) : {},
    effectClass: effectClassToString(d.effectClass),
  };
  const timeoutMs = durToMs(d.timeout);
  if (Number.isFinite(timeoutMs)) desc.timeoutMs = timeoutMs;
  return desc;
}

function effectClassToString(e: ToolDescriptor["effectClass"]): GatewayToolDescriptor["effectClass"] {
  switch (e) {
    case EffectClass.READ_ONLY:
      return "read_only";
    case EffectClass.IDEMPOTENT_WRITE:
      return "idempotent_write";
    case EffectClass.NON_IDEMPOTENT_WRITE:
      return "non_idempotent_write";
    default:
      return "read_only"; // fail-safe default (gateway rejects unspecified at registration)
  }
}

function durToMs(dur: { seconds: bigint; nanos: number } | undefined): number {
  if (!dur) return NaN;
  return Number(dur.seconds) * 1000 + Math.floor(dur.nanos / 1_000_000);
}
