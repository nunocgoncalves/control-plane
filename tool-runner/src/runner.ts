import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  ArtifactService, CredentialScheme, EffectClass, InvokeState, Retryability, RunnerService,
  RunnerMessageSchema, ToolDescriptorSchema, ToolVersionRefSchema,
  type ArtifactRef, type Credential, type Invoke, type RunnerMessage, type ToolDescriptor, type ToolVersionRef,
} from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { LoadedGeneration, LoadedTool, ToolManifest } from "./manifest.js";
import { AsyncQueue } from "./queue.js";
import { ToolError, type ToolArtifactAPI, type ToolErrorShape, type ToolInvocationContext } from "./types.js";

export interface RunnerConfig {
  gatewayURL: string;
  serverName: string;
  caFile: string;
  certFile: string;
  keyFile: string;
  runnerID: string;
  concurrency: number;
  maxGenerations: number;
  maxLoadedBytes: number;
  drainMaxAgeMs: number;
  stateFile?: string;
}

interface ActiveTool { tool: LoadedTool; generation: string; drainingAt?: number }
const keyOf = (ref: { name: string; digest: string }) => `${ref.name}\0${ref.digest}`;

class Semaphore {
  private used = 0;
  private waiters: Array<() => void> = [];
  constructor(private readonly maximum: number) {}
  async acquire(): Promise<() => void> {
    if (this.used >= this.maximum) await new Promise<void>((resolve) => this.waiters.push(resolve));
    this.used++;
    return () => { this.used--; this.waiters.shift()?.(); };
  }
}

export class ToolRunner {
  private queue: AsyncQueue<RunnerMessage> | null = null;
  private active = new Map<string, ActiveTool>();
  private generations = new Map<string, LoadedGeneration>();
  private invocationAborts = new Map<string, AbortController>();
  private heartbeatMs = 10_000;
  private semaphore: Semaphore;
  private currentGeneration = "";

  constructor(private readonly cfg: RunnerConfig) { this.semaphore = new Semaphore(cfg.concurrency); }

  async activate(generation: LoadedGeneration): Promise<void> {
    if (this.generations.has(generation.artifactDigest)) return;
    const nextKeys = new Set(generation.tools.map((tool) => keyOf(tool.manifest)));
    const newBytes = [...this.generations.values()].reduce((n, g) => n + g.sizeBytes, 0) + generation.sizeBytes;
    if (this.generations.size + 1 > this.cfg.maxGenerations || newBytes > this.cfg.maxLoadedBytes) {
      throw new Error(`generation capacity exceeded (${this.generations.size + 1}/${this.cfg.maxGenerations}, ${newBytes}/${this.cfg.maxLoadedBytes} bytes)`);
    }
    // Complete validation happened in loadGeneration. Only now publish any
    // registration, preserving atomic invalid-revision behavior.
    for (const tool of generation.tools) {
      const key = keyOf(tool.manifest);
      const existing = this.active.get(key);
      if (existing) { existing.generation = generation.artifactDigest; continue; }
      this.active.set(key, { tool, generation: generation.artifactDigest });
      this.send(registerMessage(this.cfg.runnerID, tool.manifest));
    }
    const draining: ToolVersionRef[] = [];
    for (const [key, current] of this.active) {
      if (!nextKeys.has(key) && current.drainingAt === undefined) {
        current.drainingAt = Date.now();
        draining.push(refFor(current.tool));
      }
    }
    this.generations.set(generation.artifactDigest, generation);
    this.currentGeneration = generation.artifactDigest;
    this.persistState();
    if (draining.length) this.send(create(RunnerMessageSchema, { kind: { case: "beginDrain", value: { versions: draining } } }));
    console.info(JSON.stringify({ event: "generation_activated", revision: generation.revision, digest: generation.artifactDigest, tools: generation.tools.length }));
  }

  async run(signal: AbortSignal): Promise<void> {
    let delay = 500;
    while (!signal.aborted) {
      try {
        await this.connect(signal);
        delay = 500;
      } catch (error) {
        if (signal.aborted) return;
        console.error(JSON.stringify({ event: "runner_stream_error", message: safeMessage(error) }));
        await sleep(delay, signal);
        delay = Math.min(delay * 2, 10_000);
      }
    }
  }

  private async connect(signal: AbortSignal): Promise<void> {
    const transport = createGrpcTransport({
      baseUrl: this.cfg.gatewayURL,
      nodeOptions: {
        ca: readFileSync(this.cfg.caFile), cert: readFileSync(this.cfg.certFile), key: readFileSync(this.cfg.keyFile),
        rejectUnauthorized: true, servername: this.cfg.serverName,
      } as Record<string, unknown>,
    });
    const client = createClient(RunnerService, transport);
    this.queue = new AsyncQueue<RunnerMessage>();
    const tools = [...this.active.values()];
    if (!tools.length) throw new Error("no valid generation is loaded");
    // The first message must be Register; re-register every retained version on
    // reconnect so pinned attempts remain routable where possible.
    for (const active of tools) this.queue.push(registerMessage(this.cfg.runnerID, active.tool.manifest));
    const draining = tools.filter((active) => active.drainingAt !== undefined).map((active) => refFor(active.tool));
    if (draining.length) this.queue.push(create(RunnerMessageSchema, { kind: { case: "beginDrain", value: { versions: draining } } }));
    const responses = client.registerRunner(this.queue, { signal });
    let timer: NodeJS.Timeout | undefined;
    try {
      for await (const control of responses) {
        switch (control.kind.case) {
          case "welcome":
            this.heartbeatMs = Math.max(1000, control.kind.value.heartbeatIntervalMs);
            if (!timer) { this.heartbeat(); timer = setInterval(() => this.heartbeat(), this.heartbeatMs); }
            break;
          case "invoke": void this.execute(control.kind.value); break;
          case "cancel": this.invocationAborts.get(control.kind.value.invocationId)?.abort(control.kind.value.reason); break;
          case "drainStatus": this.applyDrainStatus(control.kind.value.releasable); break;
        }
      }
      throw new Error("gateway closed runner stream");
    } finally {
      if (timer) clearInterval(timer);
      this.queue.close();
      this.queue = null;
    }
  }

  private heartbeat(): void {
    this.send(create(RunnerMessageSchema, { kind: { case: "heartbeat", value: { timestampMs: BigInt(Date.now()) } } }));
    const forced: ToolVersionRef[] = [];
    for (const active of this.active.values()) {
      if (active.drainingAt !== undefined && Date.now() - active.drainingAt >= this.cfg.drainMaxAgeMs) forced.push(refFor(active.tool));
    }
    if (forced.length) this.retire(forced, true);
  }

  private applyDrainStatus(releasable: ToolVersionRef[]): void {
    if (releasable.length) this.retire(releasable, false);
  }

  private retire(refs: ToolVersionRef[], forced: boolean): void {
    const retired: ToolVersionRef[] = [];
    for (const ref of refs) {
      const key = keyOf(ref);
      const active = this.active.get(key);
      if (!active?.drainingAt) continue;
      this.active.delete(key);
      retired.push(ref);
    }
    if (!retired.length) return;
    this.send(create(RunnerMessageSchema, { kind: { case: "retired", value: { versions: retired, forced } } }));
    this.reapUnusedGenerations();
  }

  private reapUnusedGenerations(): void {
    const used = new Set([...this.active.values()].map((active) => active.generation));
    used.add(this.currentGeneration);
    for (const digest of this.generations.keys()) if (!used.has(digest)) this.generations.delete(digest);
    this.persistState();
  }

  private persistState(): void {
    if (!this.cfg.stateFile) return;
    mkdirSync(dirname(this.cfg.stateFile), { recursive: true });
    const tmp = `${this.cfg.stateFile}.tmp`;
    writeFileSync(tmp, JSON.stringify({ current: this.currentGeneration, retained: [...this.generations.keys()] }));
    renameSync(tmp, this.cfg.stateFile);
  }

  private async execute(invoke: Invoke): Promise<void> {
    const abort = new AbortController();
    this.invocationAborts.set(invoke.invocationId, abort);
    let release: (() => void) | undefined;
    let timer: NodeJS.Timeout | undefined;
    let timedOut = false;
    try {
      release = await this.semaphore.acquire();
      if (abort.signal.aborted) throw new ToolError({ code: "canceled", message: "tool invocation canceled" });
      const timeout = descriptorTimeoutMs(invoke.descriptor);
      timer = setTimeout(() => { timedOut = true; abort.abort("tool timeout"); }, timeout);
      const descriptor = invoke.descriptor;
      if (!descriptor) throw new ToolError({ code: "invalid_dispatch", message: "invoke has no descriptor" });
      const active = this.active.get(keyOf(descriptor));
      if (!active) throw new ToolError({ code: "tool_unavailable", message: "pinned tool version is not loaded" });
      const argumentsValue = JSON.parse(new TextDecoder().decode(invoke.argumentsJson));
      const context: ToolInvocationContext = Object.freeze({
        invocationId: invoke.invocationId,
        idempotencyKey: invoke.idempotencyKey,
        credentials: Object.freeze(credentials(invoke.credentialContext?.slots ?? {})),
        artifacts: this.artifactAPI(invoke.invocationId, abort.signal),
        signal: abort.signal,
      });
      const output = await active.tool.module.invoke(context, argumentsValue);
      const resultJSON = new TextEncoder().encode(JSON.stringify(output.result ?? null));
      this.send(create(RunnerMessageSchema, { kind: { case: "invokeResult", value: {
        invocationId: invoke.invocationId, state: InvokeState.SUCCEEDED, resultJson: resultJSON,
        artifactOutputRefs: output.artifactRefs ?? [], timestampMs: BigInt(Date.now()),
      } } }));
    } catch (error) {
      const detail = typedToolError(error) ?? (timedOut ? { code: "timeout", message: "tool invocation timed out" } :
        { code: abort.signal.aborted ? "canceled" : "internal", message: abort.signal.aborted ? "tool invocation canceled" : "trusted tool failed" });
      this.send(create(RunnerMessageSchema, { kind: { case: "invokeResult", value: {
        invocationId: invoke.invocationId, state: InvokeState.FAILED, resultJson: new Uint8Array(), artifactOutputRefs: [],
        error: { code: detail.code, message: detail.message, retryability: detail.retryable ? Retryability.RETRYABLE : Retryability.NON_RETRYABLE,
          detailsJson: detail.details === undefined ? "" : safeJSON(detail.details) }, timestampMs: BigInt(Date.now()),
      } } }));
    } finally {
      if (timer) clearTimeout(timer);
      this.invocationAborts.delete(invoke.invocationId);
      release?.();
    }
  }

  private artifactAPI(invocationId: string, signal: AbortSignal): ToolArtifactAPI {
    const cfg = this.cfg;
    const transport = createGrpcTransport({ baseUrl: cfg.gatewayURL, nodeOptions: {
      ca: readFileSync(cfg.caFile), cert: readFileSync(cfg.certFile), key: readFileSync(cfg.keyFile), rejectUnauthorized: true, servername: cfg.serverName,
    } as Record<string, unknown> });
    const client = createClient(ArtifactService, transport);
    const context = { invocationId };
    return Object.freeze({
      async *read(ref: ArtifactRef): AsyncIterable<Uint8Array> {
        for await (const response of client.getArtifact({ context, artifactId: ref.artifactId }, { signal })) if (response.kind.case === "chunk") yield response.kind.value;
      },
      async write(input: Parameters<ToolArtifactAPI["write"]>[0]): Promise<ArtifactRef> {
        async function* requests() {
          yield { kind: { case: "init" as const, value: { context, mimeType: input.mimeType,
            ...(input.expectedSizeBytes === undefined ? {} : { expectedSizeBytes: input.expectedSizeBytes }), expectedDigest: input.expectedDigest ?? "" } } };
          for await (const chunk of input.bytes) yield { kind: { case: "chunk" as const, value: chunk } };
        }
        const response = await client.putArtifact(requests(), { signal });
        if (!response.metadata?.ref) throw new ToolError({ code: "artifact_write_failed", message: "artifact service returned no reference" });
        return response.metadata.ref;
      },
    });
  }

  private send(message: RunnerMessage): void {
    if (!this.queue) return; // retained state is re-registered on connect
    this.queue.push(message);
  }
}

function registerMessage(runnerID: string, manifest: ToolManifest): RunnerMessage {
  return create(RunnerMessageSchema, { kind: { case: "register", value: { runnerId: runnerID, descriptor: descriptor(manifest) } } });
}

function refFor(tool: LoadedTool): ToolVersionRef { return create(ToolVersionRefSchema, { name: tool.manifest.name, digest: tool.manifest.digest }); }

function descriptor(m: ToolManifest): ToolDescriptor {
  return create(ToolDescriptorSchema, {
    name: m.name, version: m.version, digest: m.digest, description: m.description,
    inputSchema: new TextEncoder().encode(JSON.stringify(m.inputSchema)), effectClass: effect(m.effectClass),
    credentialSlots: (m.credentialSlots ?? []).map((slot) => ({ name: slot.name, scheme: credentialScheme(slot.scheme), required: slot.required ?? false,
      bindingSchema: new TextEncoder().encode(JSON.stringify(slot.bindingSchema ?? {})) })),
    artifactCapabilities: { readsArtifacts: m.artifactCapabilities?.readsArtifacts ?? false, writesArtifacts: m.artifactCapabilities?.writesArtifacts ?? false,
      acceptedMimeTypes: m.artifactCapabilities?.acceptedMimeTypes ?? [] },
    timeout: { seconds: BigInt(Math.floor(m.timeoutMs / 1000)), nanos: (m.timeoutMs % 1000) * 1_000_000 },
    idempotencyProof: m.idempotencyProof ? { strategy: m.idempotencyProof.strategy, description: m.idempotencyProof.description ?? "", upstreamKeyHeader: m.idempotencyProof.upstreamKeyHeader } : undefined,
    consequenceSummaryTemplate: m.consequenceSummaryTemplate ? { localizedTemplates: m.consequenceSummaryTemplate.localizedTemplates, argumentPaths: m.consequenceSummaryTemplate.argumentPaths } : undefined,
  });
}

function effect(value: ToolManifest["effectClass"]): EffectClass {
  return value === "read_only" ? EffectClass.READ_ONLY : value === "idempotent_write" ? EffectClass.IDEMPOTENT_WRITE : EffectClass.NON_IDEMPOTENT_WRITE;
}
function credentialScheme(value: "bearer" | "oauth_client_credentials"): CredentialScheme {
  return value === "bearer" ? CredentialScheme.BEARER : CredentialScheme.OAUTH_CLIENT_CREDENTIALS;
}
function credentials(slots: Record<string, Credential>): Record<string, unknown> {
  return deepFreeze(Object.fromEntries(Object.entries(slots).map(([name, value]) => [name, value.scheme === CredentialScheme.BEARER ?
    { scheme: "bearer", value: value.bearerValue, resourceConstraints: parseJSON(value.resourceConstraintsJson) } :
    { scheme: "oauth_client_credentials", clientId: value.oauthClientId, clientSecret: value.oauthClientSecret, tokenUrl: value.oauthTokenUrl, scope: value.oauthScope, resourceConstraints: parseJSON(value.resourceConstraintsJson) }])));
}
function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}
function typedToolError(error: unknown): ToolErrorShape | undefined {
  if (error instanceof ToolError) return error.detail;
  if (!(error instanceof Error) || error.name !== "ToolError") return undefined;
  const candidate = error as Error & Partial<ToolErrorShape>;
  if (typeof candidate.code !== "string" || !candidate.code || typeof candidate.message !== "string") return undefined;
  if (candidate.retryable !== undefined && typeof candidate.retryable !== "boolean") return undefined;
  return { code: candidate.code, message: candidate.message, retryable: candidate.retryable, details: candidate.details };
}
function parseJSON(bytes: Uint8Array): unknown { return bytes.length ? JSON.parse(new TextDecoder().decode(bytes)) : {}; }
function safeJSON(value: unknown): string { try { return JSON.stringify(value) ?? ""; } catch { return ""; } }
function descriptorTimeoutMs(value: ToolDescriptor | undefined): number {
  if (!value?.timeout) return 60_000;
  return Math.max(1, Number(value.timeout.seconds) * 1000 + Math.floor(value.timeout.nanos / 1_000_000));
}
function safeMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
function sleep(ms: number, signal: AbortSignal): Promise<void> { return new Promise((resolve) => { const timer = setTimeout(resolve, ms); signal.addEventListener("abort", () => { clearTimeout(timer); resolve(); }, { once: true }); }); }
