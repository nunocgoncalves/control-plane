import { mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  ArtifactService, CredentialScheme, EffectClass, InvokeState, Retryability, RunnerService,
  RunnerMessageSchema, ToolDescriptorSchema, ToolVersionRefSchema,
  type ArtifactRef, type Credential, type Invoke, type RunnerMessage, type ToolDescriptor, type ToolVersionRef,
} from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { LoadedGeneration, LoadedTool, ToolManifest } from "./manifest.js";
import type { RunnerMetrics } from "./metrics.js";
import { AsyncQueue } from "./queue.js";
import { sleep } from "./sleep.js";
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
  readinessFile?: string;
}

interface ActiveTool { tool: LoadedTool; generation: string; drainingAt?: number }
interface RunnerSnapshot {
  active: Map<string, ActiveTool>;
  generations: Map<string, LoadedGeneration>;
  currentGeneration: string;
}
interface PendingActivation {
  generation: LoadedGeneration;
  requiredKeys: Set<string>;
  snapshot: RunnerSnapshot;
  resolve: () => void;
  reject: (error: Error) => void;
}
const keyOf = (ref: { name: string; digest: string }) => `${ref.name}\0${ref.digest}`;
const versionKeyOf = (ref: { name: string; version: string }) => `${ref.name}\0${ref.version}`;

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
  private versionDigests = new Map<string, string>();
  private acceptedKeys = new Set<string>();
  private registrationPending: Array<{ key: string; name: string }> = [];
  private pendingActivation: PendingActivation | null = null;
  private serving = false;

  constructor(private readonly cfg: RunnerConfig, private readonly metrics?: RunnerMetrics) {
    this.semaphore = new Semaphore(cfg.concurrency);
    this.metrics?.generationReady.set(0);
    if (cfg.readinessFile) rmSync(cfg.readinessFile, { force: true });
  }

  async activate(generation: LoadedGeneration): Promise<void> {
    if (this.generations.has(generation.artifactDigest)) return;
    if (this.pendingActivation) throw new Error("another generation activation is still awaiting gateway acceptance");

    // Immutable versions are process-lifetime identities, not just identities
    // within the currently loaded generations. Keep accepted identities after
    // retirement so a later conflicting rollback is rejected before changing
    // serving state or sending any registration to the gateway.
    for (const candidate of generation.tools) {
      const knownDigest = this.versionDigests.get(versionKeyOf(candidate.manifest));
      if (knownDigest !== undefined && knownDigest !== candidate.manifest.digest) {
        throw new Error(`${candidate.manifest.name}: immutable version ${candidate.manifest.version} was already accepted with digest ${knownDigest}`);
      }
    }

    // Drop generations no active/draining tool references before applying the
    // incoming capacity limit. Identical-tool and add-only revisions otherwise
    // consume slots forever despite having no pins.
    this.reapUnusedGenerations();
    const snapshot: RunnerSnapshot = {
      active: new Map([...this.active].map(([key, value]) => [key, { ...value }])),
      generations: new Map(this.generations),
      currentGeneration: this.currentGeneration,
    };
    const nextKeys = new Set(generation.tools.map((tool) => keyOf(tool.manifest)));
    const projected = new Map([...this.active].map(([key, value]) => [key, { ...value }]));
    for (const tool of generation.tools) {
      const key = keyOf(tool.manifest);
      const existing = projected.get(key);
      if (existing) existing.generation = generation.artifactDigest;
      else projected.set(key, { tool, generation: generation.artifactDigest });
    }
    for (const [key, current] of projected) {
      if (!nextKeys.has(key) && current.drainingAt === undefined) current.drainingAt = Date.now();
    }
    const projectedGenerations = new Map(this.generations).set(generation.artifactDigest, generation);
    const used = new Set([...projected.values()].map((active) => active.generation));
    used.add(generation.artifactDigest);
    for (const digest of projectedGenerations.keys()) if (!used.has(digest)) projectedGenerations.delete(digest);
    const projectedBytes = [...projectedGenerations.values()].reduce((n, g) => n + g.sizeBytes, 0);
    if (projectedGenerations.size > this.cfg.maxGenerations || projectedBytes > this.cfg.maxLoadedBytes) {
      throw new Error(`generation capacity exceeded (${projectedGenerations.size}/${this.cfg.maxGenerations}, ${projectedBytes}/${this.cfg.maxLoadedBytes} bytes)`);
    }

    this.active = projected;
    this.generations = projectedGenerations;
    const accepted = new Promise<void>((resolve, reject) => {
      this.pendingActivation = { generation, requiredKeys: nextKeys, snapshot, resolve, reject };
    });
    if (this.queue) {
      for (const tool of generation.tools) {
        const key = keyOf(tool.manifest);
        if (!this.acceptedKeys.has(key) && !this.registrationPending.some((pending) => pending.key === key)) this.sendRegistration(tool);
      }
    }
    this.checkActivationAccepted();
    return accepted;
  }

  async run(signal: AbortSignal): Promise<void> {
    let delay = 500;
    while (!signal.aborted) {
      try {
        await this.connect(signal);
        delay = 500;
      } catch (error) {
        if (signal.aborted) return;
        if (permanentRegistrationError(error)) this.rejectPendingActivation(error);
        this.metrics?.gatewayStreamErrors.inc();
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
    const tools = [...this.active.values()];
    if (!tools.length) throw new Error("no valid generation is loaded");
    this.queue = new AsyncQueue<RunnerMessage>();
    this.acceptedKeys.clear();
    this.registrationPending = [];
    // Re-register every retained version on reconnect so pinned attempts remain
    // routable. Registrations are tracked in send order because Welcome accepts
    // the first item and subsequent Ack messages identify only the tool name.
    for (const active of tools) this.sendRegistration(active.tool);
    const responses = client.registerRunner(this.queue, { signal });
    let timer: NodeJS.Timeout | undefined;
    try {
      for await (const control of responses) {
        switch (control.kind.case) {
          case "welcome":
            this.metrics?.gatewayConnected.set(1);
            this.acceptRegistration();
            this.heartbeatMs = Math.max(1000, control.kind.value.heartbeatIntervalMs);
            if (!timer) { this.heartbeat(); timer = setInterval(() => this.heartbeat(), this.heartbeatMs); }
            break;
          case "ack":
            if (control.kind.value.kind.case === "registered") this.acceptRegistration(control.kind.value.kind.value);
            break;
          case "invoke": void this.execute(control.kind.value); break;
          case "cancel": this.invocationAborts.get(control.kind.value.invocationId)?.abort(control.kind.value.reason); break;
          case "drainStatus": this.applyDrainStatus(control.kind.value.releasable); break;
        }
      }
      throw new Error("gateway closed runner stream");
    } finally {
      this.setServing(false);
      this.metrics?.gatewayConnected.set(0);
      if (timer) clearInterval(timer);
      this.queue.close();
      this.queue = null;
      this.acceptedKeys.clear();
      this.registrationPending = [];
    }
  }

  private sendRegistration(tool: LoadedTool): void {
    const key = keyOf(tool.manifest);
    this.registrationPending.push({ key, name: tool.manifest.name });
    this.send(registerMessage(this.cfg.runnerID, tool.manifest));
  }

  private acceptRegistration(name?: string): void {
    const accepted = this.registrationPending.shift();
    if (!accepted) throw new Error("gateway accepted an unexpected runner registration");
    if (name !== undefined && name !== accepted.name) throw new Error(`gateway acknowledged ${name} while ${accepted.name} was pending`);
    this.acceptedKeys.add(accepted.key);
    this.checkActivationAccepted();
    this.checkServing();
  }

  private checkActivationAccepted(): void {
    const pending = this.pendingActivation;
    if (!pending || [...pending.requiredKeys].some((key) => !this.acceptedKeys.has(key))) return;
    this.currentGeneration = pending.generation.artifactDigest;
    for (const tool of pending.generation.tools) this.versionDigests.set(versionKeyOf(tool.manifest), tool.manifest.digest);
    this.pendingActivation = null;
    this.reapUnusedGenerations();
    const draining = [...this.active.values()].filter((active) => active.drainingAt !== undefined).map((active) => refFor(active.tool));
    if (draining.length) this.send(create(RunnerMessageSchema, { kind: { case: "beginDrain", value: { versions: draining } } }));
    console.info(JSON.stringify({ event: "generation_activated", revision: pending.generation.revision, digest: pending.generation.artifactDigest, tools: pending.generation.tools.length }));
    pending.resolve();
    this.checkServing();
  }

  private checkServing(): void {
    if (!this.currentGeneration || !this.queue) return;
    const current = this.generations.get(this.currentGeneration);
    if (!current || current.tools.some((tool) => !this.acceptedKeys.has(keyOf(tool.manifest)))) return;
    this.setServing(true);
  }

  private rejectPendingActivation(error: unknown): void {
    const pending = this.pendingActivation;
    if (!pending) return;
    this.pendingActivation = null;
    this.active = pending.snapshot.active;
    this.generations = pending.snapshot.generations;
    this.currentGeneration = pending.snapshot.currentGeneration;
    this.persistState();
    this.updateStateMetrics();
    pending.reject(error instanceof Error ? error : new Error(String(error)));
  }

  private setServing(serving: boolean): void {
    if (this.serving === serving) return;
    this.serving = serving;
    this.metrics?.generationReady.set(serving ? 1 : 0);
    if (!this.cfg.readinessFile) return;
    if (!serving) {
      rmSync(this.cfg.readinessFile, { force: true });
      return;
    }
    mkdirSync(dirname(this.cfg.readinessFile), { recursive: true });
    const tmp = `${this.cfg.readinessFile}.tmp`;
    writeFileSync(tmp, this.currentGeneration);
    renameSync(tmp, this.cfg.readinessFile);
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
    this.updateStateMetrics();
  }

  private updateStateMetrics(): void {
    if (!this.metrics) return;
    this.metrics.loadedGenerations.set(this.generations.size);
    this.metrics.loadedBytes.set([...this.generations.values()].reduce((total, generation) => total + generation.sizeBytes, 0));
    this.metrics.registeredTools.set(this.active.size);
    this.metrics.drainingTools.set([...this.active.values()].filter((active) => active.drainingAt !== undefined).length);
  }

  private persistState(): void {
    if (!this.cfg.stateFile) return;
    mkdirSync(dirname(this.cfg.stateFile), { recursive: true });
    const tmp = `${this.cfg.stateFile}.tmp`;
    writeFileSync(tmp, JSON.stringify({ current: this.currentGeneration, retained: [...this.generations.keys()] }));
    renameSync(tmp, this.cfg.stateFile);
  }

  private async execute(invoke: Invoke): Promise<void> {
    this.metrics?.invocationsInFlight.inc();
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
      this.metrics?.invocations.labels("succeeded").inc();
      this.send(create(RunnerMessageSchema, { kind: { case: "invokeResult", value: {
        invocationId: invoke.invocationId, state: InvokeState.SUCCEEDED, resultJson: resultJSON,
        artifactOutputRefs: output.artifactRefs ?? [], timestampMs: BigInt(Date.now()),
      } } }));
    } catch (error) {
      const typed = typedToolError(error);
      const detail = typed ?? (timedOut ? { code: "timeout", message: "tool invocation timed out" } :
        { code: abort.signal.aborted ? "canceled" : "internal", message: abort.signal.aborted ? "tool invocation canceled" : "trusted tool failed" });
      this.metrics?.invocations.labels(typed ? "tool_error" : timedOut ? "timeout" : abort.signal.aborted ? "canceled" : "internal").inc();
      this.send(create(RunnerMessageSchema, { kind: { case: "invokeResult", value: {
        invocationId: invoke.invocationId, state: InvokeState.FAILED, resultJson: new Uint8Array(), artifactOutputRefs: [],
        error: { code: detail.code, message: detail.message, retryability: detail.retryable ? Retryability.RETRYABLE : Retryability.NON_RETRYABLE,
          detailsJson: detail.details === undefined ? "" : safeJSON(detail.details) }, timestampMs: BigInt(Date.now()),
      } } }));
    } finally {
      if (timer) clearTimeout(timer);
      this.invocationAborts.delete(invoke.invocationId);
      release?.();
      this.metrics?.invocationsInFlight.dec();
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
function permanentRegistrationError(error: unknown): boolean {
  const code = ConnectError.from(error).code;
  return code === Code.InvalidArgument || code === Code.PermissionDenied || code === Code.FailedPrecondition;
}
