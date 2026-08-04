import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import {
  CredentialScheme, InvokeSchema, InvokeState, RunnerMessageSchema, ToolDescriptorSchema, ToolVersionRefSchema,
  type ToolVersionRef,
} from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { LoadedGeneration, ToolManifest } from "./manifest.js";
import { AsyncQueue } from "./queue.js";
import { ToolRunner, type RunnerConfig } from "./runner.js";
import type { ToolInvocationContext, ToolModule } from "./types.js";

const manifest: ToolManifest = {
  apiVersion: "iterabase.io/tool/v1", name: "test.echo", version: "1.0.0",
  digest: `sha256:${"1".repeat(64)}`, description: "Echo", bundle: "index.mjs",
  inputSchema: { type: "object" }, effectClass: "read_only", timeoutMs: 1000,
};
const config: RunnerConfig = {
  gatewayURL: "https://gateway", serverName: "gateway", caFile: "unused", certFile: "unused", keyFile: "unused",
  runnerID: "test", concurrency: 1, maxGenerations: 8, maxLoadedBytes: 1024 * 1024, drainMaxAgeMs: 60_000,
};

function generation(module: ToolModule): LoadedGeneration {
  return { revision: "main@sha1:test", artifactDigest: "sha256:artifact", directory: "/readonly", sizeBytes: 10,
    tools: [{ manifest, module, directory: "/readonly/tools/product/echo", sizeBytes: 10, layer: "product" }] };
}

function acknowledgeDrainWithoutGateway(runner: ToolRunner): void {
  const internal = runner as unknown as {
    active: Map<string, { tool: LoadedGeneration["tools"][number]; drainingAt?: number }>;
    applyDrainStatus: (pinned: ToolVersionRef[], releasable: ToolVersionRef[]) => void;
  };
  const draining = [...internal.active.values()]
    .filter((active) => active.drainingAt !== undefined)
    .map((active) => create(ToolVersionRefSchema, { name: active.tool.manifest.name, digest: active.tool.manifest.digest }));
  if (draining.length) internal.applyDrainStatus(draining, []);
}

async function acceptWithoutGateway(runner: ToolRunner, loaded: LoadedGeneration): Promise<void> {
  const activation = runner.activate(loaded);
  const internal = runner as unknown as { acceptedKeys: Set<string>; checkActivationAccepted: () => void };
  for (const tool of loaded.tools) internal.acceptedKeys.add(`${tool.manifest.name}\0${tool.manifest.digest}`);
  internal.checkActivationAccepted();
  acknowledgeDrainWithoutGateway(runner);
  await activation;
}

async function invoke(module: ToolModule) {
  const runner = new ToolRunner(config);
  await acceptWithoutGateway(runner, generation(module));
  const queue = new AsyncQueue<ReturnType<typeof create<typeof RunnerMessageSchema>>>();
  const internal = runner as unknown as {
    queue: typeof queue;
    artifactAPI: () => Readonly<Record<string, never>>;
    execute: (request: ReturnType<typeof create<typeof InvokeSchema>>) => Promise<void>;
  };
  internal.queue = queue;
  internal.artifactAPI = () => Object.freeze({});
  const response = internal.execute(create(InvokeSchema, {
    invocationId: "inv-1", idempotencyKey: "stable-key", argumentsJson: new TextEncoder().encode('{"value":7}'),
    descriptor: create(ToolDescriptorSchema, { name: manifest.name, version: manifest.version, digest: manifest.digest }),
    credentialContext: { slots: { api: { scheme: CredentialScheme.BEARER, bearerValue: "secret", resourceConstraintsJson: new TextEncoder().encode('{"tenant":"one"}') } } },
  }));
  const message = await queue[Symbol.asyncIterator]().next();
  await response;
  return message.value;
}

describe("generation lifecycle", () => {
  it("rejects immutable version reuse before replacing the valid generation", async () => {
    const runner = new ToolRunner(config);
    const module: ToolModule = { identity: { name: manifest.name, version: manifest.version }, async invoke() { return { result: {} }; } };
    await acceptWithoutGateway(runner, generation(module));
    const conflictManifest = { ...manifest, digest: `sha256:${"2".repeat(64)}` };
    const conflict: LoadedGeneration = {
      ...generation(module), artifactDigest: "sha256:conflict",
      tools: [{ ...generation(module).tools[0], manifest: conflictManifest }],
    };
    await expect(runner.activate(conflict)).rejects.toThrow("immutable version 1.0.0 was already accepted");
    const internal = runner as unknown as { currentGeneration: string; active: Map<string, unknown> };
    expect(internal.currentGeneration).toBe("sha256:artifact");
    expect(internal.active.size).toBe(1);
  });

  it("remembers immutable version digests after a generation retires", async () => {
    const runner = new ToolRunner(config);
    const module: ToolModule = { identity: { name: manifest.name, version: manifest.version }, async invoke() { return { result: {} }; } };
    await acceptWithoutGateway(runner, generation(module));
    const nextManifest = { ...manifest, version: "2.0.0", digest: `sha256:${"2".repeat(64)}` };
    await acceptWithoutGateway(runner, {
      ...generation(module), revision: "main@sha1:next", artifactDigest: "sha256:next",
      tools: [{ ...generation(module).tools[0], manifest: nextManifest }],
    });
    const internal = runner as unknown as { retire: (refs: Array<{ name: string; digest: string }>, forced: boolean) => void };
    internal.retire([{ name: manifest.name, digest: manifest.digest }], false);

    const conflictManifest = { ...manifest, digest: `sha256:${"3".repeat(64)}` };
    const conflict: LoadedGeneration = {
      ...generation(module), artifactDigest: "sha256:conflict-after-retirement",
      tools: [{ ...generation(module).tools[0], manifest: conflictManifest }],
    };
    await expect(runner.activate(conflict)).rejects.toThrow("immutable version 1.0.0 was already accepted");
  });

  it("reactivates a still-loaded draining version on rollback", async () => {
    const runner = new ToolRunner(config);
    const module: ToolModule = { identity: { name: manifest.name, version: manifest.version }, async invoke() { return { result: {} }; } };
    const first = generation(module);
    await acceptWithoutGateway(runner, first);
    const nextManifest = { ...manifest, version: "2.0.0", digest: `sha256:${"2".repeat(64)}` };
    await acceptWithoutGateway(runner, {
      ...first, revision: "main@sha1:next", artifactDigest: "sha256:next",
      tools: [{ ...first.tools[0], manifest: nextManifest }],
    });

    const queue = new AsyncQueue<ReturnType<typeof create<typeof RunnerMessageSchema>>>();
    const internal = runner as unknown as {
      queue: typeof queue;
      active: Map<string, { drainingAt?: number }>;
      applyDrainStatus: (pinned: ToolVersionRef[], releasable: ToolVersionRef[]) => void;
    };
    internal.queue = queue;
    let activated = false;
    const activation = runner.activate({
      ...first, revision: "main@sha1:rollback", artifactDigest: "sha256:rollback",
    }).then(() => { activated = true; });

    await Promise.resolve();
    expect(activated).toBe(false);
    expect(internal.active.get(`${manifest.name}\0${manifest.digest}`)?.drainingAt).toBeUndefined();
    expect(internal.active.get(`${nextManifest.name}\0${nextManifest.digest}`)?.drainingAt).toEqual(expect.any(Number));
    const message = await queue[Symbol.asyncIterator]().next();
    if (message.done || message.value.kind.case !== "beginDrain") throw new Error("rollback did not publish drain state");
    expect(message.value.kind.value.versions.map((ref) => ({ name: ref.name, digest: ref.digest }))).toEqual([
      { name: nextManifest.name, digest: nextManifest.digest },
    ]);
    internal.applyDrainStatus([
      create(ToolVersionRefSchema, { name: nextManifest.name, digest: nextManifest.digest }),
    ], []);
    await activation;
    expect(activated).toBe(true);
  });

  it("re-registers an exact version reintroduced after retirement", async () => {
    const runner = new ToolRunner(config);
    const module: ToolModule = { identity: { name: manifest.name, version: manifest.version }, async invoke() { return { result: {} }; } };
    const first = generation(module);
    await acceptWithoutGateway(runner, first);
    const nextManifest = { ...manifest, version: "2.0.0", digest: `sha256:${"2".repeat(64)}` };
    await acceptWithoutGateway(runner, {
      ...first, revision: "main@sha1:next", artifactDigest: "sha256:next",
      tools: [{ ...first.tools[0], manifest: nextManifest }],
    });
    const internal = runner as unknown as {
      queue: AsyncQueue<ReturnType<typeof create<typeof RunnerMessageSchema>>>;
      retire: (refs: Array<{ name: string; digest: string }>, forced: boolean) => void;
      acceptRegistration: (name?: string) => void;
    };
    internal.retire([{ name: manifest.name, digest: manifest.digest }], false);
    const queue = new AsyncQueue<ReturnType<typeof create<typeof RunnerMessageSchema>>>();
    internal.queue = queue;

    const activation = runner.activate({
      ...first, revision: "main@sha1:rollback", artifactDigest: "sha256:rollback",
    });
    const message = await queue[Symbol.asyncIterator]().next();
    if (message.done || message.value.kind.case !== "register") throw new Error("reintroduced version was not registered");
    expect(message.value.kind.value.descriptor?.digest).toBe(manifest.digest);
    internal.acceptRegistration(manifest.name);
    acknowledgeDrainWithoutGateway(runner);
    await activation;
  });

  it("reclaims unreferenced identical-tool generations before capacity checks", async () => {
    const runner = new ToolRunner({ ...config, maxGenerations: 1 });
    const module: ToolModule = { identity: { name: manifest.name, version: manifest.version }, async invoke() { return { result: {} }; } };
    await acceptWithoutGateway(runner, generation(module));
    await acceptWithoutGateway(runner, { ...generation(module), revision: "main@sha1:next", artifactDigest: "sha256:next" });
    const internal = runner as unknown as { generations: Map<string, LoadedGeneration> };
    expect([...internal.generations.keys()]).toEqual(["sha256:next"]);
  });

  it("clears stale readiness and publishes it only while a generation is serving", async () => {
    const directory = mkdtempSync(join(tmpdir(), "runner-ready-"));
    const readinessFile = join(directory, "runner-ready");
    writeFileSync(readinessFile, "stale");
    const runner = new ToolRunner({ ...config, readinessFile });
    expect(existsSync(readinessFile)).toBe(false);
    const internal = runner as unknown as {
      queue: AsyncQueue<ReturnType<typeof create<typeof RunnerMessageSchema>>>;
      setServing: (serving: boolean) => void;
    };
    internal.queue = new AsyncQueue();
    const module: ToolModule = { identity: { name: manifest.name, version: manifest.version }, async invoke() { return { result: {} }; } };
    await acceptWithoutGateway(runner, generation(module));
    expect(readFileSync(readinessFile, "utf8")).toBe("sha256:artifact");
    internal.setServing(false);
    expect(existsSync(readinessFile)).toBe(false);
    rmSync(directory, { recursive: true, force: true });
  });
});

describe("trusted tool execution", () => {
  it("supplies parsed arguments and a deeply frozen invocation context", async () => {
    let observed: ToolInvocationContext | undefined;
    const message = await invoke({ identity: { name: manifest.name, version: manifest.version }, async invoke(context, args) {
      observed = context;
      return { result: args };
    } });
    expect(message.kind.case).toBe("invokeResult");
    if (message.kind.case !== "invokeResult") throw new Error("missing result");
    expect(message.kind.value.state).toBe(InvokeState.SUCCEEDED);
    expect(JSON.parse(new TextDecoder().decode(message.kind.value.resultJson))).toEqual({ value: 7 });
    expect(observed?.idempotencyKey).toBe("stable-key");
    expect(Object.isFrozen(observed)).toBe(true);
    expect(Object.isFrozen(observed?.credentials.api)).toBe(true);
  });

  it("returns explicit ToolError fields but redacts unknown exceptions", async () => {
    class ReviewedError extends Error { name = "ToolError"; code = "upstream_denied"; retryable = true; }
    const typed = await invoke({ identity: { name: manifest.name, version: manifest.version }, async invoke() { throw new ReviewedError("safe denial"); } });
    if (typed.kind.case !== "invokeResult") throw new Error("missing typed result");
    expect(typed.kind.value.error?.code).toBe("upstream_denied");
    expect(typed.kind.value.error?.message).toBe("safe denial");

    const unknown = await invoke({ identity: { name: manifest.name, version: manifest.version }, async invoke() { throw new Error("secret upstream detail"); } });
    if (unknown.kind.case !== "invokeResult") throw new Error("missing unknown result");
    expect(unknown.kind.value.error?.code).toBe("internal");
    expect(unknown.kind.value.error?.message).toBe("trusted tool failed");
  });
});
