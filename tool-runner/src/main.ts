import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { FluxMaterializer } from "./materializer.js";
import { toolDigest } from "./canonical.js";
import { loadGeneration } from "./manifest.js";
import { MaterializerMetrics, RunnerMetrics, startMetricsServer } from "./metrics.js";
import { ToolRunner, type RunnerConfig } from "./runner.js";

const abort = new AbortController();
process.on("SIGTERM", () => abort.abort());
process.on("SIGINT", () => abort.abort());

async function main(): Promise<void> {
  switch (process.argv[2]) {
    case "run": await run(); return;
    case "materialize": await materialize(); return;
    case "validate": await validate(process.argv[3]); return;
    case "digest": await digest(process.argv[3]); return;
    default: throw new Error("usage: tool-runner <run|materialize|validate|digest> [directory]");
  }
}

async function run(): Promise<void> {
  const controlRoot = env("CONTROL_ROOT", "/control");
  const cfg: RunnerConfig = {
    gatewayURL: env("TOOL_GATEWAY_URL"), serverName: env("TOOL_GATEWAY_SERVER_NAME"),
    caFile: env("TOOL_GATEWAY_CA_FILE"), certFile: env("TOOL_RUNNER_CERT_FILE"), keyFile: env("TOOL_RUNNER_KEY_FILE"),
    runnerID: env("TOOL_RUNNER_ID", "overlay-tools"), concurrency: positiveInt("TOOL_RUNNER_CONCURRENCY", 8),
    maxGenerations: positiveInt("TOOL_RUNNER_MAX_GENERATIONS", 8), maxLoadedBytes: positiveInt("TOOL_RUNNER_MAX_LOADED_BYTES", 512 * 1024 * 1024),
    drainMaxAgeMs: durationMs("TOOL_RUNNER_DRAIN_MAX_AGE", 24 * 60 * 60 * 1000), stateFile: `${controlRoot}/runner-state.json`,
  };
  const metrics = new RunnerMetrics();
  await startMetricsServer(metrics.registry, tcpPort("TOOL_RUNNER_METRICS_PORT", 9092), abort.signal);
  const runner = new ToolRunner(cfg, metrics);
  const stream = runner.run(abort.signal);
  let last = "";
  while (!abort.signal.aborted) {
    try {
      const current = JSON.parse(await readFile(`${controlRoot}/current.json`, "utf8")) as { revision: string; artifactDigest: string; directory: string };
      if (current.artifactDigest !== last) {
        const generation = await loadGeneration(current.directory, current);
        await runner.activate(generation);
        last = current.artifactDigest;
        metrics.generationActivations.labels("success").inc();
        metrics.generationReady.set(1);
        await writeFile(`${controlRoot}/runner-ready`, current.artifactDigest);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        metrics.generationActivations.labels("failure").inc();
        console.error(JSON.stringify({ event: "generation_rejected", message: error instanceof Error ? error.message : String(error) }));
      }
    }
    await sleep(1000, abort.signal);
  }
  await stream;
}

async function materialize(): Promise<void> {
  const metrics = new MaterializerMetrics();
  await startMetricsServer(metrics.registry, tcpPort("MATERIALIZER_METRICS_PORT", 9091), abort.signal);
  const instance = new FluxMaterializer({
    namespace: env("FLUX_SOURCE_NAMESPACE", "flux-system"), sourceName: env("FLUX_SOURCE_NAME", "overlay"),
    artifactsRoot: env("ARTIFACTS_ROOT", "/artifacts"), controlRoot: env("CONTROL_ROOT", "/control"),
    pollMs: positiveInt("FLUX_POLL_MS", 5000), maxArchiveBytes: positiveInt("FLUX_MAX_ARCHIVE_BYTES", 512 * 1024 * 1024),
    maxExtractedBytes: positiveInt("TOOL_RUNNER_MAX_LOADED_BYTES", 512 * 1024 * 1024), maxGenerations: positiveInt("TOOL_RUNNER_MAX_GENERATIONS", 8),
  }, undefined, metrics);
  await writeFile(`${env("CONTROL_ROOT", "/control")}/materializer-ready`, "starting");
  await instance.run(abort.signal);
}

async function digest(path: string | undefined): Promise<void> {
  if (!path) throw new Error("digest requires a tool directory");
  const directory = resolve(path);
  const manifest = JSON.parse(await readFile(`${directory}/manifest.json`, "utf8")) as Record<string, unknown>;
  delete manifest.digest;
  const bundle = await readFile(`${directory}/index.mjs`);
  console.log(toolDigest(manifest, bundle));
}

async function validate(path: string | undefined): Promise<void> {
  if (!path) throw new Error("validate requires a generation directory");
  const directory = resolve(path);
  const generation = await loadGeneration(directory, { revision: "local-validation", artifactDigest: "local-validation" });
  console.log(JSON.stringify({ valid: true, tools: generation.tools.map((tool) => ({ name: tool.manifest.name, version: tool.manifest.version, digest: tool.manifest.digest })) }, null, 2));
}

function env(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (!value) throw new Error(`${name} is required`);
  return value;
}
function positiveInt(name: string, fallback: number): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer and cannot be disabled`);
  return value;
}
function tcpPort(name: string, fallback: number): number {
  const value = positiveInt(name, fallback);
  if (value > 65535) throw new Error(`${name} must be a valid TCP port`);
  return value;
}
function durationMs(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const match = /^(\d+)(ms|s|m|h)$/.exec(raw);
  if (!match) throw new Error(`${name} must be a positive duration (ms|s|m|h)`);
  const unit = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[match[2] as "ms" | "s" | "m" | "h"];
  const value = Number(match[1]) * unit;
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be positive and cannot be disabled`);
  return value;
}
function sleep(ms: number, signal: AbortSignal): Promise<void> { return new Promise((resolve) => { const timer = setTimeout(resolve, ms); signal.addEventListener("abort", () => { clearTimeout(timer); resolve(); }, { once: true }); }); }

main().catch((error) => { console.error(error instanceof Error ? error.stack : error); process.exitCode = 1; });
