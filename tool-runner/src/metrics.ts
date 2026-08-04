import { createServer, type Server } from "node:http";
import { Counter, Gauge, Registry } from "prom-client";

export class MaterializerMetrics {
  readonly registry = new Registry();
  readonly materializations = new Counter({
    name: "tool_runner_materializations_total",
    help: "Flux artifact materialization attempts by result.",
    labelNames: ["result"] as const,
    registers: [this.registry],
  });
  readonly lastSuccess = new Gauge({
    name: "tool_runner_materialization_last_success_timestamp_seconds",
    help: "Unix timestamp of the last successfully published Flux artifact generation.",
    registers: [this.registry],
  });
  readonly generationBytes = new Gauge({
    name: "tool_runner_materialized_generation_bytes",
    help: "Expanded tool bytes in the most recently materialized generation.",
    registers: [this.registry],
  });
  readonly currentArtifact = new Gauge({
    name: "tool_runner_materialized_artifact_info",
    help: "The current exact Flux artifact revision and digest.",
    labelNames: ["revision", "digest"] as const,
    registers: [this.registry],
  });

  success(revision: string, digest: string, bytes?: number): void {
    this.materializations.labels("success").inc();
    this.lastSuccess.set(Date.now() / 1000);
    if (bytes !== undefined) this.generationBytes.set(bytes);
    this.currentArtifact.reset();
    this.currentArtifact.labels(revision, digest).set(1);
  }

  failure(): void { this.materializations.labels("failure").inc(); }
}

export class RunnerMetrics {
  readonly registry = new Registry();
  readonly generationActivations = new Counter({
    name: "tool_runner_generation_activations_total",
    help: "Complete generation activation attempts by result.",
    labelNames: ["result"] as const,
    registers: [this.registry],
  });
  readonly generationReady = new Gauge({
    name: "tool_runner_generation_ready",
    help: "Whether at least one complete valid generation has been activated.",
    registers: [this.registry],
  });
  readonly loadedGenerations = new Gauge({
    name: "tool_runner_loaded_generations",
    help: "Number of retained immutable generations loaded by the runner.",
    registers: [this.registry],
  });
  readonly loadedBytes = new Gauge({
    name: "tool_runner_loaded_bytes",
    help: "Manifest and bundle bytes retained by the runner.",
    registers: [this.registry],
  });
  readonly registeredTools = new Gauge({
    name: "tool_runner_registered_tool_versions",
    help: "Exact name and digest registrations currently retained by the runner.",
    registers: [this.registry],
  });
  readonly drainingTools = new Gauge({
    name: "tool_runner_draining_tool_versions",
    help: "Retained tool versions unavailable to new attempt snapshots while old pins drain.",
    registers: [this.registry],
  });
  readonly gatewayConnected = new Gauge({
    name: "tool_runner_gateway_connected",
    help: "Whether the outbound mTLS gateway stream is connected and welcomed.",
    registers: [this.registry],
  });
  readonly gatewayStreamErrors = new Counter({
    name: "tool_runner_gateway_stream_errors_total",
    help: "Outbound gateway stream failures and reconnects.",
    registers: [this.registry],
  });
  readonly invocations = new Counter({
    name: "tool_runner_invocations_total",
    help: "Trusted tool invocations by bounded result class.",
    labelNames: ["result"] as const,
    registers: [this.registry],
  });
  readonly invocationsInFlight = new Gauge({
    name: "tool_runner_invocations_in_flight",
    help: "Trusted tool invocations executing or waiting for the global concurrency bound.",
    registers: [this.registry],
  });
}

export async function startMetricsServer(registry: Registry, port: number, signal: AbortSignal): Promise<Server> {
  const server = createServer(async (request, response) => {
    if (request.method !== "GET" || request.url?.split("?", 1)[0] !== "/metrics") {
      response.writeHead(404).end();
      return;
    }
    try {
      const payload = await registry.metrics();
      response.writeHead(200, { "Content-Type": registry.contentType });
      response.end(payload);
    } catch {
      response.writeHead(500).end();
    }
  });
  server.requestTimeout = 10_000;
  server.headersTimeout = 5_000;
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    server.once("error", onError);
    server.listen(port, "0.0.0.0", () => { server.off("error", onError); resolve(); });
  });
  signal.addEventListener("abort", () => server.close(), { once: true });
  return server;
}
