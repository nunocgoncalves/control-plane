import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { describe, expect, it } from "vitest";
import { MaterializerMetrics, RunnerMetrics, startMetricsServer } from "./metrics.js";

describe("Prometheus metrics", () => {
  it("exports bounded materializer and runner metrics", async () => {
    const materializer = new MaterializerMetrics();
    materializer.failure();
    materializer.success("main@sha1:abc", `sha256:${"a".repeat(64)}`, 42);
    const materialized = await materializer.registry.metrics();
    expect(materialized).toContain('tool_runner_materializations_total{result="failure"} 1');
    expect(materialized).toContain("tool_runner_materialized_generation_bytes 42");

    const runner = new RunnerMetrics();
    runner.generationActivations.labels("failure").inc();
    runner.generationReady.set(1);
    runner.invocations.labels("succeeded").inc();
    const rendered = await runner.registry.metrics();
    expect(rendered).toContain('tool_runner_generation_activations_total{result="failure"} 1');
    expect(rendered).toContain("tool_runner_generation_ready 1");
    expect(rendered).toContain('tool_runner_invocations_total{result="succeeded"} 1');
  });

  it("serves only the Prometheus endpoint", async () => {
    const metrics = new RunnerMetrics();
    metrics.gatewayConnected.set(1);
    const abort = new AbortController();
    const server = await startMetricsServer(metrics.registry, 0, abort.signal);
    const port = (server.address() as AddressInfo).port;
    const response = await fetch(`http://127.0.0.1:${port}/metrics`);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/plain");
    expect(await response.text()).toContain("tool_runner_gateway_connected 1");
    expect((await fetch(`http://127.0.0.1:${port}/invoke`)).status).toBe(404);
    abort.abort();
    await once(server, "close");
  });
});
