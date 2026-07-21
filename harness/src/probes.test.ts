import { describe, it, expect, afterEach } from "vitest";
import type { AddressInfo } from "node:net";
import { Probes } from "./probes.js";

async function get(port: number, path: string): Promise<{ status: number; body: string }> {
  const res = await fetch(`http://127.0.0.1:${port}${path}`);
  return { status: res.status, body: await res.text() };
}

describe("Probes", () => {
  let probes: Probes;
  let port: number;

  afterEach(async () => {
    await probes.stop();
  });

  it("/healthz is 200 by default; /readyz is 503 until ready", async () => {
    probes = new Probes();
    const srv = await probes.start(0);
    port = (srv.address() as AddressInfo).port;
    expect((await get(port, "/healthz")).status).toBe(200);
    expect((await get(port, "/readyz")).status).toBe(503);
  });

  it("/readyz flips to 200 when ready", async () => {
    probes = new Probes();
    const srv = await probes.start(0);
    port = (srv.address() as AddressInfo).port;
    probes.setReady(true);
    expect((await get(port, "/readyz")).status).toBe(200);
    expect((await get(port, "/readyz")).body).toBe("ok");
    probes.setReady(false);
    expect((await get(port, "/readyz")).status).toBe(503);
  });

  it("/healthz flips to 503 when unhealthy (fatal)", async () => {
    probes = new Probes();
    const srv = await probes.start(0);
    port = (srv.address() as AddressInfo).port;
    probes.setHealthy(false);
    expect((await get(port, "/healthz")).status).toBe(503);
  });

  it("returns 404 for unknown paths", async () => {
    probes = new Probes();
    const srv = await probes.start(0);
    port = (srv.address() as AddressInfo).port;
    expect((await get(port, "/whatever")).status).toBe(404);
  });
});
