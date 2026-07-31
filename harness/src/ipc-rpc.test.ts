import { describe, it, expect } from "vitest";
import {
  parseChildRpcFrame,
  parseSupervisorRpcFrame,
  encodeFrame,
  FrameReader,
  type GatewayToolDescriptor,
} from "./ipc.js";

describe("parseChildRpcFrame (fd 4)", () => {
  it("parses a modelRequest", () => {
    const f = parseChildRpcFrame({ type: "modelRequest", requestId: "r1", body: { model: "m" } });
    expect(f).toEqual({ type: "modelRequest", requestId: "r1", body: { model: "m" } });
  });
  it("parses a toolCall with idempotencyKey", () => {
    const f = parseChildRpcFrame({
      type: "toolCall",
      requestId: "r2",
      toolCallId: "tc1",
      toolName: "graph.read_mail",
      toolVersionDigest: "sha256:abc",
      argumentsJson: "{}",
      idempotencyKey: "tc1",
    });
    expect(f?.type).toBe("toolCall");
    expect(f && f.type === "toolCall" && f.idempotencyKey).toBe("tc1");
  });
  it("parses a cancel", () => {
    expect(parseChildRpcFrame({ type: "cancel", requestId: "r1" })).toEqual({ type: "cancel", requestId: "r1" });
  });
  it("rejects a toolCall missing required fields", () => {
    expect(parseChildRpcFrame({ type: "toolCall", requestId: "r", toolCallId: "x" })).toBeNull();
  });
  it("rejects an unknown type / missing requestId", () => {
    expect(parseChildRpcFrame({ type: "bogus", requestId: "r" })).toBeNull();
    expect(parseChildRpcFrame({ type: "modelRequest" })).toBeNull();
  });
});

describe("parseSupervisorRpcFrame (fd 5)", () => {
  const desc: GatewayToolDescriptor = {
    name: "graph.read_mail",
    version: "1.0.0",
    digest: "sha256:abc",
    description: "read mail",
    inputSchema: { type: "object" },
    effectClass: "read_only",
  };
  it("parses a gatewayTools frame", () => {
    const f = parseSupervisorRpcFrame({ type: "gatewayTools", descriptors: [desc] });
    expect(f?.type).toBe("gatewayTools");
    expect(f && f.type === "gatewayTools" && f.descriptors[0]?.name).toBe("graph.read_mail");
  });
  it("rejects an invalid effectClass", () => {
    expect(parseSupervisorRpcFrame({ type: "gatewayTools", descriptors: [{ ...desc, effectClass: "bogus" }] })).toBeNull();
  });
  it("parses modelChunk/modelEnd/toolResult/cancel", () => {
    expect(parseSupervisorRpcFrame({ type: "modelChunk", requestId: "r", data: "{}" })?.type).toBe("modelChunk");
    expect(parseSupervisorRpcFrame({ type: "modelEnd", requestId: "r", status: "ok" })?.type).toBe("modelEnd");
    expect(parseSupervisorRpcFrame({ type: "modelEnd", requestId: "r", status: "weird" })).toBeNull();
    expect(parseSupervisorRpcFrame({ type: "toolResult", requestId: "r", isError: false, resultJson: "{}" })?.type).toBe("toolResult");
    expect(parseSupervisorRpcFrame({ type: "cancel", requestId: "r" })?.type).toBe("cancel");
  });
});

describe("RPC frame round-trip via FrameReader", () => {
  it("frames and parses a modelRequest", () => {
    const buf = encodeFrame({ type: "modelRequest", requestId: "r1", body: { model: "m" } });
    let got: unknown = null;
    const r = new FrameReader((raw) => (got = raw));
    r.feed(buf);
    expect(parseChildRpcFrame(got)?.type).toBe("modelRequest");
  });
});
