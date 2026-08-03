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
  it("parses a structured stepCompletion report", () => {
    const f = parseChildRpcFrame({ type: "stepCompletion", requestId: "r3", outcome: "approved", summary: "done", outputJson: "{}", artifactRefs: [] });
    expect(f?.type).toBe("stepCompletion");
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
    expect(parseSupervisorRpcFrame({ type: "stepCompletionAck", requestId: "r" })?.type).toBe("stepCompletionAck");
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

describe("FrameReader strict mode (fd-4 fail-closed, HOR-395)", () => {
  it("reports malformed JSON to onError and stops (does not silently drop)", () => {
    const errors: string[] = [];
    const frames: unknown[] = [];
    const r = new FrameReader((raw) => frames.push(raw), (reason) => errors.push(reason));
    // A valid frame followed by a frame whose body is not valid JSON.
    r.feed(encodeFrame({ type: "cancel", requestId: "r1" }));
    const bad = Buffer.concat([Buffer.from([0, 0, 0, 4]), Buffer.from("nope", "utf8")]);
    r.feed(bad);
    expect(frames).toHaveLength(1);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/JSON/);
  });

  it("reports an oversized length prefix to onError", () => {
    const errors: string[] = [];
    const r = new FrameReader(() => {}, (reason) => errors.push(reason));
    const huge = Buffer.alloc(4);
    huge.writeUInt32BE(0xffffffff, 0);
    r.feed(huge);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/MAX_FRAME_BYTES/);
  });

  it("reports a truncated trailing frame on end()", () => {
    const errors: string[] = [];
    const r = new FrameReader(() => {}, (reason) => errors.push(reason));
    // A length prefix announcing 10 bytes, but only 3 arrive before end.
    const partial = Buffer.concat([Buffer.from([0, 0, 0, 10]), Buffer.from("abc", "utf8")]);
    r.feed(partial);
    expect(errors).toHaveLength(0);
    r.end();
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/truncated/);
  });

  it("stays lenient (no onError): drops malformed JSON without failing", () => {
    const frames: unknown[] = [];
    const r = new FrameReader((raw) => frames.push(raw));
    r.feed(Buffer.concat([Buffer.from([0, 0, 0, 4]), Buffer.from("nope", "utf8")]));
    r.feed(encodeFrame({ type: "cancel", requestId: "r1" }));
    expect(frames).toHaveLength(1);
  });
});
