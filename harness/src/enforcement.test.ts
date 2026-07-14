import { describe, it, expect } from "vitest";
import { toolOptions } from "./enforcement.js";

describe("toolOptions", () => {
  it("broad-default disables built-in coding tools, keeps extension tools", () => {
    expect(toolOptions("*")).toEqual({ noTools: "builtin" });
  });

  it("a specific list becomes the pi allowlist (only those enabled)", () => {
    expect(toolOptions(["graph_read", "excel_write"])).toEqual({ tools: ["graph_read", "excel_write"] });
  });
});
