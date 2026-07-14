import { defineConfig } from "vitest/config";
import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";

// The generated harness_connect.js imports "./harness_pb.js", but protoc-gen-es
// emits harness_pb.ts (tsc resolves .js->.ts; vite does not). This shim rewrites
// a .js specifier to its .ts source when the .js doesn't exist — test-only;
// the tsc build (dist/) compiles both to .js, so runtime is unaffected.
function jsToTs() {
  return {
    name: "js-to-ts-shim",
    resolveId(source: string, importer?: string) {
      if (!importer || !source.endsWith(".js")) return null;
      const ts = source.slice(0, -3) + ".ts";
      const resolved = resolve(dirname(importer), ts);
      if (existsSync(resolved)) return resolved;
      return null;
    },
  };
}

export default defineConfig({
  plugins: [jsToTs()],
  test: { include: ["src/**/*.test.ts"] },
});
