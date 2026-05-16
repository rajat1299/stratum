/**
 * Vitest config — split out of vite.config.ts because vitest 2 still pins
 * to vite 5 internally and that vite-5 type bleeds through `defineConfig`,
 * fighting our runtime vite 7 under exactOptionalPropertyTypes. We don't
 * pass plugins here because vitest's built-in esbuild handles TS + JSX
 * fine for unit tests — we don't need React Refresh in a test runner.
 *
 * Once vitest v3 lands (vite 7-compatible), this can collapse back into
 * vite.config.ts and the react plugin can be shared.
 */

import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: {
    jsx: "automatic",
    jsxImportSource: "react",
  },
  test: {
    environment: "happy-dom",
    globals: true,
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    css: false,
  },
});
