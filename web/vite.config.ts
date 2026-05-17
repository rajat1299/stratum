import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// TanStackRouterVite() is intentionally not registered yet. Phase A2 ships
// with code-based routing (web/src/router.tsx) so there is no src/routes/
// tree for the file-based plugin to scan. When we migrate to file-based
// routing (likely Phase D once the route count grows), re-add the plugin.

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    rollupOptions: {
      output: {
        // Long-lived vendor split, locked in before D2 ships so the next
        // several top-level deps (forms, syntax highlighter) don't push
        // the main bundle past the 110-120 kB ceiling. `@tanstack/*`
        // (router + react-query) gets its own chunk so it caches
        // independently of our app code — these libs change on their own
        // cadence, decoupled from every app deploy.
        //
        // React + react-dom intentionally NOT broken out: Vite/rollup
        // tree-shakes them tightly with their consumers, and a separate
        // `react-vendor` slot emitted as an empty chunk. Leaving them
        // co-located with consumers gives better real-world chunking.
        //
        // The lazy route chunks the router emits (ReviewsScreen, the
        // spikes, the placeholders) are untouched — they still split.
        manualChunks: {
          "tanstack-vendor": ["@tanstack/react-router", "@tanstack/react-query"],
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Pipe all server calls through the local stratum-server during dev.
      // In prod we'll be served from the same origin or via a reverse proxy.
      "/fs":              "http://127.0.0.1:3000",
      "/tree":            "http://127.0.0.1:3000",
      "/search":          "http://127.0.0.1:3000",
      "/vcs":             "http://127.0.0.1:3000",
      "/change-requests": "http://127.0.0.1:3000",
      "/protected":       "http://127.0.0.1:3000",
      "/workspaces":      "http://127.0.0.1:3000",
      "/runs":            "http://127.0.0.1:3000",
      "/audit":           "http://127.0.0.1:3000",
      "/auth":            "http://127.0.0.1:3000",
      "/health":          "http://127.0.0.1:3000",
      // Manifest endpoint — currently mocked; will switch over when backend ships.
      "/v1/capabilities": "http://127.0.0.1:3000",
    },
  },
});
