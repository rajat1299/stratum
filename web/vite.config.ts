import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// TanStackRouterVite() is intentionally not registered yet. Phase A2 ships
// with code-based routing (web/src/router.tsx) so there is no src/routes/
// tree for the file-based plugin to scan. When we migrate to file-based
// routing (likely Phase D once the route count grows), re-add the plugin.

export default defineConfig({
  plugins: [react(), tailwindcss()],
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
