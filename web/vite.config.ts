import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { TanStackRouterVite } from "@tanstack/router-vite-plugin";

export default defineConfig({
  plugins: [TanStackRouterVite(), react(), tailwindcss()],
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
