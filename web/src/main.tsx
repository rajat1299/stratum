import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import "./styles.css";
import { AuthProvider } from "./lib/auth.tsx";
import { router } from "./router.tsx";

// AuthProvider has to wrap RouterProvider so route components can use
// useAuth() during their render. The router itself doesn't read auth;
// the per-route gates do (see web/src/lib/auth-gates.tsx + router.tsx).

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");

createRoot(root).render(
  <StrictMode>
    <AuthProvider>
      <RouterProvider router={router} />
    </AuthProvider>
  </StrictMode>,
);
