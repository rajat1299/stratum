import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import "./styles.css";
import { AuthProvider } from "./lib/auth.tsx";
import { QueryProvider } from "./lib/query.tsx";
import { router } from "./router.tsx";

// Provider order:
//   AuthProvider   — owns session state; outermost so anyone can read auth.
//   QueryProvider  — sits inside AuthProvider so it can clear cache on
//                    sign-out (prevents stale authed data leaking across
//                    sessions). Wraps the router so route components can
//                    use useQuery / useMutation.
//   RouterProvider — innermost; renders the matched route.

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");

createRoot(root).render(
  <StrictMode>
    <AuthProvider>
      <QueryProvider>
        <RouterProvider router={router} />
      </QueryProvider>
    </AuthProvider>
  </StrictMode>,
);
