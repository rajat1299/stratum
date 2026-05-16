import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";
import { SpikeApp } from "./spike/diff-spike.tsx";

// Week-2 entry point — boots the diff-view spike directly. Phase A1 will
// replace this with the TanStack Router tree once the router-vite plugin is
// installed and we have more than one screen to navigate between.
const root = document.getElementById("root");
if (!root) throw new Error("missing #root");

createRoot(root).render(
  <StrictMode>
    <SpikeApp />
  </StrictMode>,
);
