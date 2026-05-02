import { describe, expect, it } from "vitest";
import { TOOL_DESCRIPTION, createBash } from "../src/index.js";

describe("createBash", () => {
  it("exports createBash as a function", () => {
    expect(createBash).toEqual(expect.any(Function));
  });

  it("exports a tool description that mentions Stratum", () => {
    expect(TOOL_DESCRIPTION).toEqual(expect.stringContaining("Stratum"));
  });

  it.each([
    ["baseUrl", { workspaceId: "workspace", workspaceToken: "token" }],
    ["baseUrl", { baseUrl: "", workspaceId: "workspace", workspaceToken: "token" }],
    ["workspaceId", { baseUrl: "https://stratum.example", workspaceToken: "token" }],
    ["workspaceId", { baseUrl: "https://stratum.example", workspaceId: "", workspaceToken: "token" }],
    ["workspaceToken", { baseUrl: "https://stratum.example", workspaceId: "workspace" }],
    ["workspaceToken", { baseUrl: "https://stratum.example", workspaceId: "workspace", workspaceToken: "" }],
  ])("rejects missing or empty %s", async (field, options) => {
    await expect(createBash(options as Parameters<typeof createBash>[0])).rejects.toThrow(field);
  });
});
