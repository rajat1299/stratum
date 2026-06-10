import { describe, expect, it } from "vitest";
import { formatReviewActor, formatReviewActorList } from "./review-actors.ts";

describe("formatReviewActor", () => {
  it("returns root for uid 0", () => {
    expect(formatReviewActor(0)).toBe("root");
  });

  it("returns Reviewer N for default reviewer uids", () => {
    expect(formatReviewActor(42)).toBe("Reviewer 42");
  });

  it("returns Agent N only when kind agent is supplied", () => {
    expect(formatReviewActor(100, { kind: "agent" })).toBe("Agent 100");
    expect(formatReviewActor(100)).toBe("Reviewer 100");
  });
});

describe("formatReviewActorList", () => {
  it("joins reviewer labels", () => {
    expect(formatReviewActorList([42, 7])).toBe("Reviewer 42, Reviewer 7");
  });

  it("returns - for an empty list", () => {
    expect(formatReviewActorList([])).toBe("-");
  });
});
