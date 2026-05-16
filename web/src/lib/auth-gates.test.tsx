import { act, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage, useAuth } from "./auth.tsx";
import { RequireAnon, RequireAuth } from "./auth-gates.tsx";

function wrap(storage = memoryAuthStorage()) {
  return function W({ children }: { children: ReactNode }) {
    return <AuthProvider storage={storage}>{children}</AuthProvider>;
  };
}

describe("RequireAuth", () => {
  it("renders the placeholder on the initial render (before hydration resolves)", () => {
    // We can't observe the loading state by the time the assertions run under
    // React 19 — hydration flushes too fast. The assertion that proves the
    // placeholder slot exists is the custom-fallback test below; this test
    // just confirms that on anon, the children stay hidden.
    const navigate = vi.fn();
    render(
      <RequireAuth redirectTo="/login" navigate={navigate}>
        <div>protected content</div>
      </RequireAuth>,
      { wrapper: wrap() },
    );
    expect(screen.queryByText("protected content")).toBeNull();
  });

  it("renders children when authed", async () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    const navigate = vi.fn();
    render(
      <RequireAuth redirectTo="/login" navigate={navigate}>
        <div>protected content</div>
      </RequireAuth>,
      { wrapper: wrap(storage) },
    );
    await waitFor(() => expect(screen.getByText("protected content")).toBeTruthy());
    expect(navigate).not.toHaveBeenCalled();
  });

  it("calls navigate(redirectTo) when anon", async () => {
    const navigate = vi.fn();
    render(
      <RequireAuth redirectTo="/login" navigate={navigate}>
        <div>protected content</div>
      </RequireAuth>,
      { wrapper: wrap() },
    );
    await waitFor(() => expect(navigate).toHaveBeenCalledWith("/login"));
  });

  it("uses a custom fallback when supplied", () => {
    render(
      <RequireAuth redirectTo="/login" navigate={vi.fn()} fallback={<span>checking…</span>}>
        <div>protected</div>
      </RequireAuth>,
      { wrapper: wrap() },
    );
    expect(screen.getByText("checking…")).toBeTruthy();
  });
});

describe("RequireAnon", () => {
  it("renders children when anon", async () => {
    const navigate = vi.fn();
    render(
      <RequireAnon redirectTo="/reviews" navigate={navigate}>
        <div>login form</div>
      </RequireAnon>,
      { wrapper: wrap() },
    );
    await waitFor(() => expect(screen.getByText("login form")).toBeTruthy());
    expect(navigate).not.toHaveBeenCalled();
  });

  it("redirects when already authed", async () => {
    const storage = memoryAuthStorage({ type: "bearer", token: "sk_strat_x" });
    const navigate = vi.fn();
    render(
      <RequireAnon redirectTo="/reviews" navigate={navigate}>
        <div>login form</div>
      </RequireAnon>,
      { wrapper: wrap(storage) },
    );
    await waitFor(() => expect(navigate).toHaveBeenCalledWith("/reviews"));
  });
});

describe("Auth gates — react to live state transitions", () => {
  /** Combined consumer that lets us flip auth from inside the tree. */
  function Tester() {
    const auth = useAuth();
    return (
      <>
        <button type="button" onClick={() => auth.signInAsUser("alice")}>
          signin
        </button>
        <button type="button" onClick={() => auth.signOut()}>
          signout
        </button>
        <RequireAuth redirectTo="/login" navigate={() => undefined}>
          <div>secure</div>
        </RequireAuth>
      </>
    );
  }

  it("flips from placeholder to secure when sign-in fires", async () => {
    render(<Tester />, { wrapper: wrap() });
    await waitFor(() => expect(screen.getByRole("status")).toBeTruthy());
    act(() => screen.getByText("signin").click());
    await waitFor(() => expect(screen.getByText("secure")).toBeTruthy());
  });
});
