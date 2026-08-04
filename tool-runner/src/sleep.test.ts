import { describe, expect, it, vi } from "vitest";
import { sleep } from "./sleep.js";

class CountingSignal extends EventTarget {
  aborted = false;
  listeners = 0;
  override addEventListener(...args: Parameters<EventTarget["addEventListener"]>): void {
    this.listeners++;
    super.addEventListener(...args);
  }
  override removeEventListener(...args: Parameters<EventTarget["removeEventListener"]>): void {
    this.listeners--;
    super.removeEventListener(...args);
  }
  abort(): void {
    this.aborted = true;
    this.dispatchEvent(new Event("abort"));
  }
}

describe("sleep", () => {
  it("removes the abort listener when the timer wins", async () => {
    vi.useFakeTimers();
    const signal = new CountingSignal();
    const waiting = sleep(1000, signal as unknown as AbortSignal);
    expect(signal.listeners).toBe(1);
    await vi.advanceTimersByTimeAsync(1000);
    await waiting;
    expect(signal.listeners).toBe(0);
    vi.useRealTimers();
  });

  it("clears the timer and listener when abort wins", async () => {
    vi.useFakeTimers();
    const signal = new CountingSignal();
    const waiting = sleep(1000, signal as unknown as AbortSignal);
    signal.abort();
    await waiting;
    expect(signal.listeners).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  });
});
