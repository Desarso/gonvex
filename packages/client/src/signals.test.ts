import { describe, expect, it, vi } from "vitest";
import { batch, computed, effect, signal } from "./signals";

describe("signals", () => {
  it("tracks reads and keeps computed values lazy and cached", () => {
    const source = signal(2);
    const derive = vi.fn(() => source.value * 3);
    const value = computed(derive);

    expect(derive).not.toHaveBeenCalled();
    expect(value.value).toBe(6);
    expect(value.value).toBe(6);
    expect(derive).toHaveBeenCalledTimes(1);

    source.value = 4;
    expect(derive).toHaveBeenCalledTimes(1);
    expect(value.value).toBe(12);
    expect(derive).toHaveBeenCalledTimes(2);
  });

  it("re-collects dynamic dependencies on every computation", () => {
    const useLeft = signal(true);
    const left = signal("left");
    const right = signal("right");
    const selected = computed(() => (useLeft.value ? left.value : right.value));
    const seen: string[] = [];
    const dispose = effect(() => seen.push(selected.value));

    left.value = "L";
    useLeft.value = false;
    left.value = "ignored";
    right.value = "R";

    expect(seen).toEqual(["left", "L", "right", "R"]);
    dispose();
  });

  it("batches nested writes and notifies once with the final value", () => {
    const value = signal(0);
    const subscriber = vi.fn();
    const runs: number[] = [];
    value.subscribe(subscriber);
    const dispose = effect(() => runs.push(value.value));

    batch(() => {
      value.value = 1;
      batch(() => {
        value.value = 2;
        value.value = 3;
      });
      expect(subscriber).not.toHaveBeenCalled();
      expect(runs).toEqual([0]);
    });

    expect(subscriber).toHaveBeenCalledOnce();
    expect(subscriber).toHaveBeenCalledWith(3);
    expect(runs).toEqual([0, 3]);
    dispose();
  });

  it("deduplicates diamond dependencies and computed reads", () => {
    const source = signal(1);
    const leftFn = vi.fn(() => source.value + 1);
    const rightFn = vi.fn(() => source.value * 2);
    const left = computed(leftFn);
    const right = computed(rightFn);
    const diamondFn = vi.fn(() => left.value + right.value);
    const diamond = computed(diamondFn);
    const seen: number[] = [];
    const dispose = effect(() => seen.push(diamond.value + diamond.value));

    source.value = 2;

    expect(seen).toEqual([8, 14]);
    expect(leftFn).toHaveBeenCalledTimes(2);
    expect(rightFn).toHaveBeenCalledTimes(2);
    expect(diamondFn).toHaveBeenCalledTimes(2);
    dispose();
  });

  it("propagates invalidation through computed chains", () => {
    const source = signal(1);
    const plusOne = computed(() => source.value + 1);
    const asText = computed(() => `value:${plusOne.value}`);

    expect(asText.value).toBe("value:2");
    source.value = 9;
    expect(asText.value).toBe("value:10");
  });

  it("supports effect and subscription disposal during notification", () => {
    const value = signal(0);
    const effectRuns: number[] = [];
    const disposeEffect = effect(() => effectRuns.push(value.value));
    const second = vi.fn();
    let disposeSecond = () => {};
    value.subscribe(() => disposeSecond());
    disposeSecond = value.subscribe(second);

    value.value = 1;
    disposeEffect();
    value.value = 2;

    expect(second).not.toHaveBeenCalled();
    expect(effectRuns).toEqual([0, 1]);
  });

  it("uses Object.is equality for writable values", () => {
    const value = signal(Number.NaN);
    const subscriber = vi.fn();
    value.subscribe(subscriber);

    value.value = Number.NaN;
    expect(subscriber).not.toHaveBeenCalled();

    value.value = 0;
    value.value = -0;
    expect(subscriber).toHaveBeenCalledTimes(2);
  });

  it("lets an effect update itself by deferring each re-entrant run", () => {
    const value = signal(0);
    const seen: number[] = [];
    const dispose = effect(() => {
      const current = value.value;
      seen.push(current);
      if (current < 3) value.value = current + 1;
    });

    expect(seen).toEqual([0, 1, 2, 3]);
    expect(value.peek()).toBe(3);
    dispose();
  });

  it("caps an accidental infinite re-entrant effect", () => {
    const value = signal(0);
    expect(() => effect(() => {
      value.value += 1;
    })).toThrow(/100 synchronous re-runs/);
  });
});
