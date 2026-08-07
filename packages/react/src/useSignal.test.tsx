import { act, cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { signal } from "../../client/dist/signals.js";
import { useSignalValue } from "./useSignal";

afterEach(cleanup);

describe("useSignalValue", () => {
  it("re-renders a component when the signal changes", () => {
    const count = signal(0);
    const renders = vi.fn();

    function Counter() {
      renders();
      return <div data-testid="count">{useSignalValue(count)}</div>;
    }

    const view = render(<Counter />);
    expect(view.getByTestId("count").textContent).toBe("0");

    act(() => {
      count.value = 1;
    });

    expect(view.getByTestId("count").textContent).toBe("1");
    expect(renders).toHaveBeenCalledTimes(2);
  });

  it("does not re-render after an Object.is-equal assignment", () => {
    const value = signal("same");
    const renders = vi.fn();

    function Consumer() {
      renders();
      return <div>{useSignalValue(value)}</div>;
    }

    render(<Consumer />);
    act(() => {
      value.value = "same";
    });

    expect(renders).toHaveBeenCalledOnce();
  });

  it("unsubscribes when the component unmounts", () => {
    const source = signal(0);
    const unsubscribe = vi.fn();
    const subscribe = source.subscribe.bind(source);
    source.subscribe = (fn) => {
      const dispose = subscribe(fn);
      return () => {
        unsubscribe();
        dispose();
      };
    };

    function Consumer() {
      return <div>{useSignalValue(source)}</div>;
    }

    const view = render(<Consumer />);
    view.unmount();

    expect(unsubscribe).toHaveBeenCalledOnce();
  });
});
