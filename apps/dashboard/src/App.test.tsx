import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { App } from "./App";

describe("App", () => {
  it("renders the dashboard overview", () => {
    render(<App />);

    expect(
      screen.getByRole("heading", { name: /realtime control room/i }),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/watch the local project runtime/i),
    ).toBeInTheDocument();
  });

  it("renders the Glide Data Grid test surface", () => {
    render(<App />);

    expect(screen.getByRole("heading", { name: /livegrid test surface/i })).toBeInTheDocument();
    expect(screen.getByTestId("function-grid")).toBeInTheDocument();
  });

  it("renders an accessible React Aria button", async () => {
    const user = userEvent.setup();
    render(<App />);

    const button = screen.getByRole("button", { name: /ready for tests/i });
    await user.click(button);

    expect(button).toBeInTheDocument();
  });

  it("switches pages from the sidebar", async () => {
    const user = userEvent.setup();
    render(<App />);

    await user.click(screen.getByRole("button", { name: /data/i }));
    expect(screen.getByRole("heading", { name: /postgres project schema/i })).toBeInTheDocument();
    expect(screen.getByText(/tables, columns, and indexes/i)).toBeInTheDocument();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));
    expect(screen.getByRole("heading", { name: /file storage/i })).toBeInTheDocument();
  });

  it("shows real file storage shell without fake rows", async () => {
    const user = userEvent.setup();
    render(<App />);

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));

    expect(screen.getByRole("table", { name: /stored files/i })).toBeInTheDocument();
    expect(screen.getByText(/no files yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/kg27zrhezshg/i)).not.toBeInTheDocument();
  });

  it("shows settings sections", async () => {
    const user = userEvent.setup();
    render(<App />);

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /settings/i }));
    expect(screen.getByRole("button", { name: /general/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /environment variables/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /authentication/i })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /authentication/i }));
    expect(screen.getByText(/authentication providers configured/i)).toBeInTheDocument();
  });

  it("shows selectable tables on the data page", async () => {
    const user = userEvent.setup();
    render(<App />);

    await user.click(screen.getByRole("button", { name: /data/i }));

    const tables = screen.getByLabelText("Tables");
    expect(within(tables).getByRole("button", { name: /tasks/i })).toBeInTheDocument();

    await user.click(within(tables).getByRole("button", { name: /files/i }));

    expect(screen.getByRole("heading", { name: /^files$/i })).toBeInTheDocument();
    expect(screen.getByText(/runtime is offline/i)).toBeInTheDocument();
  });
});
