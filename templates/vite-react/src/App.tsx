import { useState } from "react";
import { api } from "../gonvex/_generated/api";
import { useLiveQuery, useReducer } from "../gonvex/_generated/react";

type Message = {
  id: string;
  body: string;
  author: string;
  created_at: string;
};

export default function App(props: { runtimeURL: string }) {
  const messages = useLiveQuery<Message[]>(api.messages.list, {}) ?? [];
  const sendMessage = useReducer(api.messages.send);
  const [body, setBody] = useState("");

  async function submit() {
    const nextBody = body.trim();
    if (!nextBody) return;
    setBody("");
    await sendMessage({ body: nextBody });
  }

  return (
    <main className="shell">
      <section className="hero">
        <div className="status">Connected to {props.runtimeURL}</div>
        <h1>Gonvex app code. TypeScript backend. Realtime React.</h1>
        <p>
          Edit <code>gonvex/messages.ts</code> or the versioned SQL migration in <code>migrations/</code>. The CLI regenerates bindings and applies migrations to the runtime.
        </p>
      </section>

      <section className="panel" aria-label="Messages">
        <div className="panelHeader">
          <div>
            <span className="kicker">Live Query</span>
            <h2>Messages</h2>
          </div>
          <span className="count">{messages.length}</span>
        </div>

        <div className="messages">
          {messages.length === 0 ? (
            <div className="empty">No messages yet. Send the first one.</div>
          ) : (
            messages.map((message) => (
              <article className="message" key={message.id}>
                <div className="avatar">{message.author.slice(0, 1).toUpperCase()}</div>
                <div>
                  <strong>{message.author}</strong>
                  <p>{message.body}</p>
                </div>
              </article>
            ))
          )}
        </div>

        <form className="composer" onSubmit={(event) => { event.preventDefault(); void submit(); }}>
          <input value={body} onChange={(event) => setBody(event.target.value)} placeholder="Write a message" />
          <button type="submit">Send</button>
        </form>
      </section>
    </main>
  );
}
