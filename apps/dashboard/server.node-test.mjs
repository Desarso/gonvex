import test from "node:test";
import assert from "node:assert/strict";
import {
  envFlag,
  normalizeEmail,
  signSession,
  verifyCredentials,
  verifySessionCookie,
} from "./server.mjs";

test("normalizes dashboard login emails", () => {
  assert.equal(normalizeEmail(" Malek.Gabriel33@GMAIL.COM "), "malek.gabriel33@gmail.com");
});

test("parses dashboard auth flags", () => {
  assert.equal(envFlag(undefined, true), true);
  assert.equal(envFlag("false", true), false);
  assert.equal(envFlag("true", false), true);
  assert.equal(envFlag("wat", false), false);
});

test("verifies credentials without accepting casing or password drift", () => {
  assert.equal(verifyCredentials("MALEK.GABRIEL33@gmail.com", "secret", "malek.gabriel33@gmail.com", "secret"), true);
  assert.equal(verifyCredentials("other@gmail.com", "secret", "malek.gabriel33@gmail.com", "secret"), false);
  assert.equal(verifyCredentials("malek.gabriel33@gmail.com", "nope", "malek.gabriel33@gmail.com", "secret"), false);
});

test("signs and rejects dashboard sessions", () => {
  const session = {
    email: "malek.gabriel33@gmail.com",
    expiresAt: Date.now() + 60_000,
    name: "Malek Gabriel33",
    provider: "gonvex",
  };
  const cookie = signSession(session, "session-secret");

  assert.equal(verifySessionCookie(cookie, "session-secret").email, session.email);
  assert.equal(verifySessionCookie(cookie, "wrong-secret"), null);
  assert.equal(verifySessionCookie(cookie.replace(/\.[^.]+$/, ".tampered"), "session-secret"), null);
});
