import { json, clearSessionCookie } from "./_middleware.js";

// POST /api/logout → removes the login cookie
export async function onRequestPost() {
  return json({ ok: true }, 200, { "Set-Cookie": clearSessionCookie() });
}
