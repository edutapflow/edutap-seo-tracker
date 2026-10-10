import { json, makeSessionCookie } from "./_middleware.js";

// POST /api/login  { password }  → sets a login cookie for 30 days
export async function onRequestPost({ request, env }) {
  let body = {};
  try { body = await request.json(); } catch (_) {}
  if (!env.DASH_PASSWORD) {
    return json({ error: "The website password (DASH_PASSWORD) is not set in Cloudflare settings." }, 500);
  }
  if ((body.password || "") !== env.DASH_PASSWORD) {
    await new Promise((r) => setTimeout(r, 600)); // slow down guessing
    return json({ error: "Wrong password. Try again." }, 401);
  }
  return json({ ok: true }, 200, { "Set-Cookie": await makeSessionCookie(env) });
}
