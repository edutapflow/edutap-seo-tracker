import { json } from "./_middleware.js";

// GET /api/credits → SearchApi searches left this month (this check is free)
export async function onRequestGet({ env }) {
  if (!env.SEARCHAPI_KEY) return json({ error: "SEARCHAPI_KEY is not set in Cloudflare settings." }, 500);
  const base = (env.SEARCHAPI_BASE || "https://www.searchapi.io").replace(/\/+$/, "");
  const r = await fetch(`${base}/api/v1/me?api_key=${encodeURIComponent(env.SEARCHAPI_KEY)}`);
  if (!r.ok) return json({ error: `SearchApi answered HTTP ${r.status}` }, 502);
  const d = await r.json();
  const acc = d.account || {};
  const sub = d.subscription || {};
  // SearchApi sends the plan as allowance + usage. "remaining_credits" can be only the
  // small free bonus (e.g. 2), so plan searches left = allowance - used.
  const allowance = Number(acc.monthly_allowance || 0);
  const used = Number(acc.current_month_usage || 0);
  const bonus = Number(acc.remaining_credits || 0);
  const left = Math.max(Math.max(0, allowance - used), bonus);
  return json({
    left,
    used,
    allowance,
    period_end: (sub.period_end || "").slice(0, 10),
  });
}
