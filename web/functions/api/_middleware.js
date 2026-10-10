// Runs before every /api/... request.
// 1) Checks the login cookie (except for /api/login and /api/logout).
// 2) Gives every function a small helper to read Supabase with the secret key,
//    so the key never reaches the browser.

const COOKIE = "edt_session";
const enc = new TextEncoder();

async function sign(secret, text) {
  const key = await crypto.subtle.importKey(
    "raw", enc.encode("edutap-seo:" + secret),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(text));
  return btoa(String.fromCharCode(...new Uint8Array(sig)))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export async function makeSessionCookie(env, days = 30) {
  const exp = Date.now() + days * 86400000;
  const token = `${exp}.${await sign(env.DASH_PASSWORD || "", String(exp))}`;
  return `${COOKIE}=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${days * 86400}`;
}

export function clearSessionCookie() {
  return `${COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0`;
}

async function isLoggedIn(request, env) {
  const raw = request.headers.get("Cookie") || "";
  const m = raw.match(new RegExp(`(?:^|;\\s*)${COOKIE}=([^;]+)`));
  if (!m) return false;
  const [exp, sig] = m[1].split(".");
  if (!exp || !sig || Number(exp) < Date.now()) return false;
  return sig === (await sign(env.DASH_PASSWORD || "", exp));
}

export function json(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store", ...extraHeaders },
  });
}

// Read rows from Supabase (PostgREST). Pages through results 1,000 at a time.
function makeSupabase(env) {
  const base = (env.SUPABASE_URL || "").replace(/\/+$/, "") + "/rest/v1/";
  const headers = { apikey: env.SUPABASE_KEY, Authorization: `Bearer ${env.SUPABASE_KEY}` };
  return {
    async select(table, query = "", { all = true } = {}) {
      const out = [];
      const page = 1000;
      for (let offset = 0; ; offset += page) {
        const sep = query ? "&" : "";
        const url = `${base}${table}?${query}${sep}limit=${page}&offset=${offset}`;
        const r = await fetch(url, { headers });
        if (!r.ok) throw new Error(`Supabase ${table}: HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
        const rows = await r.json();
        out.push(...rows);
        if (!all || rows.length < page) break;
      }
      return out;
    },
  };
}

export async function onRequest(context) {
  const { request, env, next } = context;
  const path = new URL(request.url).pathname;
  context.data.sb = makeSupabase(env);

  if (path === "/api/login" || path === "/api/logout") return next();

  if (!env.DASH_PASSWORD) {
    return json({ error: "The website password (DASH_PASSWORD) is not set in Cloudflare settings." }, 500);
  }
  if (!(await isLoggedIn(request, env))) {
    return json({ error: "Please log in." }, 401);
  }
  try {
    return await next();
  } catch (e) {
    return json({ error: String(e.message || e) }, 500);
  }
}
