import { json } from "./_middleware.js";

// GET  /api/runs → last 6 robot runs from GitHub (status, time, link)
// POST /api/runs { exams: [...], run_password } → asks GitHub to run the robot for those exams
// The robot then checks the ranks and sends the email, like the Monday run.

const WORKFLOW = "daily_update.yml";

function gh(env, path, init = {}) {
  const base = (env.GITHUB_API || "https://api.github.com").replace(/\/+$/, "");
  return fetch(`${base}/repos/${env.GITHUB_REPO}${path}`, {
    ...init,
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "edutap-seo-website",
      ...(init.headers || {}),
    },
  });
}

function checkSetup(env) {
  if (!env.GITHUB_TOKEN || !env.GITHUB_REPO) {
    return json({ error: "GITHUB_TOKEN or GITHUB_REPO is not set in Cloudflare settings." }, 500);
  }
  return null;
}

export async function onRequestGet({ env }) {
  const bad = checkSetup(env); if (bad) return bad;
  const r = await gh(env, `/actions/workflows/${WORKFLOW}/runs?per_page=6`);
  if (!r.ok) return json({ error: `GitHub answered HTTP ${r.status}` }, 502);
  const d = await r.json();
  const runs = (d.workflow_runs || []).map((w) => ({
    id: w.id,
    status: w.status,            // queued / in_progress / completed
    result: w.conclusion,        // success / failure / cancelled / null
    started: w.run_started_at || w.created_at,
    updated: w.updated_at,
    url: w.html_url,
    title: w.display_title || w.name,
  }));
  return json({ runs });
}

export async function onRequestPost({ request, env }) {
  const bad = checkSetup(env); if (bad) return bad;
  let body = {};
  try { body = await request.json(); } catch (_) {}

  if (env.RUN_PASSWORD && (body.run_password || "") !== env.RUN_PASSWORD) {
    return json({ error: "Wrong run password." }, 401);
  }
  const exams = (Array.isArray(body.exams) ? body.exams : [])
    .map((e) => String(e).replace(/,/g, " ").trim()).filter(Boolean);
  if (!exams.length) return json({ error: "Pick at least one exam." }, 400);
  if (exams.length > 8) return json({ error: "Pick 8 exams or fewer for one run." }, 400);

  const r = await gh(env, `/actions/workflows/${WORKFLOW}/dispatches`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ref: env.GITHUB_BRANCH || "main",
      inputs: { run_mode: "exams", exams: exams.join(",") },
    }),
  });
  if (r.status !== 204) {
    return json({ error: `GitHub did not start the run (HTTP ${r.status}): ${(await r.text()).slice(0, 200)}` }, 502);
  }
  return json({ ok: true, exams });
}
