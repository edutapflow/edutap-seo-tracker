// EduTap Rank Board — plain JavaScript, no build step.
// Data comes from /api/... (Cloudflare functions). Rank 101 (or anything > 20) = not in top 20.

const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const OUT = 21; // used for maths when a keyword is not in the top 20

const state = {
  rows: [], schedule: null, tab: "overview", mover: "gains",
  f: { exam: "", group: "", type: "", cluster: "", q: "" },
  selKw: null,
};

const BANDS = [
  { id: 1, cls: "seg-1", label: "1–3" },
  { id: 2, cls: "seg-2", label: "4–10" },
  { id: 3, cls: "seg-3", label: "11–20" },
  { id: "nc", cls: "seg-nc", label: "Not confirmed" },
  { id: 4, cls: "seg-4", label: "Not in top 20" },
];

/* ---------------- helpers ---------------- */
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const num = (n) => Number(n).toLocaleString("en-IN");
const band = (r) => (r == null ? null : r <= 3 ? 1 : r <= 10 ? 2 : r <= 20 ? 3 : 4);
const showRank = (r) => (r == null ? "–" : r > 20 ? "20+" : r);
function fmtDate(s, withDay = false) {
  if (!s) return "–";
  const d = new Date(String(s).replace(" ", "T"));
  if (isNaN(d)) return s;
  return d.toLocaleDateString("en-IN", withDay ? { weekday: "short", day: "numeric", month: "short" } : { day: "numeric", month: "short" });
}
function changeHtml(prev, cur) {
  if (prev == null || cur == null) return '<span class="muted">New</span>';
  const p = prev > 20 ? OUT : prev, c = cur > 20 ? OUT : cur;
  const d = p - c;
  if (d > 0) return `<span class="up">▲ ${d}</span>`;
  if (d < 0) return `<span class="down">▼ ${-d}</span>`;
  return '<span class="muted">same</span>';
}

async function api(path, opts = {}) {
  const r = await fetch(path, { credentials: "same-origin", ...opts,
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) } });
  let d = {};
  try { d = await r.json(); } catch (_) {}
  if (r.status === 401 && path !== "/api/login" && path !== "/api/runs") { showLogin(); throw new Error("login"); }
  if (!r.ok) throw new Error(d.error || `Request failed (HTTP ${r.status})`);
  return d;
}

/* ---------------- tooltip ---------------- */
const tip = $("#tip");
function showTip(html, x, y) {
  tip.innerHTML = html; tip.hidden = false;
  const w = tip.offsetWidth, h = tip.offsetHeight;
  tip.style.left = Math.min(window.innerWidth - w - 8, x + 14) + "px";
  tip.style.top = Math.max(8, y - h - 12) + "px";
}
const hideTip = () => { tip.hidden = true; };

/* ---------------- login ---------------- */
function showLogin() { $("#app").hidden = true; $("#login").hidden = false; $("#pw").focus(); }
$("#loginForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  $("#loginErr").textContent = "";
  try {
    await api("/api/login", { method: "POST", body: JSON.stringify({ password: $("#pw").value }) });
    $("#pw").value = "";
    $("#login").hidden = true;
    start();
  } catch (err) { $("#loginErr").textContent = err.message; }
});
$("#logoutBtn").addEventListener("click", async () => {
  try { await api("/api/logout", { method: "POST" }); } catch (_) {}
  showLogin();
});

/* ---------------- loading ---------------- */
function groupOf(exam) {
  const s = state.schedule || {};
  const n = String(exam || "").trim().toLowerCase();
  if ((s.every_week || []).some((e) => e.toLowerCase() === n)) return "Every week";
  if ((s.once_a_month || []).some((e) => e.toLowerCase() === n)) return "Once a month";
  return "Every 15 days";
}

function prepare(raw) {
  return raw.map((r) => {
    const cur = r.cur_rank == null ? null : Number(r.cur_rank);
    const prev = r.prev_rank == null ? null : Number(r.prev_rank);
    const nc = String(r.cur_url || "").startsWith("Not confirmed");
    return {
      exam: r.exam || "Others", keyword: r.keyword, type: r.type || "", cluster: (r.cluster || "").trim(),
      volume: Number(r.volume || 0), target: r.target_url || "", url: r.cur_url || "",
      cur, prev, prev2: r.prev2_rank == null ? null : Number(r.prev2_rank),
      date: r.cur_date, prevDate: r.prev_date, nc,
      band: nc ? "nc" : band(cur), prevBand: band(prev), group: groupOf(r.exam),
    };
  });
}

async function loadData() {
  $("#lastRun").textContent = "Loading…";
  const [sched, data] = await Promise.all([
    fetch("schedule.json").then((r) => r.json()).catch(() => ({})),
    api("/api/data"),
  ]);
  state.schedule = sched;
  state.rows = prepare(data.rows || []);
  const last = state.rows.map((r) => r.date).filter(Boolean).sort().pop();
  $("#lastRun").textContent = last ? `Last checked ${fmtDate(last, true)}, ${String(last).slice(11, 16)} IST` : "No checks yet";
  fillFilters();
  render();
}

async function loadCredits() {
  try {
    const c = await api("/api/credits");
    const pct = c.allowance ? (c.left / c.allowance) * 100 : 0;
    $("#credLeft").textContent = num(c.left);
    $("#credOf").textContent = `of ${num(c.allowance)} searches left`;
    $("#credBar").style.width = pct + "%";
    $(".meter").classList.toggle("low", c.left < 1000);
    const runsLeft = Math.floor(c.left / (704 * 1.4));
    $("#credNote").textContent = (c.period_end ? `Renews ${fmtDate(c.period_end)}. ` : "") +
      `Enough for about ${runsLeft} weekly run${runsLeft === 1 ? "" : "s"}.`;
  } catch (e) {
    if (e.message === "login") return;
    $("#credLeft").textContent = "–";
    $("#credOf").textContent = "searches left unknown";
    $("#credNote").textContent = e.message;
  }
}

function start() {
  $("#app").hidden = false;
  loadData().then(loadCredits).catch((e) => {
    if (e.message !== "login") $("#lastRun").textContent = "Could not load data: " + e.message;
  });
}

/* ---------------- filters ---------------- */
function fillSelect(sel, values, firstLabel) {
  const cur = sel.value;
  sel.innerHTML = `<option value="">${firstLabel}</option>` + values.map((v) => `<option>${esc(v)}</option>`).join("");
  if (values.includes(cur)) sel.value = cur;
}
function fillFilters() {
  const uniq = (k, rows = state.rows) => [...new Set(rows.map((r) => r[k]).filter(Boolean))].sort((a, b) => a.localeCompare(b));
  fillSelect($("#fExam"), uniq("exam"), "All exams");
  fillSelect($("#fType"), uniq("type"), "All");
  const forCluster = state.f.exam ? state.rows.filter((r) => r.exam === state.f.exam) : state.rows;
  fillSelect($("#fCluster"), uniq("cluster", forCluster), "All clusters");
}
function filtered() {
  const { exam, group, type, cluster, q } = state.f;
  const qq = q.trim().toLowerCase();
  return state.rows.filter((r) =>
    (!exam || r.exam === exam) && (!group || r.group === group) && (!type || r.type === type) &&
    (!cluster || r.cluster === cluster) && (!qq || r.keyword.toLowerCase().includes(qq)));
}
const bind = (id, key, ev = "change") => $(id).addEventListener(ev, (e) => {
  state.f[key] = e.target.value;
  if (key === "exam") { state.f.cluster = ""; fillFilters(); $("#fCluster").value = ""; }
  render();
});
bind("#fExam", "exam"); bind("#fGroup", "group"); bind("#fType", "type"); bind("#fCluster", "cluster"); bind("#fSearch", "q", "input");
$("#fClear").addEventListener("click", () => {
  state.f = { exam: "", group: "", type: "", cluster: "", q: "" };
  ["#fExam", "#fGroup", "#fType", "#fCluster", "#fSearch"].forEach((s) => ($(s).value = ""));
  fillFilters(); render();
});

/* ---------------- tabs ---------------- */
$$(".tab").forEach((b) => b.addEventListener("click", () => {
  state.tab = b.dataset.tab;
  $$(".tab").forEach((t) => t.setAttribute("aria-selected", String(t === b)));
  $("#tab-overview").hidden = state.tab !== "overview";
  $("#tab-movers").hidden = state.tab !== "movers";
  render();
}));

function render() {
  const rows = filtered();
  if (state.tab === "overview") renderOverview(rows); else renderMovers(rows);
}

/* ---------------- overview ---------------- */
function renderOverview(rows) {
  const checked = rows.filter((r) => r.cur != null);
  const inTop = (list, n, useBand = "cur") => list.filter((r) => r[useBand] != null && r[useBand] <= n).length;
  const withPrev = checked.filter((r) => r.prev != null);
  const stat = (label, val, prevVal, sw, help) => {
    let d = "";
    if (prevVal != null) {
      const diff = val - prevVal;
      d = diff > 0 ? `<span class="up">▲ ${diff}</span> since last check` : diff < 0 ? `<span class="down">▼ ${-diff}</span> since last check` : "No change since last check";
    }
    return `<div class="stat"><div class="k">${sw}${label}</div><div class="v">${num(val)}</div><div class="d">${help || d}</div></div>`;
  };
  const sw = (c) => `<i class="sw ${c}"></i>`;
  const nc = checked.filter((r) => r.nc).length;
  const notIn = checked.filter((r) => r.cur > 20).length;
  $("#strip").innerHTML =
    stat("Top 3", inTop(checked, 3), inTop(withPrev, 3, "prev") + inTop(checked.filter((r) => r.prev == null), 3), sw("seg-1")) +
    stat("Top 10", inTop(checked, 10), inTop(withPrev, 10, "prev") + inTop(checked.filter((r) => r.prev == null), 10), sw("seg-2")) +
    stat("Top 20", inTop(checked, 20), inTop(withPrev, 20, "prev") + inTop(checked.filter((r) => r.prev == null), 20), sw("seg-3")) +
    stat("Not in top 20", notIn, null, sw("seg-4"), `of ${num(checked.length)} keywords checked`) +
    stat("Not confirmed", nc, null, sw("sw-nc"), nc ? "Last rank kept. Check these on a phone." : "None this time");

  $("#legend").innerHTML = BANDS.map((b) => `<span><i class="sw ${b.id === "nc" ? "sw-nc" : b.cls}"></i>${b.label}</span>`).join("");

  // one row per exam
  const byExam = new Map();
  checked.forEach((r) => { if (!byExam.has(r.exam)) byExam.set(r.exam, []); byExam.get(r.exam).push(r); });
  const exams = [...byExam.entries()].map(([exam, list]) => {
    const counts = Object.fromEntries(BANDS.map((b) => [b.id, list.filter((r) => r.band === b.id).length]));
    const top10 = list.filter((r) => r.cur <= 10).length;
    const into = list.filter((r) => r.prev != null && r.prev > 10 && r.cur <= 10).length;
    const outOf = list.filter((r) => r.prev != null && r.prev <= 10 && r.cur > 10).length;
    const last = list.map((r) => r.date).filter(Boolean).sort().pop();
    return { exam, list, counts, total: list.length, top10, into, outOf, last, group: list[0].group };
  }).sort((a, b) => b.top10 / b.total - a.top10 / a.total);

  if (!exams.length) {
    $("#ladder").innerHTML = `<p class="empty">No keywords match these filters. Clear a filter to see more.</p>`;
    return;
  }
  $("#ladder").innerHTML =
    `<div class="ladder-head" role="row"><span>Exam</span><span>Keywords by position</span><span>In top 10</span><span>Top 10 change</span></div>` +
    exams.map((e) => {
      const segs = BANDS.filter((b) => e.counts[b.id] > 0).map((b) =>
        `<i class="${b.cls}" style="flex:${e.counts[b.id]}" data-tip="${esc(`<b>${e.exam}</b><br>${e.counts[b.id]} keywords: ${b.label}`)}"></i>`).join("");
      const net = e.into - e.outOf;
      const chg = (e.into || e.outOf)
        ? `${net > 0 ? `<span class="up">▲ ${net}</span>` : net < 0 ? `<span class="down">▼ ${-net}</span>` : '<span class="muted">0</span>'} <span class="muted small">(${e.into} in, ${e.outOf} out)</span>`
        : '<span class="muted">no change</span>';
      return `<button class="lrow ${state.f.exam === e.exam ? "active" : ""}" role="row" data-exam="${esc(e.exam)}">
        <span class="ename">${esc(e.exam)}<small>${e.group}, checked ${fmtDate(e.last)}</small></span>
        <span class="bar">${segs}</span>
        <span class="pct">${Math.round((e.top10 / e.total) * 100)}%<small>${e.top10} of ${e.total}</small></span>
        <span class="chg">${chg}</span>
      </button>`;
    }).join("");

  $$("#ladder .lrow").forEach((b) => b.addEventListener("click", () => {
    const ex = b.dataset.exam;
    state.f.exam = state.f.exam === ex ? "" : ex;
    fillFilters(); $("#fExam").value = state.f.exam; render();
  }));
  $$("#ladder [data-tip]").forEach((el) => {
    el.addEventListener("mousemove", (ev) => showTip(el.dataset.tip, ev.clientX, ev.clientY));
    el.addEventListener("mouseleave", hideTip);
  });
}

/* ---------------- movers ---------------- */
const MOVERS = {
  gains: { title: "Biggest gains", hint: "Keywords that moved up since their previous check.",
    pick: (r) => r.prev != null && !r.nc && delta(r) > 0, sort: (a, b) => delta(b) - delta(a) },
  drops: { title: "Biggest drops", hint: "Keywords that moved down since their previous check.",
    pick: (r) => r.prev != null && !r.nc && delta(r) < 0, sort: (a, b) => delta(a) - delta(b) },
  in10: { title: "Entered top 10", hint: "Were outside the top 10 last time, inside now.",
    pick: (r) => r.prev != null && r.prev > 10 && r.cur <= 10 && !r.nc, sort: (a, b) => a.cur - b.cur },
  out10: { title: "Left top 10", hint: "Were in the top 10 last time, outside now.",
    pick: (r) => r.prev != null && r.prev <= 10 && r.cur > 10, sort: (a, b) => a.prev - b.prev },
  nc: { title: "Not confirmed", hint: "EduTap was not seen in 3 looks at Google, so the last rank was kept. Check these on a phone.",
    pick: (r) => r.nc, sort: (a, b) => a.cur - b.cur },
  all: { title: "All keywords", hint: "Every keyword that matches the filters, best rank first.",
    pick: (r) => r.cur != null, sort: (a, b) => a.cur - b.cur || b.volume - a.volume },
};
function delta(r) {
  const p = r.prev > 20 ? OUT : r.prev, c = r.cur > 20 ? OUT : r.cur;
  return p - c;
}
$$("#moverSeg button").forEach((b) => b.addEventListener("click", () => {
  state.mover = b.dataset.m;
  render();
}));

let currentList = [];
function renderMovers(rows) {
  const checked = rows.filter((r) => r.cur != null);
  $$("#moverSeg button").forEach((b) => {
    const m = MOVERS[b.dataset.m];
    const n = checked.filter(m.pick).length;
    b.setAttribute("aria-pressed", String(b.dataset.m === state.mover));
    b.innerHTML = `${m.title}<span class="n">${num(n)}</span>`;
  });
  const m = MOVERS[state.mover];
  currentList = checked.filter(m.pick).sort(m.sort);
  $("#moverTitle").textContent = m.title;
  $("#moverHint").textContent = m.hint;
  const show = currentList.slice(0, 300);
  $("#moverTable tbody").innerHTML = show.map((r, i) => `
    <tr data-i="${i}" class="${state.selKw === r.keyword ? "sel" : ""}" tabindex="0">
      <td class="kw">${esc(r.keyword)}${r.nc ? '<span class="pill">not confirmed</span>' : ""}</td>
      <td>${esc(r.exam)}</td>
      <td class="num">${showRank(r.prev)}</td>
      <td class="num"><b>${showRank(r.cur)}</b></td>
      <td class="num">${changeHtml(r.prev, r.cur)}</td>
      <td>${fmtDate(r.date)}</td>
    </tr>`).join("");
  $("#moverEmpty").hidden = currentList.length > 0;
  $("#moverEmpty").textContent = "No keywords here for these filters.";
  if (currentList.length > 300) {
    $("#moverEmpty").hidden = false;
    $("#moverEmpty").textContent = `Showing the first 300 of ${num(currentList.length)}. Use the filters or download the full list.`;
  }
  $$("#moverTable tbody tr").forEach((tr) => {
    const open = () => openTrend(show[Number(tr.dataset.i)]);
    tr.addEventListener("click", open);
    tr.addEventListener("keydown", (e) => { if (e.key === "Enter") open(); });
  });
  if (!state.selKw && show[0]) openTrend(show[0]);
}

$("#csvBtn").addEventListener("click", () => {
  const head = ["Exam", "Keyword", "Type", "Cluster", "Previous rank", "Current rank", "Change", "Not confirmed", "Last checked", "Ranked URL"];
  const lines = [head].concat(currentList.map((r) => [r.exam, r.keyword, r.type, r.cluster, showRank(r.prev), showRank(r.cur),
    r.prev == null ? "" : delta(r), r.nc ? "yes" : "", r.date || "", r.nc ? "" : r.url]));
  const csv = lines.map((l) => l.map((v) => `"${String(v ?? "").replace(/"/g, '""')}"`).join(",")).join("\n");
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob(["﻿" + csv], { type: "text/csv" }));
  a.download = `edutap-${state.mover}-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
});

/* ---------------- trend chart ---------------- */
async function openTrend(r) {
  if (!r) return;
  state.selKw = r.keyword;
  $$("#moverTable tbody tr").forEach((tr) => tr.classList.toggle("sel", tr.querySelector(".kw").textContent.startsWith(r.keyword)));
  $("#trendTitle").textContent = r.keyword;
  $("#trendSub").textContent = `${r.exam}${r.type ? ", " + r.type : ""}${r.cluster ? ", " + r.cluster : ""}`;
  $("#trendChart").innerHTML = '<p class="muted small">Loading history…</p>';
  $("#trendFacts").innerHTML = `
    <dt>Now</dt><dd><b>${showRank(r.cur)}</b> ${r.nc ? "(not confirmed, last rank kept)" : ""}</dd>
    <dt>Before</dt><dd>${showRank(r.prev)} ${changeHtml(r.prev, r.cur)}</dd>
    <dt>Ranked page</dt><dd>${!r.nc && /^https?:/.test(r.url) ? `<a href="${esc(r.url)}" target="_blank" rel="noopener">${esc(r.url.replace(/^https?:\/\/(www\.)?/, ""))}</a>` : '<span class="muted">–</span>'}</dd>
    <dt>Target page</dt><dd>${r.target ? esc(r.target) : '<span class="muted">not set</span>'}</dd>
    <dt>Searches/month</dt><dd>${r.volume ? num(r.volume) : "–"}</dd>`;
  try {
    const h = await api("/api/history?keyword=" + encodeURIComponent(r.keyword));
    if (state.selKw !== r.keyword) return;
    drawTrend(h.rows || []);
  } catch (e) {
    if (e.message !== "login") $("#trendChart").innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}

function drawTrend(raw) {
  // keep the last check of each day
  const byDay = new Map();
  raw.forEach((p) => byDay.set(String(p.date).slice(0, 10), p));
  const pts = [...byDay.values()].map((p) => ({
    d: new Date(String(p.date).replace(" ", "T")), date: p.date, r: Number(p.rank),
    nc: String(p.url || "").startsWith("Not confirmed"),
  })).filter((p) => !isNaN(p.d));
  const el = $("#trendChart");
  if (!pts.length) { el.innerHTML = '<p class="muted small">No checks yet for this keyword.</p>'; return; }

  const W = Math.max(300, Math.round(el.clientWidth || 520)), H = 260, L = 34, R = 14, T = 10, B = 28;
  const t0 = pts[0].d.getTime(), t1 = pts[pts.length - 1].d.getTime();
  const x = (d) => pts.length === 1 ? (L + W - R) / 2 : L + ((d.getTime() - t0) / (t1 - t0)) * (W - L - R);
  // positions 1..20 on the scale; 'not in top 20' sits in its own band below, with a gap
  const LOW = 23;
  const y = (r) => T + (((r > 20 ? LOW : r) - 1) / (LOW - 1)) * (H - T - B);
  const yTicks = [1, 5, 10, 15, 20];
  const nX = Math.min(pts.length, 5);
  const xTicks = pts.length === 1 ? [pts[0]] : Array.from({ length: nX }, (_, i) => pts[Math.round((i * (pts.length - 1)) / (nX - 1))]);
  const path = pts.map((p, i) => `${i ? "L" : "M"}${x(p.d).toFixed(1)},${y(p.r).toFixed(1)}`).join("");

  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Rank over time, 1 is the top of Google">
    <rect class="band" x="${L}" y="${y(21.6)}" width="${W - L - R}" height="${y(LOW) - y(21.6) + 8}"></rect>
    <g class="grid">${yTicks.map((t) => `<line x1="${L}" x2="${W - R}" y1="${y(t)}" y2="${y(t)}"></line>`).join("")}</g>
    <g class="axis">
      ${yTicks.map((t) => `<text x="${L - 8}" y="${y(t) + 4}" text-anchor="end">#${t}</text>`).join("")}
      <text x="${L - 8}" y="${y(LOW) + 4}" text-anchor="end">20+</text>
      ${xTicks.map((p) => `<text x="${x(p.d)}" y="${H - 6}" text-anchor="middle">${fmtDate(p.date)}</text>`).join("")}
    </g>
    <path class="series" d="${path}"></path>
    ${pts.map((p) => `<circle class="dot ${p.nc ? "nc" : ""}" cx="${x(p.d)}" cy="${y(p.r)}" r="4.5"></circle>`).join("")}
    <line class="cross" x1="0" x2="0" y1="${T}" y2="${H - B}" visibility="hidden"></line>
    <rect x="${L}" y="0" width="${W - L - R}" height="${H}" fill="transparent" class="hit"></rect>
  </svg>`;

  const svg = $("svg", el), cross = $(".cross", el);
  $(".hit", el).addEventListener("mousemove", (ev) => {
    const box = svg.getBoundingClientRect();
    const mx = ((ev.clientX - box.left) / box.width) * W;
    let best = pts[0];
    pts.forEach((p) => { if (Math.abs(x(p.d) - mx) < Math.abs(x(best.d) - mx)) best = p; });
    cross.setAttribute("x1", x(best.d)); cross.setAttribute("x2", x(best.d)); cross.setAttribute("visibility", "visible");
    showTip(`<b>${fmtDate(best.date, true)}</b><br>Rank ${showRank(best.r)}${best.nc ? " (not confirmed)" : ""}`, ev.clientX, ev.clientY);
  });
  $(".hit", el).addEventListener("mouseleave", () => { cross.setAttribute("visibility", "hidden"); hideTip(); });
}

/* ---------------- run dialog ---------------- */
const dlg = $("#runDlg");
$("#runBtn").addEventListener("click", () => {
  const exams = [...new Set(state.rows.map((r) => r.exam))].sort();
  const groups = ["Every week", "Every 15 days", "Once a month"];
  $("#runExams").innerHTML = groups.map((g) => {
    const list = exams.filter((e) => groupOf(e) === g);
    if (!list.length) return "";
    return `<div class="run-group"><b>${g}</b><div class="opts">${list.map((e) => {
      const n = state.rows.filter((r) => r.exam === e).length;
      return `<label class="chk"><input type="checkbox" value="${esc(e)}" data-n="${n}"> ${esc(e)} <span class="muted">${n}</span></label>`;
    }).join("")}</div></div>`;
  }).join("");
  $$("#runExams input").forEach((i) => i.addEventListener("change", updateEstimate));
  $("#runMsg").textContent = "";
  updateEstimate();
  loadRuns();
  dlg.showModal();
});
$("#runCancel").addEventListener("click", () => dlg.close());
function updateEstimate() {
  const picked = $$("#runExams input:checked");
  const kws = picked.reduce((s, i) => s + Number(i.dataset.n), 0);
  const mins = Math.max(1, Math.round((kws * 1.4 * 3.5) / 5 / 60));
  $("#runEstimate").innerHTML = picked.length
    ? `${picked.length} exam${picked.length > 1 ? "s" : ""}, ${num(kws)} keywords. Uses about <b>${num(Math.round(kws * 1.4))}</b> searches and takes about ${mins} minute${mins > 1 ? "s" : ""}.`
    : '<span class="muted">Pick the exams to check.</span>';
  $("#runGo").disabled = !picked.length;
}
$("#runForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const exams = $$("#runExams input:checked").map((i) => i.value);
  $("#runGo").disabled = true;
  $("#runMsg").textContent = "Starting…";
  try {
    await api("/api/runs", { method: "POST", body: JSON.stringify({ exams, run_password: $("#runPw").value }) });
    $("#runMsg").innerHTML = '<span class="ok-ink">Run started.</span> The email comes when it finishes. Press Reload after that to see the new ranks.';
    $("#runPw").value = "";
    setTimeout(loadRuns, 4000);
  } catch (err) {
    $("#runMsg").innerHTML = `<span class="bad-ink">${esc(err.message)}</span>`;
  } finally { updateEstimate(); }
});
async function loadRuns() {
  try {
    const d = await api("/api/runs");
    $("#runList").innerHTML = (d.runs || []).map((r) => {
      const st = r.status !== "completed" ? (r.status === "queued" ? "Waiting to start" : "Running now")
        : r.result === "success" ? '<span class="ok-ink">Finished</span>'
        : `<span class="bad-ink">${{ failure: "Failed", cancelled: "Cancelled", timed_out: "Took too long" }[r.result] || "Stopped"}</span>`;
      return `<li><span>${fmtDate(r.started, true)}, ${new Date(r.started).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit" })}</span><span>${st} <a href="${esc(r.url)}" target="_blank" rel="noopener">details</a></span></li>`;
    }).join("") || '<li class="muted small">No runs yet.</li>';
  } catch (e) {
    $("#runList").innerHTML = `<li class="bad-ink small">${esc(e.message)}</li>`;
  }
}

$("#reloadBtn").addEventListener("click", () => { state.selKw = null; loadData().then(loadCredits).catch(() => {}); });

/* ---------------- go ---------------- */
start();
