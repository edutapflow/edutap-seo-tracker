import { json } from "./_middleware.js";

// GET /api/history?keyword=... → every check of one keyword, oldest first
export async function onRequestGet({ request, data }) {
  const kw = new URL(request.url).searchParams.get("keyword") || "";
  if (!kw) return json({ error: "No keyword given." }, 400);
  const rows = await data.sb.select(
    "rankings",
    `select=date,rank,url&keyword=eq.${encodeURIComponent(kw)}&order=date.asc`
  );
  return json({ keyword: kw, rows });
}
