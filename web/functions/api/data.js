import { json } from "./_middleware.js";

// GET /api/data → one row per keyword (latest, previous and earlier rank)
// Uses the kw_latest view (see supabase_setup.sql), so it is quick.
export async function onRequestGet({ data }) {
  const rows = await data.sb.select(
    "kw_latest",
    "select=exam,keyword,type,cluster,volume,target_url,cur_rank,cur_url,cur_target_rank,cur_bucket,cur_date,prev_rank,prev_date,prev2_rank"
  );
  return json({ rows, loaded_at: new Date().toISOString() });
}
