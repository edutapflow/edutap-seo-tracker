# FORCE UPDATE V39 - SearchApi (up to 3 looks, Not confirmed), exam schedule support, searches-left in email
import requests
import json
import time
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from io import BytesIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client, Client
from itertools import groupby
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from config import SEARCHAPI_KEY, SUPABASE_URL, SUPABASE_KEY, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER

# --- CONNECT TO CLOUD ---
supabase = None
try:
    if SUPABASE_URL and SUPABASE_KEY:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    else:
        print("⚠️ Supabase Credentials Missing in Secrets")
except Exception as e:
    print(f"Supabase Connection Error: {e}")

TARGET_DOMAIN = "edutap.in"
COMPETITORS = {
    "anujjindal": "anujjindal.in",
    "careerpower": "careerpower.in",
    "testbook": "testbook.com",
    "oliveboard": "oliveboard.in",
    "adda247": "adda247.com",
    "ixambee": "ixambee.com"
}

MAX_WORKERS = 5   # Lower to 3 if bulk inaccuracy still appears
SAVE_EVERY  = 50  # Flush results to Supabase after every N keywords

# ─────────────────────────────────────────────
# SUPABASE TABLE — run this SQL ONCE in Supabase SQL editor:
#
# CREATE TABLE IF NOT EXISTS run_logs (
#   id          BIGSERIAL PRIMARY KEY,
#   run_id      TEXT NOT NULL,
#   run_type    TEXT DEFAULT 'manual',
#   logged_at   TEXT NOT NULL,
#   level       TEXT DEFAULT 'info',
#   keyword     TEXT,
#   exam        TEXT,
#   kw_type     TEXT,
#   rank        INT,
#   ranked_url  TEXT,
#   message     TEXT
# );
# ─────────────────────────────────────────────

# --- FETCHERS ---
def fetch_all_rows(table_name):
    if not supabase: return pd.DataFrame()
    all_rows = []
    start = 0; batch_size = 1000
    while True:
        try:
            response = supabase.table(table_name).select("*").range(start, start + batch_size - 1).execute()
            rows = response.data
            if not rows: break
            all_rows.extend(rows)
            if len(rows) < batch_size: break
            start += batch_size
        except: break
    return pd.DataFrame(all_rows)

def get_current_month_cost():
    if not supabase: return 0.0
    try:
        current_month = datetime.now().strftime("%Y-%m")
        res = supabase.table("update_logs").select("total_cost").ilike("run_date", f"{current_month}%").execute()
        return sum(item['total_cost'] for item in res.data)
    except: return 0.0

def get_live_usd_inr_rate():
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=3)
        if r.status_code == 200: return float(r.json().get('rates', {}).get('INR', 90.0))
    except: pass
    return 90.0


def get_all_keywords(): return fetch_all_rows("keywords_master")

def fetch_run_ids(last_n=10):
    """Return list of the last N distinct run_ids, newest first."""
    if not supabase: return []
    try:
        res = (supabase.table("run_logs")
               .select("run_id, run_type, logged_at")
               .order("id", desc=True)
               .limit(10000)
               .execute())
        if not res.data: return []
        seen = {}
        for r in res.data:
            rid = r['run_id']
            if rid not in seen:
                seen[rid] = {"run_id": rid, "run_type": r.get("run_type", "manual"), "logged_at": r.get("logged_at", "")}
            if len(seen) >= last_n:
                break
        return list(seen.values())
    except Exception as e:
        print(f"⚠️ fetch_run_ids error: {e}")
        return []

def fetch_logs_for_run(run_id):
    """Fetch all log entries for a given run_id."""
    if not supabase: return pd.DataFrame()
    all_rows = []; start = 0; batch = 1000
    while True:
        try:
            res = (supabase.table("run_logs")
                   .select("*")
                   .eq("run_id", run_id)
                   .order("id")
                   .range(start, start + batch - 1)
                   .execute())
            rows = res.data
            if not rows: break
            all_rows.extend(rows)
            if len(rows) < batch: break
            start += batch
        except Exception as e:
            print(f"⚠️ fetch_logs_for_run error: {e}")
            break
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ─────────────────────────────────────────────
# INTERNAL LOG WRITER
# ─────────────────────────────────────────────
_log_buffer = []

def _log(run_id, run_type, level, message, keyword=None, exam=None, kw_type=None, rank=None, ranked_url=None):
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    _log_buffer.append({
        "run_id":     run_id,
        "run_type":   run_type,
        "logged_at":  ist_now.strftime("%Y-%m-%d %H:%M:%S"),
        "level":      level,
        "keyword":    keyword or "",
        "exam":       exam or "",
        "kw_type":    kw_type or "",
        "rank":       rank,
        "ranked_url": ranked_url or "",
        "message":    message
    })

def flush_logs():
    global _log_buffer
    if not _log_buffer or not supabase: return
    try:
        for i in range(0, len(_log_buffer), 500):
            supabase.table("run_logs").insert(_log_buffer[i:i+500]).execute()
    except Exception as e:
        print(f"⚠️ flush_logs error: {e}")
    _log_buffer = []


# --- DB MANAGEMENT ---
def add_keyword(exam, keyword, kw_type, cluster="", volume=0, target_url=""):
    if not supabase: return False, "Database not connected"
    try:
        if supabase.table("keywords_master").select("*").eq("keyword", keyword.strip()).execute().data:
            return False, f"Keyword '{keyword}' already exists in database."
        supabase.table("keywords_master").insert({
            "exam": exam, "keyword": keyword.strip(), "type": kw_type,
            "cluster": cluster, "volume": volume, "target_url": target_url
        }).execute()
        return True, "Success"
    except Exception as e: return False, str(e)

def delete_bulk_keywords(keyword_list):
    if not supabase: return
    try: supabase.table("keywords_master").delete().in_("keyword", keyword_list).execute()
    except: pass

def clear_master_database():
    if not supabase: return
    try: supabase.table("keywords_master").delete().gt("id", 0).execute()
    except: pass

def normalize_url(url):
    return str(url).lower().replace("https://", "").replace("http://", "").replace("www.", "").strip("/") if url else ""

def process_bulk_upload(uploaded_file, mode="append"):
    if not supabase: return False, "Database not connected"
    if mode == "replace_all": clear_master_database()
    try:
        xls = pd.ExcelFile(uploaded_file)
        if mode == "replace_exam":
            for sheet in xls.sheet_names:
                try: supabase.table("keywords_master").delete().eq("exam", sheet.strip()).execute()
                except: pass
        existing_kws = set()
        if mode != "replace_all":
            try:
                res = supabase.table("keywords_master").select("keyword").execute()
                existing_kws = {str(r['keyword']).lower().strip() for r in res.data}
            except: pass
        rows_to_insert = []
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)
            df.columns = [str(col).strip() for col in df.columns]
            current_exam = sheet.strip()
            for _, row in df.iterrows():
                cluster = str(row.get('Cluster', '')).strip()
                t_url = str(row.get('Page URLs', row.get('Page URL', row.get('Target URL', '')))).strip()
                if t_url.lower() == 'nan': t_url = ""
                if cluster.lower() == 'nan': cluster = ""
                p_kw = str(row.get('Primary Keyword', '')).strip()
                p_vol = row.get('Volume', 0)
                try: p_vol = int(float(str(p_vol).replace(',', '')))
                except: p_vol = 0
                if p_kw and p_kw.lower() != 'nan':
                    kw_clean = p_kw.lower()
                    if kw_clean not in existing_kws:
                        rows_to_insert.append({"exam": current_exam, "keyword": p_kw, "type": "Primary",
                                                "cluster": cluster, "volume": p_vol, "target_url": t_url})
                        existing_kws.add(kw_clean)
                sec_block = str(row.get('Secondary Keywords', ''))
                if sec_block and sec_block.lower() != 'nan':
                    sec_kws = [k.strip() for k in sec_block.split('\n') if k.strip()]
                    vol_block = str(row.get('Volume.1', row.get('Secondary Volume', '')))
                    sec_vols = []
                    if vol_block and vol_block.lower() != 'nan':
                        for v in vol_block.split('\n'):
                            try: sec_vols.append(int(float(str(v).replace(',', '').strip())))
                            except: sec_vols.append(0)
                    for i, s_kw in enumerate(sec_kws):
                        if not s_kw: continue
                        kw_clean = s_kw.lower()
                        s_vol = sec_vols[i] if i < len(sec_vols) else 0
                        if kw_clean not in existing_kws:
                            rows_to_insert.append({"exam": current_exam, "keyword": s_kw, "type": "Secondary",
                                                    "cluster": cluster, "volume": s_vol, "target_url": t_url})
                            existing_kws.add(kw_clean)
        if rows_to_insert:
            for i in range(0, len(rows_to_insert), 1000):
                supabase.table("keywords_master").insert(rows_to_insert[i:i+1000]).execute()
        return True, f"Success! Added {len(rows_to_insert)} new keywords. (Duplicates were ignored safely)."
    except Exception as e: return False, f"Error: {str(e)}"


# ─────────────────────────────────────────────
# GOOGLE SEARCH SETTINGS USED FOR EVERY KEYWORD  (SearchApi)
# These decide WHICH Google page SearchApi looks at.
# If you check a rank by hand on a phone, these are the settings the tool copies.
#   SEARCH_LOCATION = place on Google ("India" = all of India)
#   SEARCH_DEVICE   = mobile results (different from laptop results)
#   SEARCH_DEPTH    = look at the first 20 normal results only
# ─────────────────────────────────────────────
SEARCHAPI_URL      = "https://www.searchapi.io/api/v1/search"
SEARCH_LOCATION    = "India"
SEARCH_COUNTRY     = "in"
SEARCH_LANGUAGE    = "en"
SEARCH_DEVICE      = "mobile"
SEARCH_DEPTH       = 20

# How many times we look at Google when EduTap is NOT found.
# Google does not always show the same page, so a "not found" is often random.
MAX_LOOKS          = 3
WAIT_BETWEEN_LOOKS = 2      # seconds
# If EduTap is found only on look 2 or 3, and that number is much worse than
# the last rank, we look one more time and keep the better number.
JUMP_LIMIT         = 5      # places
COST_PER_SEARCH    = 0.004  # $40 plan / 10,000 searches (used only for the cost estimate)

# Label saved in the "url" column when EduTap was not seen in any look
# but had a Top-20 rank last time. The last rank is kept, so no false drop alert.
NOT_CONFIRMED_LABEL = "Not confirmed (EduTap not seen in 3 looks, last rank kept)"

# If the API key is wrong or the monthly searches are finished, we stop the run
# instead of wasting calls. Filled automatically during a run.
_stop_reason = None
LAST_RUN_SEARCHES = 0   # searches used by the last perform_update() (read by cron_job.py)


def _domain_matches(item_domain, item_url, wanted_domain):
    """True if the result belongs to wanted_domain (also matches sub-domains like blog.edutap.in)."""
    d = (item_domain or "").lower().replace("www.", "").split(":")[0]
    if not d:
        # fallback: take the domain out of the link
        d = normalize_url(item_url).split("/")[0]
    return d == wanted_domain or d.endswith("." + wanted_domain)


def _is_not_confirmed(url):
    return str(url or "").startswith("Not confirmed")


def _searchapi_call(keyword):
    """ONE look at Google through SearchApi.
    Returns (data_dict_or_None, error_text_or_None, searches_used).
    Network errors and 'too many requests' are tried again here (up to 3 times)
    and are NOT counted as a look."""
    global _stop_reason
    params = {
        "engine": "google_rank_tracking",
        "q": keyword,
        "device": SEARCH_DEVICE,
        "gl": SEARCH_COUNTRY,
        "hl": SEARCH_LANGUAGE,
        "num": SEARCH_DEPTH,
        "api_key": SEARCHAPI_KEY,
    }
    if SEARCH_LOCATION:
        params["location"] = SEARCH_LOCATION

    last_err = "Unknown error"
    for attempt in range(1, 4):
        if _stop_reason:
            return None, _stop_reason, 0
        try:
            r = requests.get(SEARCHAPI_URL, params=params, timeout=90)
        except Exception as e:
            last_err = f"Network error — could not reach SearchApi: {e}"
            time.sleep(3 * attempt)
            continue

        try:
            data = r.json()
        except ValueError:
            data = {"error": f"Bad reply from SearchApi (HTTP {r.status_code})"}

        err = data.get("error") if isinstance(data, dict) else "Unexpected reply"
        if r.status_code == 200 and not err:
            return data, None, 1

        err_text = str(err or f"HTTP {r.status_code}")
        low = err_text.lower()

        # Wrong key, or monthly searches finished -> stop the whole run
        if r.status_code in (401, 402, 403) or "api key" in low or "api_key" in low \
                or "credit" in low or "out of searches" in low or "upgrade" in low:
            _stop_reason = f"SearchApi stopped the run: {err_text}"
            return None, _stop_reason, 0

        # The place name was not accepted -> try once with country only
        if "location" in low and "location" in params:
            params.pop("location")
            continue

        # Too many requests / server busy -> wait and try again
        if r.status_code == 429 or r.status_code >= 500:
            last_err = f"SearchApi busy (HTTP {r.status_code}): {err_text}"
            time.sleep(5 * attempt)
            continue

        return None, f"SearchApi error (HTTP {r.status_code}): {err_text}", 0

    return None, last_err, 0


def _read_page(data, target_url):
    """Read one SearchApi Google page. Returns rank info for EduTap and competitors."""
    organic = (data or {}).get("organic_results") or []
    best = 101; best_url = "Not Ranked"; target_f = 101
    clean_t = normalize_url(target_url)
    comp_found = {k: 101 for k in COMPETITORS.keys()}
    comp_urls  = {k: ""  for k in COMPETITORS.keys()}

    for it in organic:
        r_url    = it.get("link", "") or ""
        r_domain = it.get("domain", "") or ""
        try:
            pos = int(it.get("position") or 101)
        except (TypeError, ValueError):
            pos = 101
        if pos > SEARCH_DEPTH:
            continue
        clean_r = normalize_url(r_url)

        if _domain_matches(r_domain, r_url, TARGET_DOMAIN):
            if pos < best:
                best, best_url = pos, r_url
            if clean_t and clean_t in clean_r and pos < target_f:
                target_f = pos

        for c_key, c_domain in COMPETITORS.items():
            if _domain_matches(r_domain, r_url, c_domain) and pos < comp_found[c_key]:
                comp_found[c_key] = pos
                comp_urls[c_key]  = r_url

    return {"rank": best, "url": best_url, "target_rank": target_f,
            "comp_ranks": comp_found, "comp_urls": comp_urls,
            "organic_count": len(organic),
            "search_id": ((data or {}).get("search_metadata") or {}).get("id", "")}


def _bucket(rank):
    if rank <= 3:  return "B1 (1-3)"
    if rank <= 10: return "B2 (4-10)"
    if rank <= 20: return "B3 (11-20)"
    return "B4 (>20)"


def fetch_rank_single(item, run_id, run_type, prev_info=None):
    """Check ONE keyword.
    Rule:
      - Look up to 3 times. Stop at the first look where EduTap is found.
      - If found only on look 2 or 3 and the number is more than 5 places worse
        than last time, look 1 more time and keep the better number.
      - If not found in any look:
          * last time it had a Top-20 rank (and last time was not already
            'Not confirmed')  -> save 'Not confirmed' and keep the last rank
                                 (so no false 'dropped' alert)
          * otherwise           -> save 'Not in Top 20'."""
    keyword    = item['keyword']
    target_url = item.get('target_url', '') or ''
    exam       = item.get('exam', ''); kw_type = item.get('type', '')
    prev_rank, prev_url = (prev_info or {}).get(keyword, (101, ""))

    res_data = {
        "keyword": keyword, "exam": exam, "type": kw_type,
        "rank": 101, "url": "No Data", "bucket": "B4 (>20)", "target_rank": 101, "cost": 0,
        "searches": 0, "looks": 0,
        "comp_ranks": {k: 101 for k in COMPETITORS.keys()},
        "comp_urls":  {k: ""  for k in COMPETITORS.keys()}
    }

    searches = 0
    pages = []          # every good page we got: list of (look_number, page_info)
    last_error = None
    look = 0
    max_looks = MAX_LOOKS
    jump_checked = False

    while look < max_looks:
        look += 1
        if look > 1:
            time.sleep(WAIT_BETWEEN_LOOKS)
        data, err, used = _searchapi_call(keyword)
        searches += used
        if err:
            # The call was already tried again inside _searchapi_call, so stop here
            last_error = err
            break
        page = _read_page(data, target_url)
        pages.append((look, page))

        if page["rank"] <= 20:
            # Found. Is it a suspicious jump on a later look?
            if (look >= 2 and not jump_checked and prev_rank <= 20
                    and page["rank"] - prev_rank > JUMP_LIMIT):
                jump_checked = True
                max_looks = look + 1      # one extra look
                continue
            break

    res_data["searches"] = searches
    res_data["looks"]    = look
    res_data["cost"]     = searches * COST_PER_SEARCH

    if not pages:
        # Every look failed with an error
        msg = last_error or "No answer from SearchApi"
        print(f"❌ {keyword}: {msg}")
        _log(run_id, run_type, "error", msg, keyword=keyword, exam=exam, kw_type=kw_type)
        res_data['url'] = f"Err: {msg}"[:250]
        return res_data

    found = [(lk, p) for lk, p in pages if p["rank"] <= 20]
    if found:
        # keep the best (lowest) number among the looks that found EduTap
        lk, p = min(found, key=lambda x: x[1]["rank"])
        res_data.update({"rank": p["rank"], "url": p["url"], "bucket": _bucket(p["rank"]),
                         "target_rank": p["target_rank"], "comp_ranks": p["comp_ranks"],
                         "comp_urls": p["comp_urls"]})
        url_short = p["url"][:80] + "..." if len(p["url"]) > 80 else p["url"]
        look_note = f"found on look {lk} of {look}" if look > 1 else "found on look 1"
        if jump_checked:
            look_note += f" (extra look done because number was far from last rank #{prev_rank})"
        msg = f"Ranked #{p['rank']} | {look_note} | Bucket: {res_data['bucket']} | URL: {url_short}"
        _log(run_id, run_type, "success", msg, keyword=keyword, exam=exam, kw_type=kw_type,
             rank=p["rank"], ranked_url=p["url"])
        return res_data

    # Not found in any look
    lk, p = pages[-1]
    res_data.update({"target_rank": 101, "comp_ranks": p["comp_ranks"], "comp_urls": p["comp_urls"]})

    if prev_rank <= 20 and not _is_not_confirmed(prev_url):
        res_data.update({"rank": prev_rank, "url": NOT_CONFIRMED_LABEL, "bucket": _bucket(prev_rank)})
        msg = (f"Not confirmed — EduTap not seen in {look} looks (Google page had "
               f"{p['organic_count']} normal results). Last rank #{prev_rank} kept, no drop alert. "
               f"Please check this keyword on a phone.")
        _log(run_id, run_type, "warning", msg, keyword=keyword, exam=exam, kw_type=kw_type,
             rank=prev_rank)
    else:
        res_data.update({"rank": 101, "url": "Not Ranked", "bucket": "B4 (>20)"})
        why = (" (was also 'Not confirmed' last time, so now marked as Not in Top 20)"
               if prev_rank <= 20 else "")
        msg = (f"Not in Top 20 — EduTap not seen in {look} looks (Google page had "
               f"{p['organic_count']} normal results){why}")
        _log(run_id, run_type, "info", msg, keyword=keyword, exam=exam, kw_type=kw_type)
    return res_data


# ─────────────────────────────────────────────
# PREV MAP — safe timestamp-filtered build
# Prevents prev/curr rank swap in email
# ─────────────────────────────────────────────
def build_prev_info_safe():
    """{keyword: (last rank, last url)} from runs older than 2 minutes."""
    if not supabase: return {}
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    cutoff  = (ist_now - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M")
    all_rows = []; start = 0
    while True:
        try:
            response = (supabase.table("rankings")
                        .select("keyword, rank, url, date")
                        .lte("date", cutoff)
                        .range(start, start + 999)
                        .execute())
            rows = response.data
            if not rows: break
            all_rows.extend(rows)
            if len(rows) < 1000: break
            start += 1000
        except Exception as e:
            print(f"⚠️ build_prev_info_safe error: {e}")
            break
    if not all_rows: return {}
    df = pd.DataFrame(all_rows)
    df['date_dt'] = pd.to_datetime(df['date'])
    df = df.sort_values('date_dt')
    latest = df.groupby('keyword').tail(1)
    out = {}
    for _, row in latest.iterrows():
        try: r = int(row['rank'])
        except (TypeError, ValueError): r = 101
        out[row['keyword']] = (r, str(row.get('url') or ""))
    return out


def build_prev_map_safe():
    """{keyword: last rank} — kept for the dashboard and email code."""
    return {k: v[0] for k, v in build_prev_info_safe().items()}


# ─────────────────────────────────────────────
# MAIN RUNNER
# Rolling save every 50 + run logging
# ─────────────────────────────────────────────
def perform_update(keywords_list, progress_bar=None, status_text=None, run_type="manual", prev_info=None):
    global _log_buffer, _stop_reason, LAST_RUN_SEARCHES
    _log_buffer = []
    LAST_RUN_SEARCHES = 0
    _stop_reason = None

    ist_now  = datetime.utcnow() + timedelta(hours=5, minutes=30)
    date_str = ist_now.strftime("%Y-%m-%d %H:%M")
    run_id   = date_str

    total           = len(keywords_list)
    total_run_cost  = 0.0
    total_searches  = 0
    completed       = 0
    results_to_save = []
    pending_save    = []
    log_flush_count = 0
    stop_logged     = False

    if prev_info is None:
        prev_info = build_prev_info_safe()

    if not SEARCHAPI_KEY:
        _log(run_id, run_type, "error", "SEARCHAPI_KEY is missing in Secrets. Nothing was checked.")
        flush_logs()
        return date_str, 0.0, []

    _log(run_id, run_type, "info",
         f"Run started — {total} keywords queued | Tool: SearchApi | Place: {SEARCH_LOCATION or 'India'} | "
         f"Device: {SEARCH_DEVICE} | Up to {MAX_LOOKS} looks per keyword | "
         f"Workers: {MAX_WORKERS} | Started at: {date_str} IST")

    def flush_rankings(rows):
        if not rows or not supabase: return
        for i in range(0, len(rows), 500):
            try: supabase.table("rankings").insert(rows[i:i+500]).execute()
            except Exception as e: print(f"❌ Ranking batch save error: {e}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_rank_single, item, run_id, run_type, prev_info): item
                      for item in keywords_list}
        for future in as_completed(future_map):
            try:
                res = future.result()
            except Exception as e:
                item = future_map[future]
                msg = f"Unexpected crash for '{item.get('keyword','')}': {e}"
                print(f"❌ {msg}")
                _log(run_id, run_type, "error", msg,
                     keyword=item.get('keyword',''), exam=item.get('exam',''), kw_type=item.get('type',''))
                continue

            completed += 1
            total_run_cost += res['cost']
            total_searches += res.get('searches', 0)

            if _stop_reason and not stop_logged:
                _log(run_id, run_type, "error", "⛔ " + _stop_reason +
                     " | Remaining keywords were skipped. Check the SearchApi key / monthly searches.")
                stop_logged = True

            # Do not save keywords that only got an error (keeps the last good rank on the dashboard)
            if str(res['url']).startswith("Err:"):
                if status_text: status_text.text(f"Processing... {completed}/{total}")
                if progress_bar: progress_bar.progress(completed / total)
                continue

            cr = res['comp_ranks']; cu = res['comp_urls']
            row = {
                "date": date_str, "keyword": res['keyword'], "exam": res['exam'], "type": res['type'],
                "rank": res['rank'], "url": res['url'], "bucket": res['bucket'], "target_rank": res['target_rank'],
                "rank_anujjindal": cr['anujjindal'], "rank_careerpower": cr['careerpower'],
                "rank_testbook":   cr['testbook'],   "rank_oliveboard":  cr['oliveboard'],
                "rank_adda247":    cr['adda247'],     "rank_ixambee":     cr['ixambee'],
                "url_anujjindal":  cu['anujjindal'],  "url_careerpower":  cu['careerpower'],
                "url_testbook":    cu['testbook'],    "url_oliveboard":   cu['oliveboard'],
                "url_adda247":     cu['adda247'],     "url_ixambee":      cu['ixambee']
            }
            results_to_save.append(row)
            pending_save.append(row)
            log_flush_count += 1

            # Rolling save every SAVE_EVERY
            if len(pending_save) >= SAVE_EVERY:
                flush_rankings(pending_save)
                pending_save.clear()
                print(f"💾 Saved to DB: {completed}/{total}")

            # Rolling log flush every SAVE_EVERY
            if log_flush_count >= SAVE_EVERY:
                flush_logs()
                log_flush_count = 0

            if status_text: status_text.text(f"Processing... {completed}/{total}")
            if progress_bar: progress_bar.progress(completed / total)

    # Final flushes
    if pending_save:
        flush_rankings(pending_save)
        print(f"💾 Final ranking batch saved ({len(pending_save)} rows)")

    LAST_RUN_SEARCHES = total_searches
    not_conf = sum(1 for r in results_to_save if _is_not_confirmed(r['url']))
    avg_looks = (total_searches / len(results_to_save)) if results_to_save else 0
    _log(run_id, run_type, "info",
         f"Run completed — {len(results_to_save)}/{total} keywords saved | "
         f"SearchApi searches used: {total_searches} (avg {avg_looks:.2f} per keyword) | "
         f"Not confirmed: {not_conf} | Estimated cost: ${total_run_cost:.4f}")
    flush_logs()

    try:
        if supabase:
            supabase.table("update_logs").insert({
                "run_date": date_str, "keywords_count": total, "total_cost": total_run_cost
            }).execute()
    except: pass

    return date_str, total_run_cost, results_to_save


# ─────────────────────────────────────────────
# SEARCHAPI ACCOUNT — searches left this month (this check is free)
# ─────────────────────────────────────────────
LOW_CREDITS_WARNING = 1000   # warn in the email when fewer searches than this are left

def get_searchapi_credits():
    """Returns {'left': int, 'used': int, 'allowance': int, 'period_end': str} or None."""
    if not SEARCHAPI_KEY: return None
    try:
        r = requests.get("https://www.searchapi.io/api/v1/me",
                         params={"api_key": SEARCHAPI_KEY}, timeout=15)
        if r.status_code == 200:
            d = r.json() or {}
            acc = d.get("account") or {}
            sub = d.get("subscription") or {}
            # "remaining_credits" can be only the small free bonus, so use allowance - used
            allowance = int(acc.get("monthly_allowance") or 0)
            used = int(acc.get("current_month_usage") or 0)
            bonus = int(acc.get("remaining_credits") or 0)
            return {"left": max(max(0, allowance - used), bonus),
                    "used": used,
                    "allowance": allowance,
                    "period_end": str(sub.get("period_end") or "")[:10]}
    except Exception as e:
        print(f"⚠️ Could not fetch SearchApi searches left: {e}")
    return None


def get_dataforseo_balance():
    """Old name kept so older code keeps working. Now returns SearchApi info (dict) or None."""
    return get_searchapi_credits()


def _credits_banner(credits):
    if not credits or credits.get("left", 0) >= LOW_CREDITS_WARNING:
        return ""
    return (
        "<div style='background:#c0392b;color:white;padding:10px 14px;"
        "border-radius:6px;font-size:14px;margin-bottom:12px;'>"
        f"🚨 <b>SearchApi searches are running low: only {credits['left']:,} left this month. "
        "The next run may stop before all keywords are checked.</b></div>"
    )


def _credits_line(credits):
    if not credits:
        return None
    left = credits.get("left", 0)
    if left < LOW_CREDITS_WARNING:   color, icon = "#c0392b", "🔴"
    elif left < 3000:                color, icon = "#e67e22", "🟠"
    else:                            color, icon = "#27ae60", "🟢"
    txt = (f"<b>SearchApi searches left this month:</b> "
           f"<span style='color:{color};'>{icon} {left:,} of {credits.get('allowance', 0):,}</span>")
    if credits.get("period_end"):
        txt += f" (plan renews {credits['period_end']})"
    return txt


def _subject_suffix(credits):
    return " ⚠️ SEARCHAPI SEARCHES LOW" if credits and credits.get("left", 0) < LOW_CREDITS_WARNING else ""


# ─────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────
def send_email_alert(alerts_dict, subject_prefix="Automatic Run", all_checked_data=None, run_cost=None,
                     credits_left=None, dataforseo_balance=None):
    # dataforseo_balance is the old argument name; it now carries SearchApi info
    credits = credits_left if credits_left is not None else dataforseo_balance
    if not isinstance(credits, dict): credits = None

    recipients = [e.strip() for e in EMAIL_RECEIVER.split(",")] if "," in EMAIL_RECEIVER else [EMAIL_RECEIVER]
    ist_now    = datetime.utcnow() + timedelta(hours=5, minutes=30)
    date_label = ist_now.strftime('%d %b %Y')
    has_alerts = any(alerts_dict.values())
    is_manual  = "Manual" in subject_prefix or "manual" in subject_prefix.lower()

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To']   = ", ".join(recipients)

    def fmt_rank(val):
        return "Not in Top 20" if val > 20 else val

    def generate_grouped_table(items_list):
        if not items_list: return ""
        items_list.sort(key=lambda x: x.get('exam', 'Others'))
        html = "<table border='1' cellpadding='5' style='border-collapse:collapse; width:100%; text-align:left;'>"
        for exam_name, group in groupby(items_list, key=lambda x: x.get('exam', 'Others')):
            html += f"<tr style='background-color:#2c3e50; color:white;'><th colspan='4' style='padding:8px; font-size:14px;'>{exam_name}</th></tr>"
            html += "<tr style='background-color:#ecf0f1;'><th>Type</th><th>Keyword</th><th>Previous Rank</th><th>Current Rank</th></tr>"
            for item in group:
                html += f"<tr><td>{item.get('type','-')}</td><td>{item['kw']}</td><td>{fmt_rank(item['prev'])}</td><td>{fmt_rank(item['curr'])}</td></tr>"
        html += "</table><br>"
        return html

    banner = _credits_banner(credits)
    subj_suffix = _subject_suffix(credits)

    if has_alerts:
        msg['Subject'] = f"{subject_prefix}: SEO Alert ({date_label}){subj_suffix}"
        html_body  = f"<h2>📉 {subject_prefix} Report ({date_label})</h2>" + banner
        html_body += "<p>Significant rank changes:</p>"
        if alerts_dict["red"]:
            html_body += "<h3 style='color:red;'>🔴 Critical: Dropped out of Top 10</h3>" + generate_grouped_table(alerts_dict["red"])
        if alerts_dict["orange"]:
            html_body += "<h3 style='color:orange;'>🟠 Warning: Dropped 4+ Positions</h3>" + generate_grouped_table(alerts_dict["orange"])
        if alerts_dict["yellow"]:
            html_body += "<h3 style='color:#b5b500;'>🟡 Alert: Dropped out of Top 3</h3>" + generate_grouped_table(alerts_dict["yellow"])
        if alerts_dict["green"]:
            html_body += "<h3 style='color:green;'>🟢 Celebration: Entered Top 3!</h3>" + generate_grouped_table(alerts_dict["green"])
    elif is_manual and all_checked_data:
        msg['Subject'] = f"{subject_prefix}: Report Completed ({date_label}){subj_suffix}"
        html_body  = f"<h2>✅ Manual Run Completed ({date_label})</h2>" + banner
        html_body += "<p>No significant alerts. Full status:</p>"
        html_body += generate_grouped_table(all_checked_data)
    else:
        msg['Subject'] = f"{subject_prefix}: All Stable ({date_label}){subj_suffix}"
        html_body  = f"<h2>✅ Automatic Update Completed ({date_label})</h2>" + banner
        html_body += "<p>No significant drops or changes detected. All keywords stable.</p>"

    # ── Cost & searches-left footer (shown in every email) ──────────────
    footer_lines = []
    if run_cost is not None:
        footer_lines.append(f"<b>This run cost (estimate):</b> ${run_cost:.4f}")
    cl = _credits_line(credits)
    if cl: footer_lines.append(cl)
    if footer_lines:
        html_body += (
            "<br><hr style='border:1px solid #ddd;'>"
            "<div style='background:#f8f9fa;padding:10px 14px;border-radius:6px;"
            "font-size:13px;color:#555;margin-top:8px;'>"
            "💰 <b>Run Summary</b><br><br>"
            + "<br>".join(footer_lines)
            + "</div>"
        )

    msg.attach(MIMEText(html_body, 'html'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        server.quit()
        print("📧 Email Sent Successfully!")
    except Exception as e:
        print(f"❌ Email failed: {e}")

# ─────────────────────────────────────────────
# AUTOMATIC (SCHEDULED) EMAIL — XLSX ATTACHMENT ONLY
# The sheet holds ONLY the exams that ran in this run (cron_job.py passes them).
# ─────────────────────────────────────────────
def _display_rank_for_excel(value):
    """Convert the internal 101 sentinel to a user-friendly Excel value."""
    try:
        rank = int(value)
        return rank if rank <= 20 else "Not in Top 20"
    except (TypeError, ValueError):
        return "No Result"


def _build_weekly_excel_report(master_df, results_data, run_date):
    """Build a formatted one-sheet XLSX containing the full current keyword dataset."""
    result_map = {
        str(row.get("keyword", "")).strip(): row
        for row in (results_data or [])
        if str(row.get("keyword", "")).strip()
    }

    try:
        run_dt = datetime.strptime(str(run_date), "%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        run_dt = datetime.utcnow() + timedelta(hours=5, minutes=30)

    date_label = run_dt.strftime("%d %b %Y")
    file_date = run_dt.strftime("%Y-%m-%d")
    filename = f"EduTap_SEO_Report_{file_date}.xlsx"

    headers = [
        "Exam", "Cluster", "Keyword", "Type", "Volume", "Target URL",
        "Current Rank", "Ranked URL", "Target Rank", "Bucket",
        "Anuj Jindal Rank", "Anuj Jindal URL",
        "CareerPower Rank", "CareerPower URL",
        "Testbook Rank", "Testbook URL",
        "Oliveboard Rank", "Oliveboard URL",
        "Adda247 Rank", "Adda247 URL",
        "ixamBee Rank", "ixamBee URL",
    ]

    wb = Workbook()
    ws = wb.active
    ws.title = "All Keywords"
    ws.sheet_view.showGridLines = False

    last_col = get_column_letter(len(headers))
    ws.merge_cells(f"A1:{last_col}1")
    ws["A1"] = "EduTap SEO Keyword Report"
    ws["A1"].font = Font(size=16, bold=True, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor="0097A8")
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 26

    ws.merge_cells(f"A2:{last_col}2")
    exams_in_sheet = ", ".join(sorted({str(e) for e in master_df.get("exam", pd.Series(dtype=str)).dropna().unique()}))
    ws["A2"] = f"Run Date: {date_label} | Keywords: {len(master_df)} | Exams: {exams_in_sheet}"
    ws["A2"].font = Font(size=10, color="666666")
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[2].height = 20

    header_row = 4
    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(bold=True, color="FFFFFF")
    header_border = Border(bottom=Side(style="medium", color="0097A8"))

    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=col_idx, value=header)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = header_border

    rank_columns = {7, 9, 11, 13, 15, 17, 19, 21}
    url_columns = {6, 8, 12, 14, 16, 18, 20, 22}

    light_row_fill = PatternFill("solid", fgColor="F7FBFC")
    top3_fill = PatternFill("solid", fgColor="E2F0D9")
    top10_fill = PatternFill("solid", fgColor="DDEBF7")
    top20_fill = PatternFill("solid", fgColor="FFF2CC")
    not_ranked_fill = PatternFill("solid", fgColor="FCE4D6")

    data_row = header_row + 1
    for _, master_row in master_df.iterrows():
        keyword = str(master_row.get("keyword", "")).strip()
        result = result_map.get(keyword, {})
        comp_ranks = result.get("comp_ranks", {}) or {}
        comp_urls = result.get("comp_urls", {}) or {}
        not_conf = _is_not_confirmed(result.get("url"))

        values = [
            master_row.get("exam", result.get("exam", "")),
            master_row.get("cluster", ""),
            keyword,
            master_row.get("type", result.get("type", "")),
            master_row.get("volume", 0),
            master_row.get("target_url", ""),
            _display_rank_for_excel(result.get("rank")),
            result.get("url", "No Result"),
            "Not confirmed" if not_conf else _display_rank_for_excel(result.get("target_rank")),
            result.get("bucket", "No Result"),
            _display_rank_for_excel(comp_ranks.get("anujjindal")),
            comp_urls.get("anujjindal", ""),
            _display_rank_for_excel(comp_ranks.get("careerpower")),
            comp_urls.get("careerpower", ""),
            _display_rank_for_excel(comp_ranks.get("testbook")),
            comp_urls.get("testbook", ""),
            _display_rank_for_excel(comp_ranks.get("oliveboard")),
            comp_urls.get("oliveboard", ""),
            _display_rank_for_excel(comp_ranks.get("adda247")),
            comp_urls.get("adda247", ""),
            _display_rank_for_excel(comp_ranks.get("ixambee")),
            comp_urls.get("ixambee", ""),
        ]

        for col_idx, value in enumerate(values, start=1):
            if pd.isna(value):
                value = ""
            elif hasattr(value, "item"):
                try:
                    value = value.item()
                except Exception:
                    pass

            cell = ws.cell(row=data_row, column=col_idx, value=value)
            cell.alignment = Alignment(vertical="top", wrap_text=(col_idx in url_columns or col_idx == 3))

            if data_row % 2 == 0:
                cell.fill = light_row_fill

            if col_idx in rank_columns:
                if isinstance(value, int):
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    if value <= 3:
                        cell.fill = top3_fill
                    elif value <= 10:
                        cell.fill = top10_fill
                    else:
                        cell.fill = top20_fill
                elif value in ("Not in Top 20", "No Result", "Not confirmed"):
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    cell.fill = not_ranked_fill

            if col_idx in url_columns and isinstance(value, str) and value.startswith(("http://", "https://")):
                cell.hyperlink = value
                cell.style = "Hyperlink"
                cell.alignment = Alignment(vertical="top", wrap_text=True)

        data_row += 1

    ws.freeze_panes = "A5"
    if data_row > header_row + 1:
        ws.auto_filter.ref = f"A{header_row}:{last_col}{data_row - 1}"

    widths = {
        1: 18, 2: 24, 3: 38, 4: 12, 5: 11, 6: 42,
        7: 14, 8: 48, 9: 14, 10: 16,
        11: 16, 12: 42, 13: 18, 14: 42,
        15: 15, 16: 42, 17: 16, 18: 42,
        19: 15, 20: 42, 21: 15, 22: 42,
    }
    for idx, width in widths.items():
        ws.column_dimensions[get_column_letter(idx)].width = width

    ws.row_dimensions[header_row].height = 32

    output = BytesIO()
    wb.save(output)
    return output.getvalue(), filename, date_label


def send_weekly_email_report(master_df, results_data, run_date, run_cost=None, dataforseo_balance=None,
                             credits_left=None, exam_groups=None, searches_used=None,
                             subject_prefix="📅 Automatic Run"):
    """
    Scheduled email: date + XLSX attachment (only the exams that ran) + run summary.
    exam_groups = {"Every week": [...], "Every 15 days": [...], "Once a month": [...]} — exams that ran.
    """
    credits = credits_left if credits_left is not None else dataforseo_balance
    if not isinstance(credits, dict): credits = None

    recipients = [e.strip() for e in EMAIL_RECEIVER.split(",")] if "," in EMAIL_RECEIVER else [EMAIL_RECEIVER]
    excel_bytes, filename, date_label = _build_weekly_excel_report(master_df, results_data, run_date)

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = f"{subject_prefix}: SEO Report ({date_label}){_subject_suffix(credits)}"

    html_body = f"<h2>📅 {date_label}</h2>"
    html_body += f"<p>📎 <b>Attached:</b> {filename}</p>"
    html_body += _credits_banner(credits)

    if exam_groups:
        html_body += "<p><b>Exams in this run:</b></p><ul>"
        for group_name, exams in exam_groups.items():
            if exams:
                html_body += f"<li><b>{group_name}:</b> {', '.join(exams)}</li>"
        html_body += "</ul>"

    not_conf = sum(1 for r in (results_data or []) if _is_not_confirmed(r.get("url")))
    summary_lines = [f"<b>Keywords checked:</b> {len(master_df)}"]
    if not_conf:
        summary_lines.append(f"<b>Not confirmed</b> (EduTap not seen in 3 looks, last rank kept): {not_conf}")
    if searches_used is not None:
        summary_lines.append(f"<b>SearchApi searches used:</b> {searches_used:,}")
    if run_cost is not None:
        summary_lines.append(f"<b>This run cost (estimate):</b> ${run_cost:.4f}")
    cl = _credits_line(credits)
    if cl: summary_lines.append(cl)

    html_body += (
        "<hr style='border:1px solid #ddd;'>"
        "<div style='background:#f8f9fa;padding:10px 14px;border-radius:6px;"
        "font-size:13px;color:#555;margin-top:8px;'>"
        "💰 <b>Run Summary</b><br><br>"
        + "<br>".join(summary_lines)
        + "</div>"
    )

    msg.attach(MIMEText(html_body, "html"))

    attachment = MIMEApplication(
        excel_bytes,
        _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    attachment.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(attachment)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        server.quit()
        print(f"📧 Automatic XLSX Email Sent Successfully! Attachment: {filename}")
    except Exception as e:
        print(f"❌ Weekly email failed: {e}")
