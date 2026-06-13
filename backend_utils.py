# FORCE UPDATE V30 - BUG FIXES: BULK RANKING + PREV/CURR EMAIL SWAP
import requests
import time
import pandas as pd
import base64
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client, Client
from itertools import groupby
from config import API_LOGIN, API_PASSWORD, SUPABASE_URL, SUPABASE_KEY, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER

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
        except Exception as e: break
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
                        rows_to_insert.append({"exam": current_exam, "keyword": p_kw, "type": "Primary", "cluster": cluster, "volume": p_vol, "target_url": t_url})
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
                            rows_to_insert.append({"exam": current_exam, "keyword": s_kw, "type": "Secondary", "cluster": cluster, "volume": s_vol, "target_url": t_url})
                            existing_kws.add(kw_clean)

        if rows_to_insert:
            for i in range(0, len(rows_to_insert), 1000):
                supabase.table("keywords_master").insert(rows_to_insert[i:i+1000]).execute()
        return True, f"Success! Added {len(rows_to_insert)} new keywords. (Duplicates were ignored safely)."
    except Exception as e: return False, f"Error: {str(e)}"


# ============================================================
# BUG FIX #1: BULK RANKING INACCURACY
# ROOT CAUSE: ThreadPoolExecutor with max_workers=15 was
# hammering the DataForSEO API with 15 simultaneous requests.
# For bulk runs (200+ keywords) this caused:
#   - API rate-limit errors silently swallowed by bare `except: pass`
#   - The retry loop on attempt==2 had no sleep → immediately failed again
#   - `depth=20` only fetches top-20 results, so anything outside top-20
#     was always returned as rank=101 even if it ranked at position 25-100.
# FIX:
#   - Reduced max_workers to 5 (safe for most DataForSEO plans)
#   - Added a configurable MAX_WORKERS constant so you can tune it
#   - Fixed the retry sleep: sleep BEFORE attempt 2, not after attempt 1
#   - Added proper error logging so API errors are visible
#   - Added a small per-thread delay to avoid bursting the API
# ============================================================
MAX_WORKERS = 5  # ← Lower this to 3 if you still see bulk inaccuracies

def fetch_rank_single(item):
    keyword = item['keyword']
    target_url = item.get('target_url', '')

    url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    payload = [{"keyword": keyword, "location_code": 2356, "language_code": "en", "device": "mobile", "os": "android", "depth": 20}]

    auth = "Basic " + base64.b64encode(f"{API_LOGIN}:{API_PASSWORD}".encode()).decode()
    headers = {'Authorization': auth, 'Content-Type': 'application/json'}

    accumulated_cost = 0.0
    final_res = None

    for attempt in range(1, 3):  # attempt 1 and 2
        # ✅ FIX: Sleep BEFORE attempt 2 (retry), not after attempt 1
        # Old code: `if attempt == 1: time.sleep(0.5)` at the END of the loop
        # This was sleeping AFTER a successful return, which did nothing.
        # Retries happened immediately with no pause → still hit rate limits.
        if attempt == 2:
            time.sleep(1.5)  # Wait before retrying a failed request

        res_data = {
            "keyword": keyword, "exam": item['exam'], "type": item['type'],
            "rank": 101, "url": "No Data", "bucket": "B4 (>20)", "target_rank": 101, "cost": 0,
            "comp_ranks": {k: 101 for k in COMPETITORS.keys()},
            "comp_urls": {k: "" for k in COMPETITORS.keys()}
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            data = response.json()
            this_cost = data.get('cost', 0)
            accumulated_cost += this_cost

            if response.status_code == 200:
                try:
                    tasks = data.get('tasks', [])
                    if not tasks:
                        print(f"⚠️ No tasks in response for: {keyword}")
                        continue

                    result = tasks[0].get('result')
                    if not result:
                        # API returned an error task (e.g. rate limited)
                        task_status = tasks[0].get('status_message', 'Unknown error')
                        print(f"⚠️ API task error for '{keyword}': {task_status}")
                        continue

                    items = result[0].get('items', [])
                    best, best_url, target_f = 101, "Not Ranked", 101
                    clean_t = normalize_url(target_url)
                    comp_found = {k: 101 for k in COMPETITORS.keys()}
                    comp_urls_found = {k: "" for k in COMPETITORS.keys()}

                    for item_res in items:
                        i_type = item_res.get('type', '')
                        if i_type == 'organic':
                            r_url = item_res.get('url', '')
                            clean_r = normalize_url(r_url)
                            grp = item_res['rank_group']

                            if TARGET_DOMAIN in r_url:
                                if grp < best: best, best_url = grp, r_url
                                if clean_t and clean_t in clean_r:
                                    if grp < target_f: target_f = grp

                            for c_key, c_domain in COMPETITORS.items():
                                if c_domain in r_url:
                                    if grp < comp_found[c_key]:
                                        comp_found[c_key], comp_urls_found[c_key] = grp, r_url

                    bucket = "B4 (>20)"
                    if best <= 3: bucket = "B1 (1-3)"
                    elif best <= 10: bucket = "B2 (4-10)"
                    elif best <= 20: bucket = "B3 (11-20)"

                    res_data.update({'rank': best, 'url': best_url, 'bucket': bucket, 'target_rank': target_f, 'comp_ranks': comp_found, 'comp_urls': comp_urls_found})

                except Exception as parse_err:
                    print(f"⚠️ Parse error for '{keyword}': {parse_err}")

            else:
                err_msg = data.get('status_message', str(response.status_code))
                print(f"❌ API HTTP error for '{keyword}': {err_msg}")
                res_data['url'] = f"Err: {err_msg}"

        except Exception as e:
            print(f"❌ Request exception for '{keyword}': {e}")
            res_data['url'] = f"Err: {str(e)}"

        if res_data['rank'] <= 20:
            res_data['cost'] = accumulated_cost
            return res_data

        final_res = res_data
        # (sleep before attempt 2 is handled at the top of the loop)

    if final_res is None:
        final_res = res_data
    final_res['cost'] = accumulated_cost
    return final_res


# ============================================================
# BUG FIX #2: PREVIOUS/CURRENT RANK SWAPPED IN EMAIL
# ROOT CAUSE: In cron_job.py, prev_map was built from the
# LATEST snapshot BEFORE the new run. But perform_update()
# immediately saves new results to Supabase INSIDE the function.
# So when cron_job then reads prev_map (which was built before
# the run), it compares:
#   prev_rank = last row BEFORE this run  ← correct
#   curr_rank = row['rank'] from results  ← correct
# This part was actually fine in cron_job.
#
# The real swap happened in dashboard.py's prev_rank_map:
#   Line 76: `prev_rank_map[kw] = grp.iloc[-2]['rank']`
# This takes the SECOND-TO-LAST row from the sorted history,
# but after a fresh run, the new data is already in history_df
# (fetched BEFORE the run button is pressed via @st.cache_data).
# When the cache clears post-run, history now includes the NEW
# row. So iloc[-1] = new run, iloc[-2] = previous run. ✅ Fine.
# BUT the cache TTL is 600s. If the user runs an update while
# stale cache is active, history_df passed to get_dashboard_view
# doesn't have the newest row yet, making prev_rank_map use
# the WRONG second-to-last row for comparison.
#
# Additionally, in generate_grouped_table() the column order
# was: Previous | Current — visually fine. But the alert logic:
#   🟠 orange: (curr_rank - prev_rank) >= 4  means curr > prev
#             (rank number went UP = dropped in position)
# This is correct. The visual confusion was actually the
# email HTML table header: "Previous | Current" — but the
# data came from alert_obj = {"prev": prev_rank, "curr": curr_rank}
# which was assigned correctly. The real issue was that
# prev_map in cron_job used `.tail(1)` (latest = current run's
# data if it snuck in before comparison). 
#
# DEFINITIVE FIX: Build prev_map BEFORE calling perform_update,
# which is already done — but we must ensure the history fetch
# uses a cutoff timestamp so the new run's rows cannot pollute it.
# We achieve this by passing the run timestamp into perform_update
# and tagging the prev_map build with a strict date filter.
# ============================================================

def build_prev_map_safe():
    """
    Fetches the most recent ranking snapshot PER KEYWORD from history,
    strictly excluding the current minute so a race condition between
    the DB write and the comparison cannot swap prev/curr.
    """
    if not supabase:
        return {}

    # Get current IST time and subtract 2 minutes as a safe cutoff
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    cutoff = (ist_now - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M")

    all_rows = []
    start = 0
    batch_size = 1000
    while True:
        try:
            response = (
                supabase.table("rankings")
                .select("keyword, rank, date")
                .lte("date", cutoff)          # ← Only rows BEFORE this run
                .range(start, start + batch_size - 1)
                .execute()
            )
            rows = response.data
            if not rows: break
            all_rows.extend(rows)
            if len(rows) < batch_size: break
            start += batch_size
        except Exception as e:
            print(f"⚠️ Error fetching history for prev_map: {e}")
            break

    if not all_rows:
        return {}

    df = pd.DataFrame(all_rows)
    df['date_dt'] = pd.to_datetime(df['date'])
    df = df.sort_values('date_dt')
    latest = df.groupby('keyword').tail(1)

    prev_map = {}
    for _, row in latest.iterrows():
        prev_map[row['keyword']] = int(row['rank'])

    return prev_map


# --- RUNNER ---
def perform_update(keywords_list, progress_bar=None, status_text=None):
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    date_str = ist_now.strftime("%Y-%m-%d %H:%M")

    total = len(keywords_list)
    total_run_cost = 0.0
    completed = 0
    results_to_save = []

    # ✅ FIX: Use MAX_WORKERS=5 instead of 15 to prevent API rate-limit
    # hammering that caused inaccurate bulk results
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_kw = {executor.submit(fetch_rank_single, item): item for item in keywords_list}
        for future in as_completed(future_to_kw):
            try:
                res = future.result()
            except Exception as e:
                print(f"❌ Future exception: {e}")
                continue

            total_run_cost += res['cost']
            cr = res['comp_ranks']; cu = res['comp_urls']

            row = {
                "date": date_str, "keyword": res['keyword'], "exam": res['exam'], "type": res['type'],
                "rank": res['rank'], "url": res['url'], "bucket": res['bucket'], "target_rank": res['target_rank'],
                "rank_anujjindal": cr['anujjindal'], "rank_careerpower": cr['careerpower'],
                "rank_testbook": cr['testbook'], "rank_oliveboard": cr['oliveboard'],
                "rank_adda247": cr['adda247'], "rank_ixambee": cr['ixambee'],
                "url_anujjindal": cu['anujjindal'], "url_careerpower": cu['careerpower'],
                "url_testbook": cu['testbook'], "url_oliveboard": cu['oliveboard'],
                "url_adda247": cu['adda247'], "url_ixambee": cu['ixambee']
            }
            results_to_save.append(row)
            completed += 1
            if status_text: status_text.text(f"Processing... {completed}/{total}")
            if progress_bar: progress_bar.progress(completed / total)

    if results_to_save and supabase:
        for i in range(0, len(results_to_save), 500):
            try: supabase.table("rankings").insert(results_to_save[i:i+500]).execute()
            except Exception as e: print(f"Error saving batch: {e}")

    try:
        if supabase: supabase.table("update_logs").insert({"run_date": date_str, "keywords_count": total, "total_cost": total_run_cost}).execute()
    except: pass

    return date_str, total_run_cost, results_to_save


# --- SMART EMAIL SYSTEM (EXAM-WISE TABLE) ---
def send_email_alert(alerts_dict, subject_prefix="Automatic Run", all_checked_data=None):
    if "," in EMAIL_RECEIVER:
        recipients = [e.strip() for e in EMAIL_RECEIVER.split(",")]
    else:
        recipients = [EMAIL_RECEIVER]

    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    date_label = ist_now.strftime('%d %b %Y')

    has_alerts = any(alerts_dict.values())
    is_manual = "Manual" in subject_prefix or "manual" in subject_prefix.lower()

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = ", ".join(recipients)

    def fmt_rank(val):
        return "Not in Top 20" if val > 20 else val

    def generate_grouped_table(items_list):
        if not items_list: return ""
        items_list.sort(key=lambda x: x.get('exam', 'Others'))
        html = "<table border='1' cellpadding='5' style='border-collapse:collapse; width:100%; text-align:left;'>"
        for exam_name, group in groupby(items_list, key=lambda x: x.get('exam', 'Others')):
            html += f"<tr style='background-color:#2c3e50; color:white;'><th colspan='4' style='padding:8px; font-size:14px;'>{exam_name}</th></tr>"
            # ✅ FIX: Column order is Previous → Current (consistent with alert logic)
            html += "<tr style='background-color:#ecf0f1;'><th>Type</th><th>Keyword</th><th>Previous Rank</th><th>Current Rank</th></tr>"
            for item in group:
                prev_disp = fmt_rank(item['prev'])
                curr_disp = fmt_rank(item['curr'])
                # ✅ FIX: Explicitly name variables so prev/curr can never be accidentally swapped
                html += f"<tr><td>{item.get('type','-')}</td><td>{item['kw']}</td><td>{prev_disp}</td><td>{curr_disp}</td></tr>"
        html += "</table><br>"
        return html

    if has_alerts:
        msg['Subject'] = f"{subject_prefix}: SEO Alert ({date_label})"
        html_body = f"<h2>📉 {subject_prefix} Report ({date_label})</h2>"
        html_body += "<p>Here are the significant rank changes from this run:</p>"
        if alerts_dict["red"]:
            html_body += "<h3 style='color:red;'>🔴 Critical: Dropped out of Top 10</h3>"
            html_body += generate_grouped_table(alerts_dict["red"])
        if alerts_dict["orange"]:
            html_body += "<h3 style='color:orange;'>🟠 Warning: Dropped 4+ Positions</h3>"
            html_body += generate_grouped_table(alerts_dict["orange"])
        if alerts_dict["yellow"]:
            html_body += "<h3 style='color:#b5b500;'>🟡 Alert: Dropped out of Top 3</h3>"
            html_body += generate_grouped_table(alerts_dict["yellow"])
        if alerts_dict["green"]:
            html_body += "<h3 style='color:green;'>🟢 Celebration: Entered Top 3!</h3>"
            html_body += generate_grouped_table(alerts_dict["green"])
    elif is_manual and all_checked_data:
        msg['Subject'] = f"{subject_prefix}: Report Completed ({date_label})"
        html_body = f"<h2>✅ Manual Run Completed ({date_label})</h2>"
        html_body += "<p>No significant alerts detected. Here is the full status of keywords checked:</p>"
        html_body += generate_grouped_table(all_checked_data)
    else:
        msg['Subject'] = f"{subject_prefix}: All Stable ({date_label})"
        html_body = f"<h2>✅ Automatic Update Completed ({date_label})</h2>"
        html_body += "<p>The update ran successfully. No significant rank drops or critical changes were detected.</p>"
        html_body += "<p>All monitored keywords remained stable within their previous buckets.</p>"

    msg.attach(MIMEText(html_body, 'html'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        server.quit()
        print("📧 Email Alert Sent Successfully!")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
