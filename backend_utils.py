# FORCE UPDATE V16 - DOUBLE CHECK LOGIC & EMAIL FIX
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
        if supabase.table("keywords_master").select("*").eq("keyword", keyword).execute().data:
            return False, f"Keyword '{keyword}' already exists."
        supabase.table("keywords_master").insert({
            "exam": exam, "keyword": keyword, "type": kw_type, 
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
                    rows_to_insert.append({"exam": current_exam, "keyword": p_kw, "type": "Primary", "cluster": cluster, "volume": p_vol, "target_url": t_url})
                
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
                        s_vol = sec_vols[i] if i < len(sec_vols) else 0
                        rows_to_insert.append({"exam": current_exam, "keyword": s_kw, "type": "Secondary", "cluster": cluster, "volume": s_vol, "target_url": t_url})

        if rows_to_insert:
            for i in range(0, len(rows_to_insert), 1000):
                supabase.table("keywords_master").insert(rows_to_insert[i:i+1000]).execute()
        return True, f"Success! Processed {len(rows_to_insert)} keywords."
    except Exception as e: return False, f"Error: {str(e)}"

# --- API SCRAPER (WITH DOUBLE CHECK) ---
def fetch_rank_single(item):
    keyword = item['keyword']
    target_url = item.get('target_url', '')
    
    url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    payload = [{"keyword": keyword, "location_code": 2356, "language_code": "en", "device": "mobile", "os": "android", "depth": 20}]
    auth = "Basic " + base64.b64encode(f"{API_LOGIN}:{API_PASSWORD}".encode()).decode()
    headers = {'Authorization': auth, 'Content-Type': 'application/json'}

    final_res = None
    accumulated_cost = 0.0
    
    # RETRY LOGIC: Try up to 2 times
    for attempt in range(1, 3):
        res_data = {
            "keyword": keyword, "exam": item['exam'], "type": item['type'],
            "rank": 101, "url": "No Data", "bucket": "B4 (>20)", "target_rank": 101, "cost": 0,
            "comp_ranks": {k: 101 for k in COMPETITORS.keys()},
            "comp_urls": {k: "" for k in COMPETITORS.keys()}
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            data = response.json()
            
            # Accumulate cost from this attempt
            this_cost = data.get('cost', 0)
            accumulated_cost += this_cost
            
            if response.status_code == 200:
                try:
                    items = data['tasks'][0]['result'][0]['items']
                    best, best_url, target_f = 101, "Not Ranked", 101
                    clean_t = normalize_url(target_url)
                    comp_found = {k: 101 for k in COMPETITORS.keys()}
                    comp_urls_found = {k: "" for k in COMPETITORS.keys()}

                    for item_res in items:
                        if item_res['type'] == 'organic':
                            r_url = item_res.get('url', '')
                            clean_r = normalize_url(r_url)
                            grp = item_res['rank_group']
                            
                            # Check EduTap
                            if TARGET_DOMAIN in r_url:
                                if grp < best: best, best_url = grp, r_url
                                if clean_t and clean_t in clean_r:
                                    if grp < target_f: target_f = grp
                            
                            # Check Competitors
                            for c_key, c_domain in COMPETITORS.items():
                                if c_domain in r_url:
                                    if grp < comp_found[c_key]:
                                        comp_found[c_key], comp_urls_found[c_key] = grp, r_url 

                    bucket = "B4 (>20)"
                    if best <= 3: bucket = "B1 (1-3)"
                    elif best <= 10: bucket = "B2 (4-10)"
                    elif best <= 20: bucket = "B3 (11-20)"
                    
                    res_data.update({'rank': best, 'url': best_url, 'bucket': bucket, 'target_rank': target_f, 'comp_ranks': comp_found, 'comp_urls': comp_urls_found})
                except: pass
            else: res_data['url'] = f"Err: {data.get('status_message')}"
        except Exception as e: res_data['url'] = f"Err: {str(e)}"

        # DECISION TIME
        if res_data['rank'] <= 20:
            # FOUND IT! Return immediately.
            res_data['cost'] = accumulated_cost
            return res_data
        
        # If we are here, rank > 20.
        # Store this result. If it's the last attempt, we will return it.
        res_data['cost'] = accumulated_cost
        final_res = res_data
        
        # If attempt 1 failed, we loop again to double check.
        if attempt == 1:
            time.sleep(0.5) # Tiny pause before retry

    return final_res

# --- RUNNER ---
def perform_update(keywords_list, progress_bar=None, status_text=None):
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    date_str = ist_now.strftime("%Y-%m-%d %H:%M")
    
    total = len(keywords_list)
    total_run_cost = 0.0
    completed = 0
    results_to_save = []

    with ThreadPoolExecutor(max_workers=15) as executor:
        future_to_kw = {executor.submit(fetch_rank_single, item): item for item in keywords_list}
        for future in as_completed(future_to_kw):
            res = future.result()
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

# --- SMART EMAIL SYSTEM ---
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
    
    # Helper to clean up Rank Numbers
    def fmt_rank(val):
        return "Not in Top 20" if val > 20 else val

    # 1. IF ALERTS EXIST
    if has_alerts:
        msg['Subject'] = f"{subject_prefix}: SEO Alert ({date_label})"
        html_body = f"<h2>📉 {subject_prefix} Report ({date_label})</h2>"
        html_body += "<p>Here are the significant rank changes from this run:</p>"
        
        if alerts_dict["red"]:
            html_body += "<h3 style='color:red;'>🔴 Critical: Dropped out of Top 10</h3>"
            html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
            for item in alerts_dict["red"]:
                html_body += f"<tr><td>{item['kw']}</td><td>{fmt_rank(item['curr'])}</td><td>{fmt_rank(item['prev'])}</td></tr>"
            html_body += "</table><br>"

        if alerts_dict["orange"]:
            html_body += "<h3 style='color:orange;'>🟠 Warning: Dropped 4+ Positions</h3>"
            html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
            for item in alerts_dict["orange"]:
                html_body += f"<tr><td>{item['kw']}</td><td>{fmt_rank(item['curr'])}</td><td>{fmt_rank(item['prev'])}</td></tr>"
            html_body += "</table><br>"

        if alerts_dict["yellow"]:
            html_body += "<h3 style='color:#b5b500;'>🟡 Alert: Dropped out of Top 3</h3>"
            html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
            for item in alerts_dict["yellow"]:
                html_body += f"<tr><td>{item['kw']}</td><td>{fmt_rank(item['curr'])}</td><td>{fmt_rank(item['prev'])}</td></tr>"
            html_body += "</table><br>"

        if alerts_dict["green"]:
            html_body += "<h3 style='color:green;'>🟢 Celebration: Entered Top 3!</h3>"
            html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
            for item in alerts_dict["green"]:
                html_body += f"<tr><td>{item['kw']}</td><td>{fmt_rank(item['curr'])}</td><td>{fmt_rank(item['prev'])}</td></tr>"
            html_body += "</table><br>"

    # 2. IF NO ALERTS + MANUAL RUN -> SHOW FULL REPORT
    elif is_manual and all_checked_data:
        msg['Subject'] = f"{subject_prefix}: Report Completed ({date_label})"
        html_body = f"<h2>✅ Manual Run Completed ({date_label})</h2>"
        html_body += "<p>No significant alerts detected. Here is the full status of keywords checked:</p>"
        html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
        for item in all_checked_data:
            html_body += f"<tr><td>{item['kw']}</td><td>{fmt_rank(item['curr'])}</td><td>{fmt_rank(item['prev'])}</td></tr>"
        html_body += "</table><br>"

    # 3. IF NO ALERTS + AUTOMATIC RUN -> SHORT MESSAGE
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
