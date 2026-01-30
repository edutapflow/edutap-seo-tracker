import requests
import time
import pandas as pd
import base64
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client, Client
from config import API_LOGIN, API_PASSWORD, SUPABASE_URL, SUPABASE_KEY, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER

# --- CONNECT TO CLOUD ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
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
    try: supabase.table("keywords_master").delete().in_("keyword", keyword_list).execute()
    except: pass

def clear_master_database():
    try: supabase.table("keywords_master").delete().gt("id", 0).execute()
    except: pass

def normalize_url(url):
    return str(url).lower().replace("https://", "").replace("http://", "").replace("www.", "").strip("/") if url else ""

def process_bulk_upload(uploaded_file, mode="append"):
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

# --- API SCRAPER ---
def fetch_rank_single(item):
    keyword = item['keyword']
    target_url = item.get('target_url', '')
    url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    payload = [{"keyword": keyword, "location_code": 2356, "language_code": "en", "device": "mobile", "os": "android", "depth": 20}]
    auth = "Basic " + base64.b64encode(f"{API_LOGIN}:{API_PASSWORD}".encode()).decode()
    headers = {'Authorization': auth, 'Content-Type': 'application/json'}

    res_data = {
        "keyword": keyword, "exam": item['exam'], "type": item['type'],
        "rank": 101, "url": "No Data", "bucket": "B4 (>20)", "target_rank": 101, "cost": 0,
        "comp_ranks": {k: 101 for k in COMPETITORS.keys()},
        "comp_urls": {k: "" for k in COMPETITORS.keys()}
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        res_data['cost'] = data.get('cost', 0)
        if response.status_code == 200:
            try:
                items = data['tasks'][0]['result'][0]['items']
                best, best_url, target_f = 101, "Not Ranked", 101
                clean_t = normalize_url(target_url)
                comp_found = {k: 101 for k in COMPETITORS.keys()}
                comp_urls_found = {k: "" for k in COMPETITORS.keys()}

                for item in items:
                    if item['type'] == 'organic':
                        r_url = item.get('url', '')
                        clean_r = normalize_url(r_url)
                        grp = item['rank_group']
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
            except: pass
        else: res_data['url'] = f"Err: {data.get('status_message')}"
    except Exception as e: res_data['url'] = f"Err: {str(e)}"
    return res_data

# --- RUNNER ---
def perform_update(keywords_list, progress_bar=None, status_text=None):
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
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
            
    if results_to_save:
        for i in range(0, len(results_to_save), 500):
            try: supabase.table("rankings").insert(results_to_save[i:i+500]).execute()
            except Exception as e: print(f"Error saving batch: {e}")

    try: supabase.table("update_logs").insert({"run_date": date_str, "keywords_count": total, "total_cost": total_run_cost}).execute()
    except: pass

    # RETURN DATA SO DASHBOARD CAN USE IT FOR ALERTS WITHOUT RE-FETCHING
    return date_str, total_run_cost, results_to_save

# --- EMAIL ALERT SYSTEM ---
def send_email_alert(alerts_dict, subject_prefix="Automatic Run"):
    if not any(alerts_dict.values()):
        print("📭 No alerts to send.")
        return

    date_label = datetime.now().strftime('%d %b %Y')
    
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = f"{subject_prefix}: SEO Alert ({date_label})"

    html_body = f"<h2>📉 {subject_prefix} Report ({date_label})</h2>"
    html_body += "<p>Here are the significant rank changes from this run:</p>"
    
    # 🔴 RED
    if alerts_dict["red"]:
        html_body += "<h3 style='color:red;'>🔴 Critical: Dropped out of Top 10</h3>"
        html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
        for item in alerts_dict["red"]:
            html_body += f"<tr><td>{item['kw']}</td><td>{item['curr']}</td><td>{item['prev']}</td></tr>"
        html_body += "</table><br>"

    # 🟠 ORANGE
    if alerts_dict["orange"]:
        html_body += "<h3 style='color:orange;'>🟠 Warning: Dropped 4+ Positions</h3>"
        html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
        for item in alerts_dict["orange"]:
            html_body += f"<tr><td>{item['kw']}</td><td>{item['curr']}</td><td>{item['prev']}</td></tr>"
        html_body += "</table><br>"

    # 🟡 YELLOW
    if alerts_dict["yellow"]:
        html_body += "<h3 style='color:#b5b500;'>🟡 Alert: Dropped out of Top 3</h3>"
        html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
        for item in alerts_dict["yellow"]:
            html_body += f"<tr><td>{item['kw']}</td><td>{item['curr']}</td><td>{item['prev']}</td></tr>"
        html_body += "</table><br>"

    # 🟢 GREEN
    if alerts_dict["green"]:
        html_body += "<h3 style='color:green;'>🟢 Celebration: Entered Top 3!</h3>"
        html_body += "<table border='1' cellpadding='5' style='border-collapse:collapse;'><tr><th>Keyword</th><th>Current</th><th>Previous</th></tr>"
        for item in alerts_dict["green"]:
            html_body += f"<tr><td>{item['kw']}</td><td>{item['curr']}</td><td>{item['prev']}</td></tr>"
        html_body += "</table><br>"

    msg.attach(MIMEText(html_body, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
        print("📧 Email Alert Sent Successfully!")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
