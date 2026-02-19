# FORCE UPDATE V27 - DASHBOARD MATCHING V27 BACKEND
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import altair as alt
from backend_utils import perform_update, get_all_keywords, add_keyword, delete_bulk_keywords, process_bulk_upload, normalize_url, get_current_month_cost, get_live_usd_inr_rate, clear_master_database, supabase, fetch_all_rows, send_email_alert

st.set_page_config(page_title="EduTap SEO Tracker", layout="wide")

# --- 🔒 LOGIN SECURITY ---
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

def check_password():
    def password_entered():
        # --- SAFE GET (Prevents KeyError Crash) ---
        entered = st.session_state.get("password", "")
        if entered == st.secrets["APP_PASSWORD"]:
            st.session_state["logged_in"] = True
            # Only delete if it actually exists
            if "password" in st.session_state:
                del st.session_state["password"]
        else: 
            st.session_state["logged_in"] = False

    if st.session_state['logged_in']: return True
    st.markdown("### 🔒 Private Access Only")
    st.text_input("Enter Password:", type="password", on_change=password_entered, key="password")
    
    # Only show error if password field is not empty but login failed
    if "password" in st.session_state and st.session_state["password"] and not st.session_state['logged_in']: 
        st.error("😕 Password incorrect")
        
    return st.session_state['logged_in']

if not check_password(): st.stop()

# --- MAIN APP ---
if 'is_running' not in st.session_state: st.session_state['is_running'] = False
if 'last_run_date' not in st.session_state: st.session_state['last_run_date'] = None
if 'last_run_cost' not in st.session_state: st.session_state['last_run_cost'] = None
if 'show_run_dialog' not in st.session_state: st.session_state['show_run_dialog'] = False

COMPETITORS_LIST = ["anujjindal", "careerpower", "testbook", "oliveboard", "adda247", "ixambee"]

@st.cache_data(ttl=600) 
def get_ranking_data(): return fetch_all_rows("rankings")

@st.cache_data(ttl=600)
def get_master_data(): return fetch_all_rows("keywords_master")

@st.cache_data(show_spinner=False)
def get_dashboard_view(master_df, history_df):
    if master_df.empty: return pd.DataFrame(), {}
    if not history_df.empty:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        history_df = history_df.sort_values('date_dt')
        latest = history_df.groupby('keyword').tail(1).copy()
        latest = latest[['keyword', 'rank', 'bucket', 'date', 'url', 'target_rank']]
        latest = latest.rename(columns={'rank': 'Current Rank', 'date': 'last_updated', 'url': 'Ranked URL', 'target_rank': 'Target Rank Found'})
    else: latest = pd.DataFrame(columns=['keyword', 'Current Rank', 'bucket', 'last_updated', 'Ranked URL', 'Target Rank Found'])

    merged_df = pd.merge(master_df, latest, on='keyword', how='left')
    prev_rank_map = {}
    if not history_df.empty:
        for kw, grp in history_df.groupby('keyword'):
            if len(grp) >= 2:
                prev_rec = grp.iloc[-2]
                prev_rank_map[kw] = prev_rec['rank']
    
    def process_row(row):
        kw = row['keyword']
        curr = row.get('Current Rank', 101)
        prev = prev_rank_map.get(kw, 101)
        r_url = str(row.get('Ranked URL', ''))
        t_url = str(row.get('target_url', ''))
        t_rank_val = row.get('Target Rank Found', 101)
        
        # Normalize both URLs (strip http, www, trailing slashes)
        clean_r = normalize_url(r_url)
        clean_t = normalize_url(t_url)

        def fmt_rank(val):
            try: v = int(val)
            except: v = 101
            return "Not in Top 20" if v > 20 else v

        disp_curr = fmt_rank(curr); disp_prev = fmt_rank(prev)
        disp_t_curr = fmt_rank(t_rank_val); disp_t_prev = fmt_rank(101) 
        c_val = int(curr) if pd.notna(curr) else 101
        p_val = int(prev) if pd.notna(prev) else 101
        
        alert_status = "Normal"
        if p_val <= 10 and c_val > 10: alert_status = "🔴 Out of Top 10"
        elif (c_val - p_val) >= 4: alert_status = "🟠 Dropped 4+"
        elif p_val <= 3 and c_val > 3: alert_status = "🟡 Out of Top 3"
        elif p_val > 3 and c_val <= 3: alert_status = "🟢 Entered Top 3"

        if not t_url or t_url.lower() in ["nan", "none", ""]:
            status = "⚠️ Target Not Set"; display_t_url = None
        else:
            display_t_url = t_url
            if c_val > 20: 
                status = "❌ Not Ranked"; r_url = None
            elif clean_t and clean_t == clean_r: 
                status = "✅ Matched"
            else: 
                status = "⚠️ Mismatch"
        
        if not r_url or "Err" in r_url: r_url = None
        last_upd = row.get('last_updated', '-')
        bucket = row.get('bucket', 'Pending') if pd.notna(row.get('bucket')) else 'Pending'
        return pd.Series([alert_status, status, disp_curr, disp_prev, r_url, display_t_url, disp_t_curr, disp_t_prev, last_upd, bucket])

    new_cols = ['Alert', 'Keyword Check', 'Ranked URL Rank', 'Ranked URL Pre. Rank', 'Ranked URL', 'Target URL', 'Target URL Rank', 'Target URL Pre. Rank', 'Last Updated', 'Bucket']
    merged_df[new_cols] = merged_df.apply(process_row, axis=1)
    merged_df['Volume'] = merged_df['volume'].fillna(0).astype(int)
    return merged_df, prev_rank_map 

def categorize_cluster(row):
    exam = str(row['exam']).strip()
    cluster = str(row['cluster']).strip().lower()
    if not cluster: return "Others"
    if exam in ["JAIIB", "CAIIB", "JAIIB/CAIIB"]:
        p1_keys = ["pillar", "syllabus", "exam date", "registration", "admit card", "pyq", "previous year"]
        p2_keys = ["pattern", "benefit", "eligibility", "scorecard", "certificate", "result", "preparation", "study material", "analysis", "topper"]
    else:
        p1_keys = ["pillar", "notification", "syllabus", "pyq", "previous year", "salary", "exam date"]
        p2_keys = ["eligibility", "pattern", "cut off", "cutoff", "result", "job profile", "lifestyle", "analysis", "topper", "preparation", "registration", "admit card", "interview", "study material"]
    for k in p1_keys:
        if k in cluster: return "P1"
    for k in p2_keys:
        if k in cluster: return "P2"
    return "Others"

# --- LAYOUT ---
col_header, col_btn = st.columns([6, 1])
with col_header: st.title("📊 EduTap SEO Intelligence")
with col_btn:
    st.write(""); st.write("") 
    if st.button("🔄 Refresh", help="Force update data"): st.cache_data.clear(); st.rerun()

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "📈 Visual Trends", "🏆 Competitors", "🧩 P1/P2 Analysis", "📝 Manage DB"])

with tab1:
    LOCK_ACTIVE = True; LIMIT_INR = 6000 
    if 'usd_rate' not in st.session_state: st.session_state['usd_rate'] = get_live_usd_inr_rate()
    USD_TO_INR = st.session_state['usd_rate']
    spent_usd = get_current_month_cost()
    spent_inr = spent_usd * USD_TO_INR
    remaining_inr = LIMIT_INR - spent_inr
    
    if LOCK_ACTIVE:
        c1, c2 = st.columns([3, 1])
        c1.progress(min(max(spent_inr/LIMIT_INR, 0.0), 1.0), f"📉 Monthly Budget: ₹{spent_inr:,.0f} / ₹{LIMIT_INR:,.0f} (Rate: {USD_TO_INR:.1f})")
        if remaining_inr > 0: c2.success(f"✅ ₹{remaining_inr:,.0f} left")
        else: c2.error("⛔ Exceeded")
    else: st.warning("⚠️ Budget Lock Disabled")

    # --- MANUAL RUN LOGIC ---
    if st.session_state['is_running']:
        if 'pending_update_list' in st.session_state:
            kws = st.session_state['pending_update_list']
            prev_map = st.session_state.get('prev_map_snapshot', {})
            st.toast(f"Updating {len(kws)} keywords...")
            bar = st.progress(0); txt = st.empty()
            
            # Run Update
            r_date, r_cost, results_data = perform_update(kws, bar, txt)
            
            # Generate Alerts
            alerts = {"red": [], "orange": [], "yellow": [], "green": []}
            all_checked_data = []

            for row in results_data:
                kw = row['keyword']
                ex = row['exam']    # <-- Captured Exam
                typ = row['type']   # <-- Captured Type
                curr_rank = row['rank']
                prev_rank = prev_map.get(kw, 101) 
                
                # Save for Full Report
                all_checked_data.append({'kw': kw, 'curr': curr_rank, 'prev': prev_rank, 'exam': ex, 'type': typ})

                if curr_rank > 100 and prev_rank > 100: continue

                # ✅ PASSING EXAM & TYPE TO ALERTS
                alert_obj = {"kw": kw, "curr": curr_rank, "prev": prev_rank, "exam": ex, "type": typ}

                if prev_rank <= 10 and curr_rank > 10:
                    alerts["red"].append(alert_obj)
                elif (curr_rank - prev_rank) >= 4:
                    alerts["orange"].append(alert_obj)
                elif prev_rank <= 3 and curr_rank > 3:
                    alerts["yellow"].append(alert_obj)
                elif prev_rank > 3 and curr_rank <= 3:
                    alerts["green"].append(alert_obj)
            
            # PASS 'all_checked_data' SO IT CAN BE PRINTED IF NO ALERTS
            send_email_alert(alerts, subject_prefix="🛠️ Manual Run", all_checked_data=all_checked_data)
            
            st.session_state['last_run_date'] = r_date; st.session_state['last_run_cost'] = r_cost
            st.session_state['is_running'] = False
            st.session_state['show_run_dialog'] = False # Reset dialog
            del st.session_state['pending_update_list']
            get_ranking_data.clear(); st.rerun()

    if st.session_state.get('last_run_date'):
        st.success(f"✅ Done! Time: {st.session_state['last_run_date']} | Cost: ${st.session_state['last_run_cost']:.4f}")

    master_df = get_master_data()
    history_df = get_ranking_data()
    final_view, prev_rank_map = get_dashboard_view(master_df, history_df)
    
    if not final_view.empty:
        st.divider()
        c_mode1, c_mode2 = st.columns([1, 3])
        with c_mode1: base_opt = st.radio("📊 Base for %:", ["Total Database", "Selected Exam Total", "Selected Type Total"])
        
        # --- FIX 2: NORMALIZE CLUSTERS (TITLE CASE) ---
        final_view['cluster'] = final_view['cluster'].fillna("").astype(str).str.strip().replace(['nan', 'None'], "")
        final_view['cluster'] = final_view['cluster'].apply(lambda x: x.title() if x else "Others")
        
        f1, f2, f3, f4, f5 = st.columns(5)
        
        # Filter Logic
        sel_exam = f1.multiselect("Exam", sorted(final_view['exam'].unique()), placeholder="All Exams")
        
        # --- FIX 2: DYNAMIC CLUSTER FILTER ---
        if sel_exam:
            avail_clusters = sorted(final_view[final_view['exam'].isin(sel_exam)]['cluster'].unique())
        else:
            avail_clusters = sorted(final_view['cluster'].unique())
            
        sel_cluster = f2.multiselect("Cluster", avail_clusters, placeholder="All Clusters")
        sel_type = f3.selectbox("Type", ["All"] + sorted(final_view['type'].unique().tolist()))
        sel_check = f4.selectbox("Check", ["All"] + sorted(final_view['Keyword Check'].unique().tolist()))
        sel_bucket = f5.multiselect("Bucket", sorted(final_view['Bucket'].unique()), placeholder="All Buckets")
        st.write("")
        sel_custom_keywords = st.multiselect("🎯 Select Specific Keywords (Optional)", sorted(final_view['keyword'].unique()))

        df = final_view.copy()
        if sel_exam: df = df[df['exam'].isin(sel_exam)]
        if sel_cluster: df = df[df['cluster'].isin(sel_cluster)]
        if sel_type != "All": df = df[df['type'] == sel_type]
        if sel_check != "All": df = df[df['Keyword Check'] == sel_check]
        if sel_bucket: df = df[df['Bucket'].isin(sel_bucket)]
        if sel_custom_keywords: df = df[df['keyword'].isin(sel_custom_keywords)]

        base_count = len(final_view)
        base_lbl = "Total Database"
        if base_opt == "Selected Exam Total":
            if sel_exam: base_count = len(final_view[final_view['exam'].isin(sel_exam)]); base_lbl = "Selected Exams"
            else: base_lbl = "Total (No Exam Selected)"
        elif base_opt == "Selected Type Total":
            if sel_type != "All":
                if sel_exam: base_count = len(final_view[(final_view['type'] == sel_type) & (final_view['exam'].isin(sel_exam))]); base_lbl = f"Total '{sel_type}' in Selection"
                else: base_count = len(final_view[final_view['type'] == sel_type]); base_lbl = f"Total '{sel_type}'"
            else: base_lbl = "Total (All Types)"

        sel_count = len(df); pct = (sel_count/base_count*100) if base_count>0 else 0
        with c_mode2:
            m1, m2, m3 = st.columns(3)
            m1.metric("Selected Keywords", sel_count)
            m2.metric("Comparison %", f"{pct:.1f}%", help=f"RelativeTo: {base_lbl}")
            m3.metric("Base Context", base_count, help=f"Count of {base_lbl}")

        st.markdown("---")
        est_cost = len(df) * 0.0035 * USD_TO_INR
        can_run = (spent_inr + est_cost) <= LIMIT_INR if LOCK_ACTIVE else True
        
        col_btn, col_msg = st.columns([1, 4])
        with col_btn:
            # --- POP-UP LOGIC ---
            if st.button(f"🚀 Run Update ({len(df)})", type="primary", disabled=not can_run):
                st.session_state['show_run_dialog'] = True
            
            if st.session_state.get('show_run_dialog'):
                with st.form("run_conf_form"):
                    st.write("🔒 **Confirm Manual Update**")
                    run_pass = st.text_input("Enter Admin Run Password:", type="password")
                    c1, c2 = st.columns(2)
                    submit = c1.form_submit_button("✅ Confirm & Run")
                    cancel = c2.form_submit_button("❌ Cancel")
                    
                    if submit:
                        # --- UPDATED: CHECKS NEW SECRET HERE ---
                        if run_pass == st.secrets["RUN_UPDATE_PASSWORD"]:
                            st.session_state['pending_update_list'] = df.to_dict('records')
                            st.session_state['prev_map_snapshot'] = prev_rank_map
                            st.session_state['is_running'] = True
                            st.rerun()
                        else: st.error("Wrong Run Password")
                    
                    if cancel:
                        st.session_state['show_run_dialog'] = False
                        st.rerun()

        with col_msg:
            if not can_run: st.error("Insufficient Budget")
            else: st.caption("Updates visible keywords. Email will be sent for changed ranks.")

        def highlight_alert(row):
            status = row['Alert']
            if "🔴" in status: return ['background-color: #ffcccc; color: black'] * len(row) 
            if "🟠" in status: return ['background-color: #ffe5cc; color: black'] * len(row) 
            if "🟡" in status: return ['background-color: #ffffcc; color: black'] * len(row) 
            if "🟢" in status: return ['background-color: #ccffcc; color: black'] * len(row) 
            return [''] * len(row)

        cols = ["Alert", "exam", "cluster", "keyword", "type", "Keyword Check", "Ranked URL", "Ranked URL Rank", "Ranked URL Pre. Rank", "Target URL", "Target URL Rank", "Target URL Pre. Rank", "Volume", "Last Updated"]
        st.dataframe(df[cols].style.apply(highlight_alert, axis=1), use_container_width=True, hide_index=True, column_config={"Ranked URL": st.column_config.LinkColumn(display_text=r"https?://[^/]+(/.*)"), "Target URL": st.column_config.LinkColumn(display_text=r"https?://[^/]+(/.*)"), "Volume": st.column_config.NumberColumn(format="%d")})

with tab5:
    st.title("📝 Manage Database (Cloud)")
    c1, c2 = st.columns(2)
    
    # ⚠️ FIXED: SAFE CHECK FOR EMPTY DATAFRAME
    if not master_df.empty and 'exam' in master_df.columns:
        ex_count = master_df['exam'].nunique()
        kw_count = len(master_df)
    else:
        ex_count = 0
        kw_count = 0
        
    c1.metric("Exams", ex_count)
    c2.metric("Keywords", kw_count)
    
    st.divider()
    if not master_df.empty:
        editable_df = master_df.copy(); editable_df.insert(0, "Select", False)
        edited_df = st.data_editor(editable_df, hide_index=True, column_config={"Select": st.column_config.CheckboxColumn(required=True)}, disabled=["exam", "keyword", "type", "cluster", "volume", "target_url"], use_container_width=True)
        rows_to_delete = edited_df[edited_df["Select"] == True]
        if not rows_to_delete.empty and st.button(f"🗑️ Delete Selected ({len(rows_to_delete)})", type="primary"):
            delete_bulk_keywords(rows_to_delete['keyword'].tolist()); get_master_data.clear(); st.success("Deleted!"); st.rerun()
    st.divider()
    tb, tm = st.tabs(["📂 Bulk Upload", "➕ Add"])
    with tb:
        f = st.file_uploader("Excel", type=["xlsx"])
        m = st.radio("Mode:", ["Append", "Replace Exam", "⚠️ REPLACE ALL"], horizontal=True)
        wipe = []
        if "Replace Exam" in m: wipe = st.multiselect("Select:", sorted(master_df['exam'].unique()) if not master_df.empty and 'exam' in master_df.columns else [])
        if f and st.button("Process"):
            if "Append" in m: s,t = process_bulk_upload(f, "append")
            elif "Replace Exam" in m:
                if wipe:
                    for e in wipe: 
                        try: supabase.table("keywords_master").delete().eq("exam", e).execute()
                        except: pass
                s,t = process_bulk_upload(f, "append")
            else: s,t = process_bulk_upload(f, "replace_all")
            if s: st.success(t); get_master_data.clear(); st.rerun()
            else: st.error(t)
    with tm:
        with st.form("a"):
            c1,c2,c3 = st.columns(3)
            e = c1.text_input("Exam"); k = c2.text_input("Keyword"); t = c3.selectbox("Type", ["Primary", "Secondary"])
            c4,c5,c6 = st.columns(3)
            cl = c4.text_input("Cluster"); v = c5.number_input("Vol"); u = c6.text_input("URL")
            if st.form_submit_button("Add"):
                add_keyword(e,k,t,cl,v,u); st.success("Added"); st.rerun()
