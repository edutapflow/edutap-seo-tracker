# FORCE UPDATE V35 - RUN LOGS TAB + ALL PREVIOUS FIXES
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import altair as alt
from backend_utils import (
    perform_update, get_all_keywords, add_keyword, delete_bulk_keywords,
    process_bulk_upload, normalize_url, get_current_month_cost,
    get_live_usd_inr_rate, clear_master_database, supabase,
    fetch_all_rows, send_email_alert, build_prev_map_safe,
    fetch_run_ids, fetch_logs_for_run
)

st.set_page_config(page_title="EduTap SEO Tracker", layout="wide")

# ─────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

def check_password():
    def password_entered():
        if st.session_state.get("password", "") == st.secrets["APP_PASSWORD"]:
            st.session_state["logged_in"] = True
            if "password" in st.session_state: del st.session_state["password"]
        else:
            st.session_state["logged_in"] = False

    if st.session_state['logged_in']: return True
    st.markdown("### 🔒 Private Access Only")
    st.text_input("Enter Password:", type="password", on_change=password_entered, key="password")
    if "password" in st.session_state and st.session_state["password"] and not st.session_state['logged_in']:
        st.error("😕 Password incorrect")
    return st.session_state['logged_in']

if not check_password(): st.stop()

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if 'is_running'       not in st.session_state: st.session_state['is_running']       = False
if 'last_run_date'    not in st.session_state: st.session_state['last_run_date']    = None
if 'last_run_cost'    not in st.session_state: st.session_state['last_run_cost']    = None
if 'show_run_dialog'  not in st.session_state: st.session_state['show_run_dialog']  = False
if 'logs_unlocked'    not in st.session_state: st.session_state['logs_unlocked']    = False

COMPETITORS_LIST = ["anujjindal", "careerpower", "testbook", "oliveboard", "adda247", "ixambee"]

@st.cache_data(ttl=600)
def get_ranking_data(): return fetch_all_rows("rankings")

@st.cache_data(ttl=600)
def get_master_data():  return fetch_all_rows("keywords_master")

@st.cache_data(show_spinner=False)
def get_dashboard_view(master_df, history_df):
    date_labels = ["Current", "Prev", "-2", "-3"]
    if master_df.empty: return pd.DataFrame(), {}, date_labels

    past_data = []; prev_rank_map = {}

    if not history_df.empty:
        history_df = history_df.copy()
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        history_df['day']     = history_df['date_dt'].dt.strftime('%b %d')
        unique_days   = history_df.sort_values('date_dt', ascending=False)['day'].unique()[:4]
        date_labels   = list(unique_days)
        while len(date_labels) < 4: date_labels.append("N/A")

        for kw, grp in history_df.groupby('keyword'):
            grp     = grp.sort_values('date_dt')
            kw_data = {'keyword': kw, 'URL-0': "", 'bucket': 'Pending', 'Date-0': "-"}
            for i in range(4): kw_data[f'R-{i}'] = 101; kw_data[f'T-{i}'] = 101

            unique_days_for_kw = grp['day'].unique()
            if len(unique_days_for_kw) >= 2:
                second_day = sorted(unique_days_for_kw, reverse=True)[1]
                prev_rank_map[kw] = int(grp[grp['day'] == second_day].iloc[-1]['rank'])
            else:
                prev_rank_map[kw] = 101

            latest_per_day = grp.groupby('day').last()
            for i, day in enumerate(date_labels):
                if day != "N/A" and day in latest_per_day.index:
                    kw_data[f'R-{i}'] = latest_per_day.loc[day, 'rank']
                    kw_data[f'T-{i}'] = latest_per_day.loc[day, 'target_rank']
                    if i == 0:
                        kw_data['URL-0']  = latest_per_day.loc[day, 'url']
                        kw_data['bucket'] = latest_per_day.loc[day, 'bucket']
                        kw_data['Date-0'] = latest_per_day.loc[day, 'date']
            past_data.append(kw_data)

        past_df = pd.DataFrame(past_data) if past_data else pd.DataFrame(columns=['keyword', 'bucket'])
    else:
        past_df = pd.DataFrame(columns=['keyword', 'bucket'])

    merged_df = pd.merge(master_df, past_df, on='keyword', how='left')

    def process_row(row):
        curr = int(row.get('R-0', 101)) if pd.notna(row.get('R-0')) else 101
        prev = int(row.get('R-1', 101)) if pd.notna(row.get('R-1')) else 101
        r_url = str(row.get('URL-0', '')); t_url = str(row.get('target_url', ''))
        clean_r = normalize_url(r_url); clean_t = normalize_url(t_url)

        def fmt(val):
            try: v = int(val)
            except: v = 101
            return "Not in Top 20" if v > 20 else v

        alert_status = "Normal"
        if   prev <= 10 and curr > 10:         alert_status = "🔴 Out of Top 10"
        elif (curr - prev) >= 4:               alert_status = "🟠 Dropped 4+"
        elif prev <= 3 and curr > 3:            alert_status = "🟡 Out of Top 3"
        elif prev > 3 and curr <= 3:            alert_status = "🟢 Entered Top 3"

        if not t_url or t_url.lower() in ["nan", "none", ""]:
            status = "⚠️ Target Not Set"; display_t_url = None
        else:
            display_t_url = t_url
            if curr > 20:                      status = "❌ Not Ranked"; r_url = None
            elif clean_t and clean_t == clean_r: status = "✅ Matched"
            else:                              status = "⚠️ Mismatch"

        if not r_url or "Err" in r_url: r_url = None
        raw_date = str(row.get('Date-0', '-'))
        last_upd = raw_date[:16] if raw_date != '-' else '-'
        bucket   = row.get('bucket', 'Pending') if pd.notna(row.get('bucket')) else 'Pending'

        return pd.Series([
            alert_status, status,
            fmt(curr), fmt(prev), fmt(row.get('R-2',101)), fmt(row.get('R-3',101)),
            r_url, display_t_url,
            fmt(row.get('T-0',101)), fmt(row.get('T-1',101)), fmt(row.get('T-2',101)), fmt(row.get('T-3',101)),
            last_upd, bucket
        ])

    new_cols = [
        'Alert', 'Keyword Check',
        f'Rank ({date_labels[0]})', f'Rank ({date_labels[1]})', f'Rank ({date_labels[2]})', f'Rank ({date_labels[3]})',
        'Ranked URL', 'Target URL',
        f'Target ({date_labels[0]})', f'Target ({date_labels[1]})', f'Target ({date_labels[2]})', f'Target ({date_labels[3]})',
        'Last Updated', 'Bucket'
    ]
    merged_df[new_cols] = merged_df.apply(process_row, axis=1)
    merged_df['Volume'] = merged_df['volume'].fillna(0).astype(int)
    return merged_df, prev_rank_map, date_labels

def categorize_cluster(row):
    exam = str(row['exam']).strip(); cluster = str(row['cluster']).strip().lower()
    if not cluster: return "Others"
    if exam in ["JAIIB", "CAIIB", "JAIIB/CAIIB"]:
        p1_keys = ["pillar","syllabus","exam date","registration","admit card","pyq","previous year"]
        p2_keys = ["pattern","benefit","eligibility","scorecard","certificate","result","preparation","study material","analysis","topper"]
    else:
        p1_keys = ["pillar","notification","syllabus","pyq","previous year","salary","exam date"]
        p2_keys = ["eligibility","pattern","cut off","cutoff","result","job profile","lifestyle","analysis","topper","preparation","registration","admit card","interview","study material"]
    for k in p1_keys:
        if k in cluster: return "P1"
    for k in p2_keys:
        if k in cluster: return "P2"
    return "Others"

# ─────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────
col_header, col_btn = st.columns([6, 1])
with col_header: st.title("📊 EduTap SEO Intelligence")
with col_btn:
    st.write(""); st.write("")
    if st.button("🔄 Refresh"): st.cache_data.clear(); st.rerun()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Dashboard", "📈 Visual Trends", "🏆 Competitors",
    "🧩 P1/P2 Analysis", "📝 Manage DB", "📋 Run Logs"
])

# ─────────────────────────────────────────────
# TAB 1 — DASHBOARD
# ─────────────────────────────────────────────
with tab1:
    LOCK_ACTIVE = True; LIMIT_INR = 6000
    if 'usd_rate' not in st.session_state: st.session_state['usd_rate'] = get_live_usd_inr_rate()
    USD_TO_INR   = st.session_state['usd_rate']
    spent_usd    = get_current_month_cost()
    spent_inr    = spent_usd * USD_TO_INR
    remaining_inr = LIMIT_INR - spent_inr

    if LOCK_ACTIVE:
        c1, c2 = st.columns([3, 1])
        c1.progress(min(max(spent_inr/LIMIT_INR, 0.0), 1.0),
                    f"📉 Monthly Budget: ₹{spent_inr:,.0f} / ₹{LIMIT_INR:,.0f} (Rate: {USD_TO_INR:.1f})")
        if remaining_inr > 0: c2.success(f"✅ ₹{remaining_inr:,.0f} left")
        else: c2.error("⛔ Exceeded")
    else: st.warning("⚠️ Budget Lock Disabled")

    if st.session_state['is_running']:
        if 'pending_update_list' in st.session_state:
            kws      = st.session_state['pending_update_list']
            prev_map = build_prev_map_safe()
            st.toast(f"Updating {len(kws)} keywords...")
            bar = st.progress(0); txt = st.empty()

            r_date, r_cost, results_data = perform_update(kws, bar, txt, run_type="manual")

            alerts = {"red": [], "orange": [], "yellow": [], "green": []}
            all_checked_data = []

            for row in results_data:
                kw = row['keyword']; ex = row['exam']; typ = row['type']
                curr_rank = row['rank']; prev_rank = prev_map.get(kw, 101)
                all_checked_data.append({'kw': kw, 'curr': curr_rank, 'prev': prev_rank, 'exam': ex, 'type': typ})
                if curr_rank > 100 and prev_rank > 100: continue
                alert_obj = {"kw": kw, "curr": curr_rank, "prev": prev_rank, "exam": ex, "type": typ}
                if   prev_rank <= 10 and curr_rank > 10:       alerts["red"].append(alert_obj)
                elif (curr_rank - prev_rank) >= 4:             alerts["orange"].append(alert_obj)
                elif prev_rank <= 3 and curr_rank > 3:          alerts["yellow"].append(alert_obj)
                elif prev_rank > 3 and curr_rank <= 3:          alerts["green"].append(alert_obj)

            send_email_alert(alerts, subject_prefix="🛠️ Manual Run", all_checked_data=all_checked_data)
            st.session_state['last_run_date'] = r_date; st.session_state['last_run_cost'] = r_cost
            st.session_state['is_running']    = False;  st.session_state['show_run_dialog'] = False
            del st.session_state['pending_update_list']
            get_ranking_data.clear(); st.rerun()

    if st.session_state.get('last_run_date'):
        st.success(f"✅ Done! Time: {st.session_state['last_run_date']} | Cost: ${st.session_state['last_run_cost']:.4f}")

    master_df  = get_master_data()
    history_df = get_ranking_data()
    final_view, prev_rank_map, date_labels = get_dashboard_view(master_df, history_df)

    if not final_view.empty:
        st.divider()
        c_mode1, c_mode2 = st.columns([1, 3])
        with c_mode1: base_opt = st.radio("📊 Base for %:", ["Total Database", "Selected Exam Total", "Selected Type Total"])

        final_view['cluster'] = final_view['cluster'].fillna("").astype(str).str.strip().replace(['nan','None'],"")
        final_view['cluster'] = final_view['cluster'].apply(lambda x: x.title() if x else "Others")

        f1, f2, f3, f4, f5 = st.columns(5)
        sel_exam    = f1.multiselect("Exam",   sorted(final_view['exam'].unique()),   placeholder="All Exams")
        avail_clusters = sorted(final_view[final_view['exam'].isin(sel_exam)]['cluster'].unique()) if sel_exam else sorted(final_view['cluster'].unique())
        sel_cluster = f2.multiselect("Cluster", avail_clusters, placeholder="All Clusters")
        sel_type    = f3.selectbox("Type",  ["All"] + sorted(final_view['type'].unique().tolist()))
        sel_check   = f4.selectbox("Check", ["All"] + sorted(final_view['Keyword Check'].unique().tolist()))
        sel_bucket  = f5.multiselect("Bucket", sorted(final_view['Bucket'].unique()), placeholder="All Buckets")
        st.write("")
        sel_custom_keywords = st.multiselect("🎯 Select Specific Keywords (Optional)", sorted(final_view['keyword'].unique()))

        df = final_view.copy()
        if sel_exam:            df = df[df['exam'].isin(sel_exam)]
        if sel_cluster:         df = df[df['cluster'].isin(sel_cluster)]
        if sel_type != "All":   df = df[df['type'] == sel_type]
        if sel_check != "All":  df = df[df['Keyword Check'] == sel_check]
        if sel_bucket:          df = df[df['Bucket'].isin(sel_bucket)]
        if sel_custom_keywords: df = df[df['keyword'].isin(sel_custom_keywords)]

        base_count = len(final_view); base_lbl = "Total Database"
        if base_opt == "Selected Exam Total":
            if sel_exam: base_count = len(final_view[final_view['exam'].isin(sel_exam)]); base_lbl = "Selected Exams"
        elif base_opt == "Selected Type Total" and sel_type != "All":
            base_count = len(final_view[final_view['type'] == sel_type]); base_lbl = f"Total '{sel_type}'"
            if sel_exam: base_count = len(final_view[(final_view['type']==sel_type)&(final_view['exam'].isin(sel_exam))])

        sel_count = len(df); pct = (sel_count/base_count*100) if base_count > 0 else 0
        with c_mode2:
            m1, m2, m3 = st.columns(3)
            m1.metric("Selected Keywords", sel_count)
            m2.metric("Comparison %", f"{pct:.1f}%", help=f"Relative to: {base_lbl}")
            m3.metric("Base Context", base_count, help=f"Count of {base_lbl}")

        st.markdown("---")
        est_cost = len(df) * 0.0035 * USD_TO_INR
        can_run  = (spent_inr + est_cost) <= LIMIT_INR if LOCK_ACTIVE else True

        col_btn2, col_msg = st.columns([1, 4])
        with col_btn2:
            if st.button(f"🚀 Run Update ({len(df)})", type="primary", disabled=not can_run):
                st.session_state['show_run_dialog'] = True

            if st.session_state.get('show_run_dialog'):
                with st.form("run_conf_form"):
                    st.write("🔒 **Confirm Manual Update**")
                    run_pass = st.text_input("Enter Admin Run Password:", type="password")
                    c1, c2  = st.columns(2)
                    submit  = c1.form_submit_button("✅ Confirm & Run")
                    cancel  = c2.form_submit_button("❌ Cancel")
                    if submit:
                        if run_pass == st.secrets["RUN_UPDATE_PASSWORD"]:
                            st.session_state['pending_update_list'] = df.to_dict('records')
                            st.session_state['is_running']          = True
                            st.rerun()
                        else: st.error("Wrong Run Password")
                    if cancel:
                        st.session_state['show_run_dialog'] = False
                        st.rerun()
        with col_msg:
            if not can_run: st.error("Insufficient Budget")
            else: st.caption("Updates visible keywords. Email will be sent for changed ranks.")

        def highlight_alert(row):
            s = row['Alert']
            if "🔴" in s: return ['background-color:#ffcccc;color:black'] * len(row)
            if "🟠" in s: return ['background-color:#ffe5cc;color:black'] * len(row)
            if "🟡" in s: return ['background-color:#ffffcc;color:black'] * len(row)
            if "🟢" in s: return ['background-color:#ccffcc;color:black'] * len(row)
            return [''] * len(row)

        cols = [
            "Alert", "exam", "cluster", "keyword", "type", "Keyword Check", "Ranked URL",
            f"Rank ({date_labels[0]})", f"Rank ({date_labels[1]})", f"Rank ({date_labels[2]})", f"Rank ({date_labels[3]})",
            "Target URL",
            f"Target ({date_labels[0]})", f"Target ({date_labels[1]})", f"Target ({date_labels[2]})", f"Target ({date_labels[3]})",
            "Volume", "Last Updated"
        ]
        st.dataframe(
            df[cols].style.apply(highlight_alert, axis=1),
            use_container_width=True, hide_index=True,
            column_config={
                "Ranked URL": st.column_config.LinkColumn(display_text=r"https?://[^/]+(/.*)" ),
                "Target URL": st.column_config.LinkColumn(display_text=r"https?://[^/]+(/.*)" ),
                "Volume":     st.column_config.NumberColumn(format="%d")
            }
        )

# ─────────────────────────────────────────────
# TAB 2 — VISUAL TRENDS
# ─────────────────────────────────────────────
with tab2:
    st.title("📈 Keyword Rank Trends")
    if history_df.empty: st.info("No history data yet.")
    else:
        valid_kws    = set(master_df['keyword'].unique()) if not master_df.empty else set()
        history_clean = history_df[history_df['keyword'].isin(valid_kws)]
        if history_clean.empty: st.warning("No trend data matches current keyword list.")
        else:
            kws   = sorted(history_clean['keyword'].unique())
            sel_k = st.multiselect("Select Keyword(s):", kws)
            c1, c2 = st.columns(2)
            today  = datetime.now().date()
            start_d = c1.date_input("Start Date", today - timedelta(days=30))
            end_d   = c2.date_input("End Date", today)
            if sel_k:
                chart_data = history_clean[history_clean['keyword'].isin(sel_k)].copy()
                chart_data['date_dt'] = pd.to_datetime(chart_data['date'])
                chart_data = chart_data[(chart_data['date_dt'].dt.date >= start_d) & (chart_data['date_dt'].dt.date <= end_d)]
                if chart_data.empty: st.warning("No data for this date range.")
                else:
                    chart_data['Day'] = chart_data['date_dt'].dt.date
                    chart_data = chart_data.sort_values('date_dt').groupby(['keyword','Day'], as_index=False).last()
                    chart_data['Plot Rank'] = chart_data['rank'].apply(lambda x: x if x <= 20 else 21)
                    c = alt.Chart(chart_data).mark_line(point=True).encode(
                        x=alt.X('Day:T', axis=alt.Axis(format='%b %d')),
                        y=alt.Y('Plot Rank:Q', scale=alt.Scale(domain=[21,1], reverse=True)),
                        color='keyword:N', tooltip=['Day','keyword','rank']
                    ).interactive()
                    st.altair_chart(c, use_container_width=True)
            else: st.info("Select a keyword.")

# ─────────────────────────────────────────────
# TAB 3 — COMPETITORS
# ─────────────────────────────────────────────
with tab3:
    st.title("🏆 Competitor Analysis")
    if history_df.empty or master_df.empty: st.info("No data.")
    else:
        history_clean  = history_df.drop(columns=['exam','type'], errors='ignore')
        merged_history = pd.merge(history_clean, master_df[['keyword','exam','type']], on='keyword', how='inner')
        f_c1, f_c2 = st.columns(2)
        avail_exams = sorted(merged_history['exam'].unique())
        avail_types = sorted(merged_history['type'].unique())
        sel_comp_exam = f_c1.multiselect("Filter by Exam:", avail_exams, placeholder="All Exams")
        sel_comp_type = f_c2.selectbox("Filter by Type:", ["All"] + avail_types)
        if sel_comp_exam: merged_history = merged_history[merged_history['exam'].isin(sel_comp_exam)]
        if sel_comp_type != "All": merged_history = merged_history[merged_history['type'] == sel_comp_type]
        if merged_history.empty: st.warning("No data.")
        else:
            merged_history['date_dt'] = pd.to_datetime(merged_history['date'])
            latest_comp = merged_history.sort_values('date_dt').groupby('keyword').tail(1).copy()
            st.subheader("1. Head-to-Head Comparison")
            comp_cols = ["keyword","rank"] + [f"rank_{c}" for c in COMPETITORS_LIST] + [f"url_{c}" for c in COMPETITORS_LIST]
            disp_comp = latest_comp[comp_cols].copy()
            for c in COMPETITORS_LIST:
                r_col = f"rank_{c}"; u_col = f"url_{c}"
                def make_link(row, r_col=r_col, u_col=u_col):
                    try: r_val = int(row.get(r_col))
                    except: r_val = 101
                    if r_val <= 20 and row.get(u_col): return f"{row.get(u_col)}?rank_display={r_val}"
                    return "Not in Top 20"
                disp_comp[c.title()] = disp_comp.apply(make_link, axis=1)
            disp_comp = disp_comp.rename(columns={"rank": "EduTap"})
            disp_comp['EduTap'] = disp_comp['EduTap'].apply(lambda x: "Not in Top 20" if x > 20 else x)
            st.dataframe(
                disp_comp[["keyword","EduTap"] + [c.title() for c in COMPETITORS_LIST]],
                use_container_width=True, hide_index=True,
                column_config={c.title(): st.column_config.LinkColumn(display_text=r"rank_display=(\d+)") for c in COMPETITORS_LIST}
            )
            st.subheader("2. Consistent Outrankers (Last 4 Updates)")
            counts    = merged_history['keyword'].value_counts()
            valid_kws = counts[counts >= 4].index
            if len(valid_kws) == 0: st.info("Need 4 updates.")
            else:
                outrank_data = []
                valid_hist   = merged_history[merged_history['keyword'].isin(valid_kws)].sort_values('date_dt')
                for k, grp in valid_hist.groupby('keyword'):
                    last_4 = grp.tail(4)
                    for comp in COMPETITORS_LIST:
                        col = f"rank_{comp}"; wins = 0
                        cur_edu = last_4.iloc[-1]['rank']
                        try: cur_comp = int(last_4.iloc[-1][col])
                        except: cur_comp = 101
                        for _, r in last_4.iterrows():
                            e = r['rank']; c_r = 101
                            try: c_r = int(r[col])
                            except: pass
                            if c_r <= 20 and c_r < e: wins += 1
                        if wins == 4:
                            outrank_data.append({"Keyword": k, "Competitor": comp.title(),
                                                  "EduTap Rank": cur_edu if cur_edu<=20 else "20+", "Comp Rank": cur_comp})
                if outrank_data: st.dataframe(pd.DataFrame(outrank_data), use_container_width=True)
                else: st.info("No consistent outrankers.")

# ─────────────────────────────────────────────
# TAB 4 — P1/P2 ANALYSIS
# ─────────────────────────────────────────────
with tab4:
    st.title("🧩 P1 vs P2 Cluster Analysis")
    if master_df.empty: st.info("No data.")
    else:
        all_exams = sorted(master_df['exam'].unique())
        sel_e = st.multiselect("Select Exam(s):", all_exams, default=all_exams[:1] if all_exams else None)
        if sel_e:
            latest_p  = history_df.sort_values(by=['date'], key=pd.to_datetime).groupby('keyword').tail(1)
            merged_p  = pd.merge(master_df, latest_p[['keyword','rank']], on='keyword', how='left')
            merged_p['rank']     = merged_p['rank'].fillna(101)
            merged_p['Category'] = merged_p.apply(categorize_cluster, axis=1)
            clean_p   = merged_p[(merged_p['Category'] != "Others") & (merged_p['exam'].isin(sel_e))]
            if clean_p.empty: st.warning("No P1/P2 data.")
            else:
                stats = clean_p.groupby(['exam','Category']).agg(
                    Total_Keywords=('keyword','count'),
                    Avg_Rank=('rank', lambda x: x[x<=20].mean()),
                    Top10=('rank', lambda x: (x<=10).sum())
                ).reset_index()
                stats['Avg_Rank'] = stats['Avg_Rank'].fillna(0).round(1)
                for ex in stats['exam'].unique():
                    st.markdown(f"#### {ex}")
                    d = stats[stats['exam'] == ex]
                    st.table(d.pivot(index='Category', columns=[], values=['Total_Keywords','Avg_Rank','Top10']))
                    c = alt.Chart(d).mark_bar().encode(
                        x='Category', y='Top10', color='Category',
                        tooltip=['Total_Keywords','Top10','Avg_Rank']
                    ).properties(height=200)
                    st.altair_chart(c, use_container_width=True)
        else: st.info("Select exam.")

# ─────────────────────────────────────────────
# TAB 5 — MANAGE DB
# ─────────────────────────────────────────────
with tab5:
    st.title("📝 Manage Database (Cloud)")
    c1, c2 = st.columns(2)
    if not master_df.empty and 'exam' in master_df.columns:
        ex_count = master_df['exam'].nunique(); kw_count = len(master_df)
        existing_exams    = sorted(master_df['exam'].unique().tolist())
        existing_clusters = sorted(master_df['cluster'].fillna("").astype(str).str.strip().unique().tolist())
    else:
        ex_count = kw_count = 0; existing_exams = []; existing_clusters = []
    c1.metric("Exams", ex_count); c2.metric("Keywords", kw_count)

    st.divider()
    if not master_df.empty:
        editable_df = master_df.copy(); editable_df.insert(0, "Select", False)
        edited_df   = st.data_editor(
            editable_df, hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)},
            disabled=["exam","keyword","type","cluster","volume","target_url"],
            use_container_width=True
        )
        rows_to_delete = edited_df[edited_df["Select"] == True]
        if not rows_to_delete.empty and st.button(f"🗑️ Delete Selected ({len(rows_to_delete)})", type="primary"):
            delete_bulk_keywords(rows_to_delete['keyword'].tolist())
            get_master_data.clear(); st.success("Deleted!"); st.rerun()

    st.divider()
    tb, tm = st.tabs(["📂 Bulk Upload", "➕ Add Manual Keyword"])

    with tb:
        f = st.file_uploader("Excel", type=["xlsx"])
        m = st.radio("Mode:", ["Append", "Replace Exam", "⚠️ REPLACE ALL"], horizontal=True)
        wipe = []
        if "Replace Exam" in m: wipe = st.multiselect("Select Exam to Replace:", existing_exams)
        if f and st.button("Process Upload"):
            if "Append" in m: s, t = process_bulk_upload(f, "append")
            elif "Replace Exam" in m:
                if wipe:
                    for e in wipe:
                        try: supabase.table("keywords_master").delete().eq("exam", e).execute()
                        except: pass
                s, t = process_bulk_upload(f, "append")
            else: s, t = process_bulk_upload(f, "replace_all")
            if s: st.success(t); get_master_data.clear(); st.rerun()
            else: st.error(t)

    with tm:
        st.markdown("### Add Single Keyword")
        col_e1, col_e2 = st.columns(2)
        e_sel   = col_e1.selectbox("Select Exam",    ["-- Select --", "➕ Add New Exam"] + existing_exams)
        e_final = col_e1.text_input("Or Enter New Exam Name", disabled=(e_sel != "➕ Add New Exam")) if e_sel == "➕ Add New Exam" else e_sel
        cl_sel  = col_e2.selectbox("Select Cluster", ["-- Select --", "➕ Add New Cluster"] + [c for c in existing_clusters if c])
        cl_final = col_e2.text_input("Or Enter New Cluster Name", disabled=(cl_sel != "➕ Add New Cluster")) if cl_sel == "➕ Add New Cluster" else cl_sel
        col_k1, col_k2, col_k3, col_k4 = st.columns(4)
        k = col_k1.text_input("Keyword"); t = col_k2.selectbox("Type", ["Primary","Secondary"])
        v = col_k3.number_input("Volume", min_value=0, step=10); u = col_k4.text_input("Target URL")
        if st.button("✅ Save Keyword", type="primary"):
            if e_final == "-- Select --" or not e_final: st.error("❌ Please provide an Exam.")
            elif not k: st.error("❌ Please provide a Keyword.")
            elif cl_final == "-- Select --" or not cl_final: st.error("❌ Please provide a Cluster.")
            else:
                success, msg = add_keyword(e_final, k, t, cl_final, v, u)
                if success: st.success(f"✅ Successfully added '{k}'!"); get_master_data.clear(); st.rerun()
                else: st.error(f"❌ {msg}")

# ─────────────────────────────────────────────
# TAB 6 — RUN LOGS (Password Protected)
# ─────────────────────────────────────────────
with tab6:
    st.title("📋 Run Logs")
    st.caption("Every keyword result and error from each DataForSEO run — in plain English.")

    # ── Separate password lock for this tab ──────────────────────────────
    if not st.session_state['logs_unlocked']:
        st.warning("🔒 This section is separately protected.")
        with st.form("logs_unlock_form"):
            logs_pass = st.text_input("Enter Logs Password:", type="password")
            unlock_btn = st.form_submit_button("🔓 Unlock")
            if unlock_btn:
                if logs_pass == st.secrets["LOGS_PASSWORD"]:
                    st.session_state['logs_unlocked'] = True
                    st.rerun()
                else:
                    st.error("❌ Incorrect password.")
        st.stop()

    # ── Unlocked ─────────────────────────────────────────────────────────
    st.success("✅ Access granted.")

    # Fetch list of recent run_ids
    run_ids_info = fetch_run_ids(last_n=10)

    if not run_ids_info:
        st.info("No run logs found yet. Logs will appear here after the first update run.")
    else:
        # Build a readable label for each run
        run_options = {}
        for r in run_ids_info:
            rid  = r['run_id']
            rtyp = r.get('run_type', 'manual').capitalize()
            label = f"{rid} IST  [{rtyp}]"
            run_options[label] = rid

        selected_label = st.selectbox(
            "📅 Select a Run to inspect:",
            list(run_options.keys())
        )
        selected_run_id = run_options[selected_label]

        # Load logs for this run
        with st.spinner("Loading log entries..."):
            logs_df = fetch_logs_for_run(selected_run_id)

        if logs_df.empty:
            st.warning("No log entries found for this run.")
        else:
            # ── Summary metrics ───────────────────────────────────────────
            total_kws   = len(logs_df[logs_df['level'].isin(['success','info']) & (logs_df['keyword'] != "")])
            ranked       = len(logs_df[logs_df['level'] == 'success'])
            not_ranked   = len(logs_df[(logs_df['level'] == 'info') & (logs_df['keyword'] != "")])
            warnings_ct  = len(logs_df[logs_df['level'] == 'warning'])
            errors_ct    = len(logs_df[logs_df['level'] == 'error'])

            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Total Processed", total_kws)
            m2.metric("✅ Ranked (Top 20)", ranked)
            m3.metric("⬜ Not in Top 20", not_ranked)
            m4.metric("⚠️ Warnings", warnings_ct)
            m5.metric("❌ Errors", errors_ct)

            st.divider()

            # ── Filters ───────────────────────────────────────────────────
            fc1, fc2, fc3 = st.columns(3)
            level_filter = fc1.multiselect(
                "Filter by Level:",
                ["success", "info", "warning", "error"],
                default=["success", "warning", "error"],
                format_func=lambda x: {"success":"✅ Ranked","info":"⬜ Not Ranked","warning":"⚠️ Warning","error":"❌ Error"}[x]
            )
            exam_opts = ["All"] + sorted(logs_df[logs_df['exam'] != ""]["exam"].unique().tolist())
            exam_filter = fc2.selectbox("Filter by Exam:", exam_opts)
            search_kw   = fc3.text_input("🔍 Search keyword:")

            filtered = logs_df[logs_df['level'].isin(level_filter)]
            if exam_filter != "All": filtered = filtered[filtered['exam'] == exam_filter]
            if search_kw: filtered = filtered[filtered['keyword'].str.contains(search_kw, case=False, na=False)]

            st.caption(f"Showing {len(filtered)} log entries")
            st.divider()

            # ── Log entries rendered as readable cards ────────────────────
            LEVEL_CONFIG = {
                "success": ("✅", "#d4edda", "#155724"),
                "info":    ("⬜", "#f0f0f0", "#333333"),
                "warning": ("⚠️", "#fff3cd", "#856404"),
                "error":   ("❌", "#f8d7da", "#721c24"),
            }

            # Group by exam for better readability
            if not filtered.empty:
                # Show system messages (no keyword) first
                sys_rows = filtered[filtered['keyword'] == ""]
                kw_rows  = filtered[filtered['keyword'] != ""]

                if not sys_rows.empty:
                    st.markdown("#### 🤖 Run System Messages")
                    for _, row in sys_rows.iterrows():
                        icon, bg, fg = LEVEL_CONFIG.get(row['level'], ("•","#fff","#000"))
                        st.markdown(
                            f"<div style='background:{bg};color:{fg};padding:8px 12px;"
                            f"border-radius:6px;margin-bottom:4px;font-size:13px;'>"
                            f"{icon} <b>{row.get('logged_at','')}</b> — {row['message']}</div>",
                            unsafe_allow_html=True
                        )
                    st.divider()

                if not kw_rows.empty:
                    # Group keyword rows by exam
                    exam_groups = kw_rows['exam'].unique()
                    for exam_name in sorted(exam_groups):
                        exam_rows = kw_rows[kw_rows['exam'] == exam_name]
                        with st.expander(
                            f"📚 {exam_name}  —  {len(exam_rows)} entries  "
                            f"| ✅ {len(exam_rows[exam_rows['level']=='success'])} ranked  "
                            f"| ❌ {len(exam_rows[exam_rows['level']=='error'])} errors",
                            expanded=(exam_name == exam_groups[0])
                        ):
                            for _, row in exam_rows.iterrows():
                                icon, bg, fg = LEVEL_CONFIG.get(row['level'], ("•","#fff","#000"))
                                kw_part    = f"<b>{row['keyword']}</b>" if row['keyword'] else ""
                                type_badge = f"<span style='background:#6c757d;color:white;padding:1px 6px;border-radius:4px;font-size:11px;'>{row.get('kw_type','')}</span>" if row.get('kw_type') else ""
                                time_part  = f"<span style='color:#888;font-size:11px;'>{row.get('logged_at','')[:19]}</span>"
                                msg_part   = row['message']

                                st.markdown(
                                    f"<div style='background:{bg};color:{fg};padding:6px 12px;"
                                    f"border-radius:6px;margin-bottom:3px;font-size:13px;'>"
                                    f"{icon} {time_part}  {type_badge}  {kw_part}<br>"
                                    f"<span style='margin-left:20px;'>{msg_part}</span></div>",
                                    unsafe_allow_html=True
                                )

            st.divider()

            # ── Raw table download ────────────────────────────────────────
            with st.expander("📥 Download full log as table"):
                dl_df = filtered[['logged_at','level','exam','kw_type','keyword','rank','ranked_url','message']].copy()
                dl_df.columns = ['Time','Level','Exam','Type','Keyword','Rank','Ranked URL','Message']
                st.dataframe(dl_df, use_container_width=True, hide_index=True)
                csv = dl_df.to_csv(index=False).encode('utf-8')
                st.download_button("⬇️ Download CSV", csv,
                                   file_name=f"run_log_{selected_run_id[:10]}.csv",
                                   mime="text/csv")
