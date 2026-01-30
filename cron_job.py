import pandas as pd
from backend_utils import perform_update, fetch_all_rows, send_email_alert

def run_automation():
    print("🤖 Robot: Starting scheduled update...")
    
    # 1. Fetch Master List
    print("   ... Fetching keywords")
    master_df = fetch_all_rows("keywords_master")
    if master_df.empty:
        print("   ⚠️ No keywords found. Exiting.")
        return
    
    # 2. Fetch History (To find Previous Ranks)
    print("   ... Fetching previous rankings")
    history_df = fetch_all_rows("rankings")
    
    prev_map = {}
    if not history_df.empty:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        history_df = history_df.sort_values('date_dt')
        latest_snapshot = history_df.groupby('keyword').tail(1)
        for _, row in latest_snapshot.iterrows():
            prev_map[row['keyword']] = row['rank']

    # 3. Run the Update
    keywords_list = master_df.to_dict('records')
    date_str, cost, results_data = perform_update(keywords_list, progress_bar=None, status_text=None)
    
    # 4. Compare using the returned data directly (No need to re-fetch)
    alerts = {"red": [], "orange": [], "yellow": [], "green": []}

    for row in results_data:
        kw = row['keyword']
        curr_rank = row['rank']
        prev_rank = prev_map.get(kw, 101) 

        if curr_rank > 100 and prev_rank > 100: continue

        if prev_rank <= 10 and curr_rank > 10:
            alerts["red"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})
        elif (curr_rank - prev_rank) >= 4:
            alerts["orange"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})
        elif prev_rank <= 3 and curr_rank > 3:
            alerts["yellow"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})
        elif prev_rank > 3 and curr_rank <= 3:
            alerts["green"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})

    # 5. Send Email (With "Weekly" Label)
    print("   ... Sending Email Alert")
    send_email_alert(alerts, subject_prefix="📅 Weekly Automatic Run")
    
    print(f"✅ Robot: Update Complete & Email Sent! Cost: ${cost:.4f}")

if __name__ == "__main__":
    run_automation()
