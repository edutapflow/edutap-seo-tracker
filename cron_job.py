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
    
    # Create a dictionary of {keyword: previous_rank}
    prev_map = {}
    if not history_df.empty:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        history_df = history_df.sort_values('date_dt')
        latest_snapshot = history_df.groupby('keyword').tail(1)
        for _, row in latest_snapshot.iterrows():
            prev_map[row['keyword']] = row['rank']

    # 3. Run the Update (Get New Ranks)
    keywords_list = master_df.to_dict('records')
    date_str, cost = perform_update(keywords_list, progress_bar=None, status_text=None)
    
    # 4. Fetch the NEW data we just saved
    print("   ... Verifying new data for alerts")
    new_history_df = fetch_all_rows("rankings")
    new_history_df['date_dt'] = pd.to_datetime(new_history_df['date'])
    latest_run_data = new_history_df.groupby('keyword').tail(1)

    # 5. COMPARE & GENERATE ALERTS
    alerts = {"red": [], "orange": [], "yellow": [], "green": []}

    for _, row in latest_run_data.iterrows():
        kw = row['keyword']
        curr_rank = row['rank']
        prev_rank = prev_map.get(kw, 101) 

        if curr_rank > 100 and prev_rank > 100: continue

        # Rule 1 🔴: Out of Top 10
        if prev_rank <= 10 and curr_rank > 10:
            alerts["red"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})

        # Rule 2 🟠: Lost 4+ Positions
        elif (curr_rank - prev_rank) >= 4:
            alerts["orange"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})

        # Rule 3 🟡: Out of Top 3
        elif prev_rank <= 3 and curr_rank > 3:
            alerts["yellow"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})

        # Rule 4 🟢: Entered Top 3
        elif prev_rank > 3 and curr_rank <= 3:
            alerts["green"].append({"kw": kw, "curr": curr_rank, "prev": prev_rank})

    # 6. Send Email
    print("   ... Sending Email Alert")
    send_email_alert(alerts)
    
    print(f"✅ Robot: Update Complete & Email Sent! Cost: ${cost:.4f}")

if __name__ == "__main__":
    run_automation()
