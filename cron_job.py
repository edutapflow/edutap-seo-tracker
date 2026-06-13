# FORCE UPDATE V7 - FIXED PREV/CURR SWAP BUG
import pandas as pd
from backend_utils import perform_update, fetch_all_rows, send_email_alert, build_prev_map_safe

def run_automation():
    print("🤖 Robot: Starting scheduled update...")

    # 1. Fetch Master List
    print("   ... Fetching keywords")
    master_df = fetch_all_rows("keywords_master")
    if master_df.empty:
        print("   ⚠️ No keywords found. Exiting.")
        return

    # 2. ✅ FIX: Build prev_map BEFORE the run using safe timestamp-filtered fetch
    # Old code used fetch_all_rows("rankings") then .tail(1) per keyword.
    # Problem: if perform_update() writes new rows to Supabase before comparison
    # is done (race condition in threading), the "latest" row becomes the NEW row,
    # making prev_rank == curr_rank or swapping them.
    # build_prev_map_safe() filters rows to only those created >2 minutes ago,
    # so the new run's rows can never pollute the previous rank lookup.
    print("   ... Building previous rank snapshot (safe filtered fetch)")
    prev_map = build_prev_map_safe()
    print(f"   ... Got previous ranks for {len(prev_map)} keywords")

    # 3. Run the Update
    keywords_list = master_df.to_dict('records')
    date_str, cost, results_data = perform_update(keywords_list, progress_bar=None, status_text=None)

    # 4. Compare new results against the safely-fetched previous snapshot
    alerts = {"red": [], "orange": [], "yellow": [], "green": []}

    for row in results_data:
        kw = row['keyword']
        ex = row['exam']
        typ = row['type']
        curr_rank = row['rank']
        prev_rank = prev_map.get(kw, 101)

        if curr_rank > 100 and prev_rank > 100: continue

        alert_obj = {"kw": kw, "curr": curr_rank, "prev": prev_rank, "exam": ex, "type": typ}

        if prev_rank <= 10 and curr_rank > 10:
            alerts["red"].append(alert_obj)
        elif (curr_rank - prev_rank) >= 4:
            alerts["orange"].append(alert_obj)
        elif prev_rank <= 3 and curr_rank > 3:
            alerts["yellow"].append(alert_obj)
        elif prev_rank > 3 and curr_rank <= 3:
            alerts["green"].append(alert_obj)

    # 5. Send Email
    print("   ... Sending Email Alert")
    send_email_alert(alerts, subject_prefix="📅 Weekly Automatic Run", all_checked_data=None)

    print(f"✅ Robot: Update Complete & Email Sent! Cost: ${cost:.4f}")

if __name__ == "__main__":
    run_automation()
