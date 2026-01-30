import pandas as pd
from backend_utils import perform_update, fetch_all_rows, supabase

def run_automation():
    print("🤖 Robot: Starting scheduled update...")
    
    # 1. Get All Keywords from Cloud
    print("   ... Fetching keywords")
    df = fetch_all_rows("keywords_master")
    
    if df.empty:
        print("   ⚠️ No keywords found. Exiting.")
        return

    # 2. Convert to list format
    keywords_list = df.to_dict('records')
    print(f"   ... Found {len(keywords_list)} keywords.")

    # 3. Run the Update (Headless)
    # We pass None for progress_bar/status_text since there is no UI
    date_str, cost = perform_update(keywords_list, progress_bar=None, status_text=None)
    
    print(f"✅ Robot: Update Complete! Date: {date_str} | Cost: ${cost:.4f}")

if __name__ == "__main__":
    run_automation()
