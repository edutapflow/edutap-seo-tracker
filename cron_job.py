# FORCE UPDATE V9 - schedule read from web/schedule.json + runs started from the website (chosen exams)
import os
import json
from datetime import datetime, timedelta, date
import pandas as pd
import backend_utils
from backend_utils import (perform_update, fetch_all_rows, send_weekly_email_report,
                           build_prev_info_safe, get_searchapi_credits)

# ─────────────────────────────────────────────
# 📅 EXAM SCHEDULE — lives in web/schedule.json (the website reads the same file)
# Any exam NOT in those two lists runs every 15 days.
# Every-15-days exams run on fortnight_start's Monday, then every 2nd Monday.
# Once-a-month exams run on the FIRST Monday of each month (date 1 to 7).
# ─────────────────────────────────────────────
_DEFAULT_SCHEDULE = {
    "every_week": ["RBI Grade B", "EPFO", "SEBI Grade A", "NABARD Grade A",
                   "JAIIB", "CAIIB", "IFSCA Grade A", "IRDAI Grade A"],
    "once_a_month": ["FCI AG3", "LEO", "IB ACIO", "ALC", "ESIC DD", "UPSC CSAT"],
    "fortnight_start": "2026-10-12",
}

def _load_schedule():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "web", "schedule.json")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        print(f"   ... Exam schedule read from web/schedule.json")
        return {**_DEFAULT_SCHEDULE, **data}
    except Exception as e:
        print(f"   ⚠️ Could not read web/schedule.json ({e}) — using the built-in schedule")
        return _DEFAULT_SCHEDULE

_SCHEDULE = _load_schedule()
EVERY_WEEK = _SCHEDULE["every_week"]
ONCE_A_MONTH = _SCHEDULE["once_a_month"]
FORTNIGHT_START = datetime.strptime(_SCHEDULE["fortnight_start"], "%Y-%m-%d").date()

# RUN_MODE comes from GitHub (Run workflow button or the website):
#   auto  = follow the schedule above (default, used by cron-job.org every Monday)
#   all   = run every exam now
#   exams = run only the exams written in RUN_EXAMS (comma separated) — used by the website
RUN_MODE = (os.environ.get("RUN_MODE") or "auto").strip().lower()
RUN_EXAMS = [e.strip() for e in (os.environ.get("RUN_EXAMS") or "").split(",") if e.strip()]


def _norm(x):
    return str(x or "").strip().lower()


def pick_groups(today):
    """Which groups run today. today = IST date."""
    if RUN_MODE == "all":
        return True, True, True
    monday = today - timedelta(days=today.weekday())          # Monday of this week
    weeks_since = (monday - FORTNIGHT_START).days // 7
    fortnight_due = (weeks_since % 2 == 0)
    monthly_due = monday.day <= 7      # this week's Monday is the first Monday of its month
    return True, fortnight_due, monthly_due


def run_automation():
    print("🤖 Robot: Starting scheduled update...")

    # 1. Fetch Master List
    print("   ... Fetching keywords")
    master_df = fetch_all_rows("keywords_master")
    if master_df.empty:
        print("   ⚠️ No keywords found. Exiting.")
        return

    # 2. Decide which exams run today
    ist_today = (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()
    weekly_on, fortnight_on, monthly_on = pick_groups(ist_today)
    weekly_set  = {_norm(e) for e in EVERY_WEEK}
    monthly_set = {_norm(e) for e in ONCE_A_MONTH}

    def group_of(exam):
        n = _norm(exam)
        if n in weekly_set:  return "Every week"
        if n in monthly_set: return "Once a month"
        return "Every 15 days"

    master_df["_group"] = master_df["exam"].apply(group_of)
    run_groups = []
    if weekly_on:    run_groups.append("Every week")
    if fortnight_on: run_groups.append("Every 15 days")
    if monthly_on:   run_groups.append("Once a month")

    if RUN_MODE == "exams":
        wanted = {_norm(e) for e in RUN_EXAMS}
        run_df = master_df[master_df["exam"].apply(lambda e: _norm(e) in wanted)].copy()
        print(f"   ... Website run for: {', '.join(RUN_EXAMS) or '(no exams given)'}")
    else:
        run_df = master_df[master_df["_group"].isin(run_groups)].copy()
    exam_groups = {g: sorted(run_df[run_df["_group"] == g]["exam"].astype(str).unique().tolist())
                   for g in ["Every week", "Every 15 days", "Once a month"]}
    print(f"   ... Mode: {RUN_MODE} | Date (IST): {ist_today} | Groups today: {', '.join(run_groups)}")
    for g, ex in exam_groups.items():
        if ex: print(f"       {g}: {', '.join(ex)}")
    print(f"   ... Keywords this run: {len(run_df)} of {len(master_df)}")

    # Warn if an exam in the lists does not exist in the database (spelling check)
    db_exams = {_norm(e) for e in master_df["exam"].unique()}
    for e in EVERY_WEEK + ONCE_A_MONTH:
        if _norm(e) not in db_exams:
            print(f"   ⚠️ Exam '{e}' in the schedule list was not found in keywords_master (check spelling).")

    if run_df.empty:
        print("   ⚠️ No exams due today. Exiting.")
        return

    # 3. Build previous ranks BEFORE the run (safe timestamp-filtered fetch)
    print("   ... Building previous rank snapshot")
    prev_info = build_prev_info_safe()
    prev_map = {k: v[0] for k, v in prev_info.items()}
    print(f"   ... Got previous ranks for {len(prev_map)} keywords")

    # 4. Run the Update — run_type="scheduled" so Run Logs tab shows it correctly
    keywords_list = run_df.drop(columns=["_group"]).to_dict('records')
    date_str, cost, results_data = perform_update(
        keywords_list,
        progress_bar=None,
        status_text=None,
        run_type="manual" if RUN_MODE == "exams" else "scheduled",
        prev_info=prev_info
    )

    # 5. Send Email (only the exams that ran)
    print("   ... Fetching SearchApi searches left")
    credits = get_searchapi_credits()
    print("   ... Sending Email")
    send_weekly_email_report(
        master_df=run_df.drop(columns=["_group"]),
        results_data=results_data,
        run_date=date_str,
        run_cost=cost,
        credits_left=credits,
        exam_groups=exam_groups,
        searches_used=backend_utils.LAST_RUN_SEARCHES,
        subject_prefix="🛠️ Website Run" if RUN_MODE == "exams" else "📅 Automatic Run"
    )

    print(f"✅ Robot: Update Complete & Email Sent! Searches used: {backend_utils.LAST_RUN_SEARCHES} | "
          f"Estimated cost: ${cost:.4f}")


if __name__ == "__main__":
    run_automation()
