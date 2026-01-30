import os

# --- CREDENTIALS ---
# 1. DataForSEO (For fetching ranks)
API_LOGIN = "rohit.sharma@edutap.co.in"
API_PASSWORD = "226859e76e14d926"

# 2. Supabase (For saving data)
SUPABASE_URL = "https://ofpdbgxnmjyarlmnoynj.supabase.co"
# We use the Service Role Key so the app has permission to write/delete data
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9mcGRiZ3hubWp5YXJsbW5veW5qIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2OTY5Njc5OCwiZXhwIjoyMDg1MjcyNzk4fQ.0nKTQy5BQoyrZ0w8hk_o4EpC3gKgpqiA7FR91bWZ_KQ"

# --- DATABASE CONFIG ---
# This is kept for compatibility, though we primarily use Supabase now
DB_NAME = "edutap_rankings.db"