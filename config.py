import os
import streamlit as st

def get_secret(key):
    # 1. Try Streamlit Secrets (For the Dashboard)
    try:
        return st.secrets[key]
    except (FileNotFoundError, KeyError, AttributeError):
        # 2. Fallback to Environment Variables (For GitHub Actions Robot)
        return os.environ.get(key)

# API Keys
SEARCHAPI_KEY = get_secret("SEARCHAPI_KEY")   # SearchApi.io key (used for all rank checks)
API_LOGIN = get_secret("API_LOGIN")           # old DataForSEO login (not used for ranks any more)
API_PASSWORD = get_secret("API_PASSWORD")     # old DataForSEO password (not used for ranks any more)
SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")

# Email Config
EMAIL_SENDER = get_secret("EMAIL_SENDER")
EMAIL_PASSWORD = get_secret("EMAIL_PASSWORD")
# Who gets the emails: set the EMAIL_RECEIVER secret (GitHub) to a comma-separated list,
# e.g.  rohit.sharma@edutap.co.in, deepak@edutap.co.in
# If the secret is empty, only the address below gets the email.
EMAIL_RECEIVER = get_secret("EMAIL_RECEIVER") or "rohit.sharma@edutap.co.in"

# Database Name
DB_NAME = "edutap_rankings.db"
