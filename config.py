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
API_LOGIN = get_secret("API_LOGIN")
API_PASSWORD = get_secret("API_PASSWORD")
SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")

# Email Config
EMAIL_SENDER = get_secret("EMAIL_SENDER")
EMAIL_PASSWORD = get_secret("EMAIL_PASSWORD")
EMAIL_RECEIVER = "rohit.sharma@edutap.co.in" 

# Database Name
DB_NAME = "edutap_rankings.db"





