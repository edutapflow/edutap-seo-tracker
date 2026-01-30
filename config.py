import os
import streamlit as st

def get_secret(key):
    try:
        return st.secrets[key]
    except (FileNotFoundError, KeyError, AttributeError):
        return os.environ.get(key)

# API Keys
API_LOGIN = get_secret("API_LOGIN")
API_PASSWORD = get_secret("API_PASSWORD")
SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")

# Email Config
EMAIL_SENDER = get_secret("EMAIL_SENDER")    # Your email (rohit...)
EMAIL_PASSWORD = get_secret("EMAIL_PASSWORD") # The 16-char App Password
EMAIL_RECEIVER = "rohit.sharma@edutap.co.in"  # Who gets the alert? (You)

DB_NAME = "edutap_rankings.db"
