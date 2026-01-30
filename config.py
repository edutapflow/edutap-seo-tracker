import streamlit as st

# Read secrets from Streamlit's secure vault
API_LOGIN = st.secrets["API_LOGIN"]
API_PASSWORD = st.secrets["API_PASSWORD"]
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

DB_NAME = "edutap_rankings.db"
