from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY

print("🚀 Starting One-Time Cleanup...")

if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # This deletes all rows in the 'rankings' table
        data = supabase.table("rankings").delete().gt("id", 0).execute()
        
        print("✅ SUCCESS: All ranking history has been erased.")
        print("   You are now ready to start fresh.")
    except Exception as e:
        print(f"❌ Error during deletion: {e}")
else:
    print("❌ Error: Credentials not found.")
