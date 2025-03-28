from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")

def reset_mongodb():
    """
    Reset MongoDB collections by dropping them to start a fresh streaming session.
    """
    print("Connecting to MongoDB...")
    client = MongoClient(MONGODB_URI)
    db = client["census"]
    
    # List of collections to reset
    collections = [
        "summary_statistics",
        "anomalies",
        "age_group_distribution", 
        "education_income",
        "gender_income",
        "work_hours",
        "occupation_stats",
        "raw_data"
    ]
    
    print("Resetting MongoDB collections...")
    for collection_name in collections:
        try:
            db[collection_name].delete_many({})
            print(f"✓ Cleared collection: {collection_name}")
        except Exception as e:
            print(f"× Error clearing {collection_name}: {e}")
    
    print("\nDatabase reset complete. You can now start streaming data from scratch.")

if __name__ == "__main__":
    reset_mongodb()
