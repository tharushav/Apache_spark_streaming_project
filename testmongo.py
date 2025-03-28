from pymongo import MongoClient

client = MongoClient("MONGODB_URI")
db = client["census"]
print(db.list_collection_names())