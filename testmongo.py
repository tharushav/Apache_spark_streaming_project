from pymongo import MongoClient

client = MongoClient("mongodb+srv://tharushavihanga2003:NiP4a8ccTJyPGSQb@cluster0.3d8js.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["census"]
print(db.list_collection_names())