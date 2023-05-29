import pymongo
import ijson
from tqdm import tqdm
from bson import ObjectId

# Connect to MongoDB
myclient = pymongo.MongoClient("mongodb://adminuser:password123@gateway.opix.ai:27017/")

# Specify the database and collection
mydb = myclient["STI_viewer_data"]
mycol = mydb["SDGs"]

# Open your JSON file
with open("/media/datalake/stiviewer/database/SDGs.json") as f:
    # Parse the JSON file
    objects = ijson.items(f, "item")
    for obj in tqdm(objects, desc="Inserting documents", unit="doc"):
        # Check if "_id" is a dictionary containing "$oid"
        if isinstance(obj.get("_id", {}), dict) and "$oid" in obj["_id"]:
            # Replace the value of "_id" with the value of "$oid"
            obj["_id"] = ObjectId(obj["_id"]["$oid"])
        # Check if the document's "_id" already exists in the database
        if mycol.count_documents({"_id": obj["_id"]}, limit=1) == 0:
            # If it does not exist, insert the document into the collection
            mycol.insert_one(obj)

print("Data inserted successfully.")
