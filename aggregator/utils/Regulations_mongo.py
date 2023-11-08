import json
import pymongo
import os


def main():
    # Connect to MongoDB
    myclient = pymongo.MongoClient(
        "mongodb://adminuser:password123@gateway.opix.ai:27017/"
    )
    
    # Define the database
    STI_viewer_data = myclient["STI_viewer_data"]  # change this to your database name

    collection = STI_viewer_data["companies"]

    

    # Insert documents into collection
    insert_result = collection.insert_many(documents)

    # Output the ids of the inserted documents
    print("Inserted documents with ids:", insert_result.inserted_ids)
