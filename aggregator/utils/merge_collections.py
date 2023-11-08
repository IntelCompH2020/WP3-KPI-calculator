import pymongo
import os


def main():
    # Connect to MongoDB
    myclient = pymongo.MongoClient(
        "mongodb://adminuser:password123@gateway.opix.ai:27017/"
    )
    
    # Define the database
    mydb = myclient["STI_viewer_data"]  # change this to your database name
    
    # Define the collections to merge
    # collections_to_merge = ["agrofood_companies_dataset_unique", "energy_companies_dataset", "rarediseases_companies_dataset"]  # change these to your collection names
    merged_collection_name = "companies"  # change this to your merged collection name
    backup_collection_name = "companies3"  # change this to your backup collection name
    missing_field = "CompanySize"  # change this to your actual missing field name

    # # Initiate a new empty list to hold all documents from all collections
    # merged_documents = []

    # for collection_name in collections_to_merge:
    #     collection = mydb[collection_name]
    #     # Find all documents in the collection and add them to the list
    #     merged_documents.extend(list(collection.find({})))

    # # Create the merged collection and insert all documents
    merged_collection = mydb[merged_collection_name]
    # merged_collection.insert_many(merged_documents)

    # # Get the backup collection
    # backup_collection = mydb[backup_collection_name]

    # # Iterate over each document in the new collection
    # for document in merged_collection.find({}):
    #     # Find a matching document in the backup collection based on the company name
    #     backup_document = backup_collection.find_one({"company_name": document["company_name"]})

    #     # If a matching document is found and it has the missing field
    #     if backup_document and missing_field in backup_document:
    #         # Update the document in the new collection with the missing field
    #         merged_collection.update_one({"_id": document["_id"]}, {"$set": {missing_field: backup_document[missing_field]}})

    # Add 'no category' to documents that don't have the 'category' field
    merged_collection.update_many({"category": {"$exists": False}}, {"$set": {"category": "rarediseases"}})


if __name__ == "__main__":
    main()