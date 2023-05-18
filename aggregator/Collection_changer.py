from pymongo import MongoClient
from tqdm import tqdm


def main():
    # Connect to MongoDB
    myclient = MongoClient("mongodb://adminuser:password123@gateway.opix.ai:27017/")
    db = myclient["STI_viewer_data"]
    from_collection = db["agrofood_companies_dataset_unique"]

    # Iterate over each document in the collection
    for document in tqdm(
        from_collection.find(), total=from_collection.count_documents({})
    ):
        # Check if the field "company name" exists in the document
        if "company name" in document:
            # Get the value of the "company name" field
            company_name = document["company name"]
            # Remove the "company name" field from the document
            del document["company name"]
            # Add the "company_name" field with the same value
            document["company_name"] = company_name
            # Update the document in the collection
            from_collection.update_one({"_id": document["_id"]}, {"$set": document})

    # Print the total number of documents in the collection after the update
    print(from_collection.estimated_document_count())


if __name__ == "__main__":
    main()
