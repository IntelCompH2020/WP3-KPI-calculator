import json

# # Read data from ESG.json
# with open('ESG.json', 'r') as file:
#     data = json.load(file)

# # Update the Company_ID
# for company_data in data["data"]:
#     try:
#         company_data["Company_ID"] = str(int(company_data["Company_ID"])) + "."
#     except:
#         continue

# # Optionally: Write the updated data back to a JSON file
# with open('Updated_ESG.json', 'w') as file:
#     json.dump(data, file, indent=4)



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

    # # Update documents
    # collection.update_many(
    #     {"ESG data": ""},  # filter: documents where "ESG data" is an empty string
    #     {"$set": {"ESG data": []}}  # update: set "ESG data" to an empty array
    # )
    # collection.update_many(
    #     {},  # Filter: match all documents
    #     {"$set": {"ESG data": []}}  # Update: set "ESG data" to an empty array
    # )

    with open('Updated_ESG.json', 'r') as file:
        json_data = json.load(file)

    # Iterate through each record in your JSON data
    for record in json_data["data"]:
        company_id = record["Company_ID"]
        
        # Find a company in MongoDB with the same Company_ID
        company = collection.find_one({"ID": company_id})
        
        if company:
            # If the company exists, update it with the JSON data
            collection.update_one(
                {"ID": company_id},
                {"$push": {"ESG data": record}}
            )


if __name__ == "__main__":
    main()