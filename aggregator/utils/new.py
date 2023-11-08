# import pymongo
# from tqdm import tqdm

# def main():
#     myclient = pymongo.MongoClient(
#         "mongodb://adminuser:password123@gateway.opix.ai:27017/"
#     )
    
#     db = myclient["STI_viewer_data"]
#     companies = db['companies4']
#     SDGs = db['SDGs']

#     # Get the total number of SDGs for the progress bar
#     total_SDGs = SDGs.count_documents({})

#     # Start a session
#     with myclient.start_session() as session:
#         SDGs_cursor = SDGs.find({}, {'_id': 0}, no_cursor_timeout=True, session=session).batch_size(100)

#         for SDG in tqdm(SDGs_cursor, total=total_SDGs, desc="Processing SDGs"):
#             doi = SDG['doi']
#             # Prepare bulk operations
#             bulk_ops = []

#             # Get all the companies that have a publication with this DOI
#             matching_companies_cursor = companies.find({'Publications.DOI': doi}, {'Publications.$': 1}, session=session)
#             for company in matching_companies_cursor:
#                 # Update publication with matching DOI
#                 bulk_ops.append(pymongo.UpdateOne(
#                     {'_id': company['_id'], 'Publications.DOI': doi},
#                     {'$set': {'Publications.$.SDGs': SDG['SDGs']}}
#                 ))

#             # Execute bulk operations for this DOI
#             if bulk_ops:
#                 companies.bulk_write(bulk_ops, session=session)

#     SDGs_cursor.close()

# if __name__ == "__main__":
#     main()


import pymongo
from tqdm import tqdm

def main():
    myclient = pymongo.MongoClient(
        "mongodb://adminuser:password123@gateway.opix.ai:27017/"
    )
    
    db = myclient["STI_viewer_data"]
    companies_collection = db['companies']
    SDGs_collection = db['SDGs']

    # Determine the total number of SDGs for the progress bar
    total_SDGs = SDGs_collection.count_documents({})

    # Load all SDGs into a dictionary for faster lookups using tqdm for progress
    SDGs_dict = {
        SDG['doi']: SDG['SDGs']
        for SDG in tqdm(SDGs_collection.find({}), total=total_SDGs, desc="Loading SDGs")
    }

    # Prepare bulk operations
    bulk_ops = []

    # Get all the companies and their publications
    for company in tqdm(companies_collection.find({}), desc="Processing Companies"):
        for publication in company.get('Publications', []):
            doi = publication.get('DOI')
            if doi in SDGs_dict:
                # If this publication's DOI is in our SDGs dictionary, prepare an update operation
                bulk_ops.append(pymongo.UpdateOne(
                    {'_id': company['_id'], 'Publications.DOI': doi},
                    {'$set': {'Publications.$.SDGs': SDGs_dict[doi]}}
                ))

    # Execute bulk operations
    if bulk_ops:
        companies_collection.bulk_write(bulk_ops)

if __name__ == "__main__":
    main()