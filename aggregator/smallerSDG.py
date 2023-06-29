import pymongo

# # Connect to MongoDB
# myclient = pymongo.MongoClient(
#         "mongodb://adminuser:password123@gateway.opix.ai:27017/"
#     )


# STI_viewer_data = myclient["STI_viewer_data"]

# # Get the list of DOIs from the second collection
# doi_list = STI_viewer_data.scientific.distinct('doi')

# # Filter the first collection based on the list
# first_collection_filtered = STI_viewer_data.SDGs.find({'doi': {'$in': doi_list}})

# # If you want to replace the first collection with the filtered results,
# # you would need to create a new collection:
# STI_viewer_data.SDGs_scientific.insert_many(first_collection_filtered)

# And if you want to remove the original collection:
# STI_viewer_data.first_collection.drop()


# Connect to MongoDB
myclient = pymongo.MongoClient("mongodb://adminuser:password123@gateway.opix.ai:27017/")


STI_viewer_data = myclient["STI_viewer_data"]

# # Get the list of DOIs from the second collection
# doi_list = STI_viewer_data.companies.distinct('Publications.DOI')

# # Filter the first collection based on the list.
# # Here, 'publications' is assumed to be the array containing the DOIs.
# first_collection_filtered = STI_viewer_data.SDGs.find({'doi': {'$in': doi_list}})

# # If you want to replace the first collection with the filtered results,
# # you would need to create a new collection:
# STI_viewer_data.SDGs_companies.insert_many(first_collection_filtered)

# # And if you want to remove the original collection:
# STI_viewer_data.first_collection.drop()

STI_viewer_data.agrofood_scientific_data.aggregate(
    [
        {"$match": {"category": "Agrifood"}},
        {
            "$lookup": {
                "from": "SDGs",
                "localField": "doi",
                "foreignField": "doi",
                "as": "sdg",
            }
        },
        {"$unwind": {"path": "$sdg", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"sdg": "$sdg.SDGs"}},
        {"$merge": "scientific"},
    ]
)
