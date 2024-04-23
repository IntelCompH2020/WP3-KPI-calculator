import pymongo
import pandas as pd

def update_in_batches(collection, doi_list, batch_size=100):
    """
    Update the documents in MongoDB in batches.
    """
    # Calculate the total number of batches
    total_batches = (len(doi_list) + batch_size - 1) // batch_size
    
    for i in range(total_batches):
        # Calculate start and end indices for the current batch
        start_index = i * batch_size
        end_index = start_index + batch_size
        batch_dois = doi_list[start_index:end_index]
        
        # Step 1: Update documents where DOI matches in the current batch
        collection.update_many({'doi': {'$in': batch_dois}}, {'$set': {'patent_cited': True}})
        
        print(f"Batch {i+1}/{total_batches} processed.")
        
    # Step 2: Update documents where DOI does not match any in the list
    # This sets 'patent_cited = false' for all documents not containing a DOI from the list
    # Note: This step is not batched and may be slow for very large collections
    collection.update_many({'doi': {'$nin': doi_list}}, {'$set': {'patent_cited': False}})

def main():
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://adminuser:password123@gateway.opix.ai:27017/")
    db = client["STI_viewer_data"]
    collection = db['scientific-2000-2023']
    
    # Read CSV
    csv_path = '/home/gkou/dev/aggregator/doi-patents.csv'
    df = pd.read_csv(csv_path)
    
    # Convert DOI column to a list
    doi_list = df['doi'].tolist()
    
    # Update documents in batches
    update_in_batches(collection, doi_list, batch_size=100)

if __name__ == "__main__":
    main()
