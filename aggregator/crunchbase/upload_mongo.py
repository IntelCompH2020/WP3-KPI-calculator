import json
import pymongo
import re

def categorize_company_size(num_employees_enum):
    if num_employees_enum is None:
        return "Unknown"
    
    # Extract numeric parts from the string
    numeric_parts = re.findall(r'\d+', num_employees_enum)
    
    if len(numeric_parts) == 2:
        lower_bound, upper_bound = map(int, numeric_parts)
    elif len(numeric_parts) == 1:
        lower_bound = int(numeric_parts[0])
        upper_bound = float('inf')
    else:
        return "Unknown"
    
    # Categorize based on the bounds
    if lower_bound >= 501 or upper_bound >= 501:
        return "Over 500 employees"
    elif 251 <= lower_bound <= 500 or 251 <= upper_bound <= 500:
        return "250 to 500 employees"
    elif 51 <= lower_bound <= 250 or 51 <= upper_bound <= 250:
        return "50 to 250 employees"
    elif 11 <= lower_bound <= 50 or 11 <= upper_bound <= 50:
        return "10 to 50 employees"
    elif 6 <= lower_bound <= 10 or 6 <= upper_bound <= 10:
        return "5 to 10 employees"
    elif lower_bound <= 5 or upper_bound <= 5:
        return "up to 5 employees"
    else:
        return "Unknown"

def main():
    # Connect to MongoDB
    myclient = pymongo.MongoClient("mongodb://adminuser:password123@gateway.opix.ai:27017/")
    
    # Define the database
    mydb = myclient["STI_viewer_data"]  # change this to your database name
    
    # Define the collection
    rare_diseases_collection_name = "rarediseases_companies_dataset"
    rare_diseases_collection = mydb[rare_diseases_collection_name]

    # Assuming your JSON data is in a file called 'data.json'
    with open('/home/gkou/dev/aggregator/aggregator/crunchbase/rare_diseases_crunchbase_icd11.json', 'r') as file:
        data = json.load(file)

    # Extract required fields
    updates = {}
    for company, details in data.items():
        num_employees_enum = details.get("crunchbase", {}).get("num_employees_enum")
        company_size = categorize_company_size(num_employees_enum)

        update_data = {
            "icd11_cancer": [details.get("icd11_cancer", {})],
            "Website address": details.get("crunchbase", {}).get("website"),
            "funding_total": details.get("crunchbase", {}).get("funding_total", {}).get("value"),
            "Number of employees": details.get("crunchbase", {}).get("num_employees_enum"),
            "CompanySize": company_size,
            "status": details.get("crunchbase", {}).get("status")
        }
        updates[company] = update_data
        
    # Update documents in MongoDB
    for company_name, update_data in updates.items():
        query = {"company_name": company_name}
        new_values = {"$set": update_data}
        rare_diseases_collection.update_one(query, new_values)


if __name__ == "__main__":
    main()

