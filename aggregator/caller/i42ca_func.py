import pandas as pd

# Define MongoDB template for filtering documents
template = [
    {
        "$match": {
            "ESG data": {"$exists": True},
        }
    },
    {
        "$addFields": {
            "TurnoverNumeric": {"$ifNull": [{"$toDouble": "$Turnover"}, 0]},
            "NumberOfESGdata": {"$size": "$ESG data"},
        }
    },
    {
        "$match": {
            "NumberOfESGdata": {"$ne": 0},
            "Turnover": {"$ne": None},
        }
    },
]


def ind_caller(enco, results):
    # Initialize a new dictionary for the results
    results["i42ca"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["TurnoverNumeric"], ascending=False).reset_index(drop=True)
    df = df.head(10)

    # Loop through the rows and count the unique Metric_Titles for each Metric_Scope
    companies_dict = {}
    for i in range(len(df)):
        company = df["company_name"][i]
        df_ESG_nonvalue = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]
        # Create a pivot table
        numerator = df_ESG_nonvalue.pivot_table(
            index="Sub_Metric_Title", columns="Year", aggfunc="count"
        )["Rank"]
        # Calculate the sum of the columns
        denominator = numerator.sum()
        df_result = 100 * numerator / denominator
        # Calculate the sum of each row
        df_result["sum"] = df_result.sum(axis=1)
        # Sort the DataFrame based on the sum, in descending order
        df_sorted = (
            df_result.sort_values(by="sum", ascending=False)
            .head(n=10)
            .drop(columns=["sum"])
        )
        companies_dict[company] = df_sorted.to_dict()

    # Add the dictionary of results to the main results dictionary
    results["i42ca"]["sv0"] = companies_dict

    return results
