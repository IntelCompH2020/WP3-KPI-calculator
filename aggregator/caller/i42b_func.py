import pandas as pd

# Define MongoDB template for filtering documents
template = [
    {
        "$match": {
            "ESG data": {"$exists": True},
            "ESG data.Sub_Metric_Title": {"$exists": True},
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
    results["i42b"] = {}

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
        df_ESG = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]
        title_counts = (
            df_ESG.groupby("Metric_Scope")["Sub_Metric_Title"].nunique().to_dict()
        )
        companies_dict[company] = title_counts

    # Add the dictionary of results to the main results dictionary
    results["i42b"]["sv17"] = companies_dict

    return results
