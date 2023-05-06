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
    results["i43"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["TurnoverNumeric"], ascending=False).reset_index(drop=True)
    df = df.head(10)

    # Loop through the rows and count the unique Metric_Titles for each Metric_Scope
    companies_dict_sv17 = {}
    companies_dict_sv18 = {}
    for i in range(len(df)):
        company = df["company_name"][i]
        df_ESG = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]
        companies_dict_sv17[company] = df_ESG.pivot_table(
            index="Metric_Scope", columns="Year", aggfunc="mean"
        )["Weighted_Rank"].to_dict()
        companies_dict_sv18[company] = df_ESG.pivot_table(
            index="Metric_Title", columns="Year", aggfunc="mean"
        )["Weighted_Rank"].to_dict()
        break

    # Add the dictionary of results to the main results dictionary
    results["i43"]["sv17"] = companies_dict_sv17
    results["i43"]["sv18"] = companies_dict_sv18

    return results
