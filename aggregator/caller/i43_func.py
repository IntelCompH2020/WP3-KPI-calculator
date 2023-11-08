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
            "NumberOfESGdata": {"$size": "$ESG data"},
            "Year": "$ESG data.Year",
        }
    },
    {
        "$match": {
            "NumberOfESGdata": {"$ne": 0},
        }
    },
]


def ind_caller(enco, results, logging, extra_aggr_param=[], working_path=""):
    # Initialize a new dictionary for the results
    results["i43"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Loop through the rows and count the unique Metric_Titles for each Metric_Scope
    companies_dict_sv17 = {}
    companies_dict_sv18 = {}
    for i in range(len(df)):
        company = df["company_name"][i]
        df_ESG = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]
        companies_dict_sv17[company] = (
            df_ESG.pivot_table(
                index="Year",
                columns="Metric_Scope",
                values="Weighted_Rank",
                aggfunc="sum",
            )
            .fillna(0)
            .to_dict()
        )
        companies_dict_sv18[company] = (
            df_ESG.pivot_table(
                index="Year",
                columns="Metric_Title",
                values="Weighted_Rank",
                aggfunc="sum",
            )
            .fillna(0)
            .to_dict()
        )

    # Add the dictionary of results to the main results dictionary
    results["i43"]["sv17.01"] = companies_dict_sv17
    results["i43"]["sv18.01"] = companies_dict_sv18

    return results

    # "sv18.01" per year per company