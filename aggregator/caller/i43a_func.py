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
    # Initialize an empty DataFrame to store all results
    all_sv17_df = pd.DataFrame()
    all_sv18_df = pd.DataFrame()
    for i in range(len(df)):
        df_ESG = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]

        company_name = df_ESG['Company_Name'].iloc[0]  # Get the first company name

        df_sv17 = df_ESG.groupby(['Metric_Scope', 'Year'])['Weighted_Rank'].sum().reset_index()
        df_sv18 = df_ESG.groupby(['Metric_Title', 'Year'])['Weighted_Rank'].sum().reset_index()

        df_sv17['Company_Name'] = [company_name] * len(df_sv17)
        df_sv18['Company_Name'] = [company_name] * len(df_sv18)

        all_sv17_df = pd.concat([all_sv17_df, df_sv17])
        all_sv18_df = pd.concat([all_sv18_df, df_sv18])

    # Add the dictionary of results to the main results dictionary
    results["i43"]["sv17.01"] = all_sv17_df.pivot_table(index = "Year", columns = "Metric_Scope", values = "Weighted_Rank", aggfunc="mean").fillna(0).to_dict()
    results["i43"]["sv18.01"] = all_sv18_df.pivot_table(index = "Year", columns = "Metric_Title", values = "Weighted_Rank", aggfunc="mean").fillna(0).to_dict()

    print(results["i43"]["sv17.01"])
    return results
