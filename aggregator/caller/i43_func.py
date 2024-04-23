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
    results["i43a"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Loop through the rows and count the unique Metric_Titles for each Metric_Scope
    # Initialize an empty DataFrame to store all results
    all_sv17_df_categorical = pd.DataFrame()
    all_sv18_df_categorical = pd.DataFrame()
    all_sv17_df_numeric = pd.DataFrame()
    all_sv18_df_numeric = pd.DataFrame()
    for i in range(len(df)):
        df_ESG = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]

        company_name = df_ESG['Company_Name'].iloc[0]  # Get the first company name

        df_ESG_numeric = df_ESG[df_ESG['Unit'].notnull()]
        
        df_sv17 = df_ESG_numeric.groupby(['Metric_Scope', 'Year'])['Weighted_Rank'].sum().reset_index()
        df_sv18 = df_ESG_numeric.groupby(['Metric_Title', 'Year'])['Weighted_Rank'].sum().reset_index()

        df_sv17['Company_Name'] = [company_name] * len(df_sv17)
        df_sv18['Company_Name'] = [company_name] * len(df_sv18)

        all_sv17_df_numeric = pd.concat([all_sv17_df_numeric, df_sv17])
        all_sv18_df_numeric = pd.concat([all_sv18_df_numeric, df_sv18])

        df_ESG_categorical = df_ESG[df_ESG['Unit'].isnull()]
        
        df_sv17 = df_ESG_categorical.groupby(['Metric_Scope', 'Year'])['Weighted_Rank'].sum().reset_index()
        df_sv18 = df_ESG_categorical.groupby(['Metric_Title', 'Year'])['Weighted_Rank'].sum().reset_index()

        df_sv17['Company_Name'] = [company_name] * len(df_sv17)
        df_sv18['Company_Name'] = [company_name] * len(df_sv18)

        all_sv17_df_categorical = pd.concat([all_sv17_df_categorical, df_sv17])
        all_sv18_df_categorical = pd.concat([all_sv18_df_categorical, df_sv18])

    # Add the dictionary of results to the main results dictionary
    results["i43"]["sv17.01"] = all_sv17_df_numeric.pivot_table(index = "Year", columns = "Metric_Scope", values = "Weighted_Rank", aggfunc="mean").fillna(0).to_dict()
    results["i43"]["sv18.01"] = all_sv18_df_numeric.pivot_table(index = "Year", columns = "Metric_Title", values = "Weighted_Rank", aggfunc="mean").fillna(0).to_dict()
    results["i43a"]["sv17.01"] = all_sv17_df_categorical.pivot_table(index = "Year", columns = "Metric_Scope", values = "Weighted_Rank", aggfunc="mean").fillna(0).to_dict()
    results["i43a"]["sv18.01"] = all_sv18_df_categorical.pivot_table(index = "Year", columns = "Metric_Title", values = "Weighted_Rank", aggfunc="mean").fillna(0).to_dict()


    print(results["i43"]["sv17.01"])
    print(results["i43a"]["sv17.01"])
    return results
