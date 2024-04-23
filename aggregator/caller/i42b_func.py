import pandas as pd


# Define MongoDB template for filtering documents
template = [
    {
        "$match": {
            "ESG data": {"$exists": True},
            "ESG data.Sub_Metric_Title": {"$exists": True},
            "ESG data.Metric_Scope": {"$exists": True}
        }
    },
    {
        "$addFields": {
            "NumberOfESGdata": {"$size": "$ESG data"},
            "Sub_Metric_Title": "$ESG data.Sub_Metric_Title",
            "Metric_Scope": "$ESG data.Metric_Scope",
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
    results["i42b"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    results["i42b"]["sv06"] = (
        df.groupby("company_name")["Sub_Metric_Title"].apply(lambda x: len(set(x.sum()))).to_dict()
    )

    total_counts = {}
    # Iterate through each iteration of the for loop
    for i in range(len(df)):
        ESG = pd.DataFrame(df["ESG data"][i])[["Sub_Metric_Title", "Metric_Scope"]]
        # Count distinct Sub_Metric_Titles for each Metric_Scope
        distinct_counts = ESG.groupby('Metric_Scope')['Sub_Metric_Title'].nunique().to_dict()
        # Add the counts from this iteration to the running total
        for scope, count in distinct_counts.items():
            if scope in total_counts:
                total_counts[scope] += count
            else:
                total_counts[scope] = count
    results["i42b"]["sv17"] = total_counts

    return results
