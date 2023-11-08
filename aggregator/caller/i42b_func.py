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
            "Sub_Metric_Title": "$ESG data.Sub_Metric_Title",
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

    results["i42b"]["sv17"] = (
        df.groupby("company_name")["Sub_Metric_Title"].apply(lambda x: len(set(x.sum()))).to_dict()
    )

    return results
