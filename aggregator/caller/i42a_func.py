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
    results["i42a"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    results["i42a"]["sv00"] = (
        df.groupby("company_name")["Year"].apply(lambda x: len(set(x.sum()))).to_dict()
    )
    
    return results
