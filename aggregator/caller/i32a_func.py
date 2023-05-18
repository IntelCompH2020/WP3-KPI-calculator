import pandas as pd

template = [
    {"$match": {"Patents": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_patents": {"$size": "$Patents"},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param):
    results["i32a"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_patents"], ascending=False).reset_index(drop=True)
    df = df.head(100)
    results["i32a"]["sv00"] = {"total_patents": int(df["total_patents"].sum())}

    return results
