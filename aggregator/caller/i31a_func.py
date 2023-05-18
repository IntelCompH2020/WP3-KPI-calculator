import pandas as pd

template = [
    {"$match": {"Publications": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_publications": {"$size": "$Publications"},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param):
    results["i31a"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)
    results["i31a"]["sv00"] = {
        "total_publications": int(df["total_publications"].sum())
    }

    return results
