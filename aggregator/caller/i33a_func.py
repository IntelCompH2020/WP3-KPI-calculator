import pandas as pd

template = [
    {"$match": {"Trademarks": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_trademarks": {"$size": "$Trademarks"},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param):
    results["i33a"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_trademarks"], ascending=False).reset_index(drop=True)
    df = df.head(100)
    try:
        results["i33a"]["sv00"] = {
            "total_trademarks": int(df["total_trademarks"].sum())
        }
    except Exception as e:
        results["i33a"]["sv00"] = None
        print(f"Error calculating i33a[sv00]: {str(e)}")

    return results
