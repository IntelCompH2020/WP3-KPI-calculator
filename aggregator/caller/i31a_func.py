import pandas as pd

template = [
    {"$match": {"Publications": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_publications": {"$size": "$Publications"},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param=[]):
    results["i31a"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)

    try:
        results["i31a"]["sv00"] = {
            "total_publications": int(df["total_publications"].sum())
        }
    except Exception as e:
        results["i31a"]["sv00"] = None
        print(f"Error calculating i31a[sv00]: {str(e)}")

    return results
