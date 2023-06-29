import pandas as pd


template = [
    {"$match": {"Publications": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_publications": {"$size": "$Publications"},
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
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

    try:
        results["i31a"]["sv07"] = (
            df.groupby("NACE2dl")["total_publications"].sum().to_dict()
        )
    except Exception as e:
        results["i31a"]["sv07"] = None
        print(f"Error calculating i31[sv07]: {str(e)}")

    try:
        results["i31a"]["sv07b"] = (
            df.groupby("NACE4dl")["total_publications"].sum().to_dict()
        )
    except Exception as e:
        results["i31a"]["sv07b"] = None
        print(f"Error calculating i31[sv07b]: {str(e)}")

    try:
        results["i31a"]["sv09"] = (
            df.groupby("Country ISO code")["total_publications"].sum().to_dict()
        )
    except Exception as e:
        results["i31a"]["sv09"] = None
        print(f"Error calculating i31[sv09]: {str(e)}")

    return results
