import pandas as pd

template = [
    {"$match": {"Trademarks": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_trademarks": {"$size": "$Trademarks"},
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},
        }
    },
]


def ind_caller(enco, results, logging, extra_aggr_param=[], working_path=""):
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
    try:
        results["i33a"]["sv07"] = (
            df.groupby("NACE2dl")["total_trademarks"].sum().to_dict()
        )
    except Exception as e:
        results["i33a"]["sv07"] = None
        print(f"Error calculating i33a[sv07]: {str(e)}")
    try:
        results["i33a"]["sv07b"] = (
            df.groupby("NACE4dl")["total_trademarks"].sum().to_dict()
        )
    except Exception as e:
        results["i33a"]["sv07b"] = None
        print(f"Error calculating i33a[sv07b]: {str(e)}")
    try:
        results["i33a"]["sv09"] = (
            df.groupby("Country ISO code")["total_trademarks"].sum().to_dict()
        )
    except Exception as e:
        results["i33a"]["sv09"] = None
        print(f"Error calculating i33a[sv09]: {str(e)}")

    return results
