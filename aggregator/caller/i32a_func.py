import pandas as pd

template = [
    {"$match": {"Patents": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_patents": {"$size": "$Patents"},
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
    results["i32a"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_patents"], ascending=False).reset_index(drop=True)
    df = df.head(100)
    try:
        results["i32a"]["sv00"] = {"total_patents": int(df["total_patents"].sum())}
    except Exception as e:
        results["i32a"]["sv00"] = None
        print(f"Error calculating i32a[sv00]: {str(e)}")

    try:
        results["i32a"]["sv07"] = df.groupby("NACE2dl")["total_patents"].sum().to_dict()
    except Exception as e:
        results["i32a"]["sv07"] = None
        print(f"Error calculating i32a[sv07]: {str(e)}")

    try:
        results["i32a"]["sv07b"] = (
            df.groupby("NACE4dl")["total_patents"].sum().to_dict()
        )
    except Exception as e:
        results["i32a"]["sv07b"] = None
        print(f"Error calculating i32a[sv07b]: {str(e)}")

    try:
        results["i32a"]["sv09"] = (
            df.groupby("Country ISO code")["total_patents"].sum().to_dict()
        )
    except Exception as e:
        results["i32a"]["sv09"] = None
        print(f"Error calculating i32a[sv09]: {str(e)}")

    return results
