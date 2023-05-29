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


def ind_caller(enco, results, extra_aggr_param):
    results["i32"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_patents"], ascending=False).reset_index(drop=True)
    df = df.head(100)

    try:
        results["i32"]["sv00"] = df.set_index("company_name")["total_patents"].to_dict()
    except Exception as e:
        results["i32"]["sv00"] = None
        print(f"Error calculating i32[sv00]: {str(e)}")
    try:
        results["i32"]["sv07"] = df.groupby("NACE2dl")["total_patents"].sum().to_dict()
    except Exception as e:
        results["i32"]["sv07"] = None
        print(f"Error calculating i32[sv07]: {str(e)}")
    try:
        results["i32"]["sv07b"] = df.groupby("NACE4dl")["total_patents"].sum().to_dict()
    except Exception as e:
        results["i32"]["sv07b"] = None
        print(f"Error calculating i32[sv07b]: {str(e)}")
    try:
        results["i32"]["sv09"] = (
            df.groupby("Country ISO code")["total_patents"].sum().to_dict()
        )
    except Exception as e:
        results["i32"]["sv09"] = None
        print(f"Error calculating i32[sv09]: {str(e)}")

    return results
