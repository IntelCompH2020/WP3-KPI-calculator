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
    results["i32aa"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_patents"], ascending=False).reset_index(drop=True)
    df = df.head(100)

    total_pats = df["total_patents"].sum()

    results["i32aa"]["sv00"] = {
        "average_per_company": (
            total_pats / len(df.set_index("company_name")["total_patents"])
        )
    }
    results["i32aa"]["sv07"] = {
        "average_per_NACE2dl": (
            total_pats / len(df.groupby("NACE4dl")["total_patents"].sum())
        )
    }
    results["i32aa"]["sv07b"] = {
        "average_per_NACE4dl": (
            total_pats / len(df.groupby("NACE2dl")["total_patents"].sum())
        )
    }
    results["i32aa"]["sv09"] = {
        "average_per_Country": (
            total_pats / len(df.groupby("Country ISO code")["total_patents"].sum())
        )
    }

    # print(results)

    return results
