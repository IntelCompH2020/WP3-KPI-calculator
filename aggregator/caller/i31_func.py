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


def ind_caller(enco, results, extra_aggr_param):
    results["i31"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)

    results["i31"]["sv00"] = df.set_index("company_name")[
        "total_publications"
    ].to_dict()
    results["i31"]["sv07"] = df.groupby("NACE2dl")["total_publications"].sum().to_dict()
    results["i31"]["sv07b"] = (
        df.groupby("NACE4dl")["total_publications"].sum().to_dict()
    )
    results["i31"]["sv09"] = (
        df.groupby("Country ISO code")["total_publications"].sum().to_dict()
    )

    return results
