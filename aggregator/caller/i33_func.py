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


def ind_caller(enco, results, extra_aggr_param):
    results["i33"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_trademarks"], ascending=False).reset_index(drop=True)
    df = df.head(100)

    results["i33"]["sv00"] = df.set_index("company_name")["total_trademarks"].to_dict()
    results["i33"]["sv07"] = df.groupby("NACE2dl")["total_trademarks"].sum().to_dict()
    results["i33"]["sv07b"] = df.groupby("NACE4dl")["total_trademarks"].sum().to_dict()
    results["i33"]["sv09"] = (
        df.groupby("Country ISO code")["total_trademarks"].sum().to_dict()
    )

    return results
