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
    results["i33aa"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_trademarks"], ascending=False).reset_index(drop=True)
    df = df.head(100)

    total_trade = df["total_trademarks"].sum()

    results["i33aa"]["sv00"] = {
        "average_per_company": (
            total_trade / len(df.set_index("company_name")["total_trademarks"])
        )
    }
    results["i33aa"]["sv07"] = {
        "average_per_NACE2dl": (
            total_trade / len(df.groupby("NACE4dl")["total_trademarks"].sum())
        )
    }
    results["i33aa"]["sv07b"] = {
        "average_per_NACE4dl": (
            total_trade / len(df.groupby("NACE2dl")["total_trademarks"].sum())
        )
    }
    results["i33aa"]["sv09"] = {
        "average_per_Country": (
            total_trade / len(df.groupby("Country ISO code")["total_trademarks"].sum())
        )
    }

    return results
