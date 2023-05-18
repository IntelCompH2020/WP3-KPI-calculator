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
    results["i31aa"] = {}

    # # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)

    total_pubs = df["total_publications"].sum()

    results["i31aa"]["sv00"] = {
        "average_per_company": (
            total_pubs / len(df.set_index("company_name")["total_publications"])
        )
    }
    results["i31aa"]["sv07"] = {
        "average_per_NACE2dl": (
            total_pubs / len(df.groupby("NACE4dl")["total_publications"].sum())
        )
    }
    results["i31aa"]["sv07b"] = {
        "average_per_NACE4dl": (
            total_pubs / len(df.groupby("NACE2dl")["total_publications"].sum())
        )
    }
    results["i31aa"]["sv09"] = {
        "average_per_Country": (
            total_pubs / len(df.groupby("Country ISO code")["total_publications"].sum())
        )
    }

    return results
