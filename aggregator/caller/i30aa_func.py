import pandas as pd

template = [
    {
        "$addFields": {
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},  #
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},  #
        }
    }
]


def ind_caller(enco, results, extra_aggr_param):
    results["i30aa"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    row_count = len(df)

    results["i30aa"]["sv00"] = {"Companies": row_count}
    results["i30aa"]["sv07"] = df.groupby("NACE2dl")["_id"].count().to_dict()
    results["i30aa"]["sv07b"] = df.groupby("NACE4dl")["_id"].count().to_dict()
    results["i30aa"]["sv09"] = df.groupby("Country ISO code")["_id"].count().to_dict()

    return results
