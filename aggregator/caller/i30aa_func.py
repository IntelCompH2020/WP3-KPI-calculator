import pandas as pd


template = [
    {
        "$addFields": {
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},  #
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},  #
        }
    }
]


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
    results["i30aa"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    try:
        row_count = len(df)
        results["i30aa"]["sv00"] = {"Companies": row_count}
    except Exception as e:
        results["i30aa"]["sv00"] = None
        print(f"Error calculating i30aa[sv00]: {str(e)}")

    try:
        results["i30aa"]["sv07"] = df.groupby("NACE2dl")["_id"].count().to_dict()
    except Exception as e:
        results["i30aa"]["sv07"] = None
        print(f"Error calculating i30aa[sv07]: {str(e)}")

    try:
        results["i30aa"]["sv07b"] = df.groupby("NACE4dl")["_id"].count().to_dict()
    except Exception as e:
        results["i30aa"]["sv07b"] = None
        print(f"Error calculating i30aa[sv07b]: {str(e)}")

    try:
        results["i30aa"]["sv09"] = (
            df.groupby("Country ISO code")["_id"].count().to_dict()
        )
    except Exception as e:
        results["i30aa"]["sv09"] = None
        print(f"Error calculating i30aa[sv09]: {str(e)}")

    return results
