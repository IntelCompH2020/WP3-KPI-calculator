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


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
    results["i31"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)

    try:
        results["i31"]["sv00"] = df.set_index("company_name")[
            "total_publications"
        ].to_dict()
    except Exception as e:
        results["i31"]["sv00"] = None
        print(f"Error calculating i31[sv00]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby("company_name")[["NACE2dl", "total_publications"]]
            .sum()
            .to_dict(orient="index")
        )
        # Post-processing to get desired format
        results["i31"]["sv07"] = {
            k: {v["NACE2dl"]: v["total_publications"]} for k, v in df_dict.items()
        }
    except Exception as e:
        results["i31"]["sv07"] = None
        print(f"Error calculating i31[sv07]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby("company_name")[["NACE4dl", "total_publications"]]
            .sum()
            .to_dict(orient="index")
        )
        # Post-processing to get desired format
        results["i31"]["sv07b"] = {
            k: {v["NACE4dl"]: v["total_publications"]} for k, v in df_dict.items()
        }
    except Exception as e:
        results["i31"]["sv07b"] = None
        print(f"Error calculating i31[sv07b]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby("company_name")[["Country ISO code", "total_publications"]]
            .sum()
            .to_dict(orient="index")
        )
        # Post-processing to get desired format
        results["i31"]["sv09"] = {
            k: {v["Country ISO code"]: v["total_publications"]}
            for k, v in df_dict.items()
        }
    except Exception as e:
        results["i31"]["sv09"] = None
        print(f"Error calculating i31[sv09]: {str(e)}")

    return results
