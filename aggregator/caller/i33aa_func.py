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


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
    results["i33aa"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    df = df.sort_values(by=["total_trademarks"], ascending=False).reset_index(drop=True)
    df = df.head(100)

    try:
        frames = []  # list to store all dataframes

        for i in range(len(df)):
            df_pub = pd.DataFrame(df["Trademarks"][i])
            frames.append(df_pub)
        # concatenate all the dataframes in the list
        publications_df = pd.concat(frames, ignore_index=True)
        results["i33aa"]["sv01"] = (
            publications_df["registration_year"].value_counts() / len(df)
        ).to_dict()
    except Exception as e:
        results["i33aa"]["sv01"] = None
        print(f"Error calculating i33aa[sv01]: {str(e)}")

    try:
        frames = []  # list to store all dataframes

        for i in range(len(df)):
            df_pub = pd.DataFrame(df["Trademarks"][i])
            frames.append(df_pub)
        # concatenate all the dataframes in the list
        publications_df = pd.concat(frames, ignore_index=True)
        results["i33aa"]["sv21"] = (
            publications_df["trademark_type"].value_counts() / len(df)
        ).to_dict()
    except Exception as e:
        results["i33aa"]["sv21"] = None
        print(f"Error calculating i33aa[sv21]: {str(e)}")

    try:
        results["i33aa"]["sv09"] = (
            df.groupby("Country ISO code")["total_trademarks"].mean().to_dict()
        )
    except Exception as e:
        results["i33aa"]["sv09"] = None
        print(f"Error calculating i33aa[sv09]: {str(e)}")

    try:
        results["i33aa"]["sv15"] = (
            df.groupby("CompanySize")["total_trademarks"].mean().to_dict()
        )
    except Exception as e:
        results["i33aa"]["sv15"] = None
        print(f"Error calculating i33aa[sv15]: {str(e)}")

    return results
