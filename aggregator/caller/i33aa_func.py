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


def ind_caller(enco, results, logging, extra_aggr_param=[], working_path=""):
    results["i33aa"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    df = df.sort_values(by=["total_trademarks"], ascending=False).reset_index(drop=True)

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
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby(["Country ISO code"])["total_trademarks"]
            .sum()
        ) / len(df)
        df_dict = df_dict.to_dict()
        # df_dict_new = {}
        # for k, v in df_dict.items():
        #     company, country = k
        #     if company not in df_dict_new:
        #         df_dict_new[company] = {}
        #     df_dict_new[company][country] = v
        # Post-processing to get desired format
        results["i33aa"]["sv09"] = df_dict
    except Exception as e:
        results["i33aa"]["sv09"] = None
        print(f"Error calculating i33aa[sv09]: {str(e)}")
        
    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby(["CompanySize"])["total_trademarks"]
            .sum()
        ) / len(df)
        df_dict = df_dict.to_dict()
        results["i33aa"]["sv15"] = df_dict
    except Exception as e:
        results["i33aa"]["sv15"] = None
        print(f"Error calculating i33aa[sv15]: {str(e)}")

    return results
