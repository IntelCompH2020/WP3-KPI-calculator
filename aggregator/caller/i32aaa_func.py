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


def ind_caller(enco, results, logging, extra_aggr_param=[], working_path=""):
    results["i32aaa"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    df = df.sort_values(by=["total_patents"], ascending=False).reset_index(drop=True)
    

    try:
        frames = []  # list to store all dataframes

        for i in range(len(df)):
            df_pub = pd.DataFrame(df["Patents"][i]).explode("topic")
            frames.append(df_pub)

        # concatenate all the dataframes in the list
        publications_df = pd.concat(frames, ignore_index=True)
        cross = pd.crosstab(publications_df["year"], publications_df["topic"])
        results["i32aaa"]["sv02.01"] = (cross / len(df)).to_dict()
    except Exception as e:
        results["i32aaa"]["sv02.01"] = None
        print(f"Error calculating i32aaa[sv02.01]: {str(e)}")


    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby(["company_name", "Country ISO code"])["total_patents"]
            .sum()
        ) / len(df)
        df_dict = df_dict.to_dict()
        df_dict_new = {}
        for k, v in df_dict.items():
            company, country = k
            if company not in df_dict_new:
                df_dict_new[company] = {}
            df_dict_new[company][country] = v
        # Post-processing to get desired format
        results["i32aaa"]["sv09"] = df_dict_new
    except Exception as e:
        results["i32aaa"]["sv09"] = None
        print(f"Error calculating i32aaa[sv09]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby(["company_name", "CompanySize"])["total_patents"]
            .sum()
        ) / len(df)
        df_dict = df_dict.to_dict()
        df_dict_new = {}
        for k, v in df_dict.items():
            company, country = k
            if company not in df_dict_new:
                df_dict_new[company] = {}
            df_dict_new[company][country] = v
        # Post-processing to get desired format
        results["i32aaa"]["sv15"] = df_dict_new
    except Exception as e:
        results["i32aaa"]["sv15"] = None
        print(f"Error calculating i32aaa[sv15]: {str(e)}")

    return results
