
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
    results["i32aa"] = {}

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
        results["i32aa"]["sv02.01"] = (cross / len(df)).to_dict()
    except Exception as e:
        results["i32aa"]["sv02.01"] = None
        print(f"Error calculating i32aa[sv02.01]: {str(e)}")


    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby(["Country ISO code"])["total_patents"]
            .sum()
        ) / len(df)
        df_dict = df_dict.to_dict()
        results["i32aa"]["sv09"] = df_dict
    except Exception as e:
        results["i32aa"]["sv09"] = None
        print(f"Error calculating i32aa[sv09]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby(["CompanySize"])["total_patents"]
            .sum()
        ) / len(df)
        df_dict = df_dict.to_dict()
        # Post-processing to get desired format
        results["i32aa"]["sv15"] = df_dict
    except Exception as e:
        results["i32aa"]["sv15"] = None
        print(f"Error calculating i32aa[sv15]: {str(e)}")

    return results
