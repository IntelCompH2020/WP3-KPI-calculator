import pandas as pd
from utils import uf

template = [
    {"$match": {"Publications": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_publications": {"$size": "$Publications"},
        }
    },
]


def ind_caller(enco, results, logging, extra_aggr_param=[], working_path=""):
    
    results["i31aa"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )

    try:
        frames = []  # list to store all dataframes

        for i in range(len(df)):
            print(i)
            df_pub = pd.DataFrame(df["Publications"][i]).explode("Topics")
            frames.append(df_pub)

        # concatenate all the dataframes in the list
        publications_df = pd.concat(frames, ignore_index=True)
        cross = pd.crosstab(publications_df["Year"], publications_df["Topics"])
        results["i31aa"]["sv02.01"] = (cross / len(df)).to_dict()
    except Exception as e:
        results["i31aa"]["sv02.01"] = None
        print(f"Error calculating i31aa[sv02.01]: {str(e)}")

    # try:
    #     documents = enco.aggregate(extra_aggr_param + template)
    #     df = pd.DataFrame(list(documents))
    #     df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
    #         drop=True
    #     )

    #     df_pubs = df.explode('Publications').reset_index(drop=True)
    #     df_pubs['Publications'] = df_pubs['Publications'].apply(lambda x: {} if pd.isna(x) else x)  # Convert NaN to empty dict
    #     df_pubs = pd.concat([df_pubs.drop(['Publications'], axis=1), df_pubs['Publications'].apply(pd.Series)], axis=1)
    #     # Drop rows where SDGs is NaN
    #     df_pubs = df_pubs.dropna(subset=['SDGs'])
    #     # Explode the SDGs column
    #     df_pubs = df_pubs.explode('SDGs').reset_index(drop=True)
    #     # Convert DataFrame to dictionary
    #     df_dict = df_pubs.groupby("SDGs").size() / len(df_pubs)
    #     df_dict = df_dict.to_dict()
    #     # Post-processing to get desired format
    #     results["i31aa"]["sv05"] = df_dict
    # except Exception as e:
    #     results["i31aa"]["sv05"] = None
    #     print(f"Error calculating i31aa[sv05]: {str(e)}")

    # try:
    #     # Convert DataFrame to dictionary
    #     df_dict = (
    #         df.groupby(["Country ISO code"])["total_publications"]
    #         .sum()
    #     ) / len(df)
    #     df_dict = df_dict.to_dict()
    #     # # Post-processing to get desired format
    #     results["i31aa"]["sv09"] = df_dict
    # except Exception as e:
    #     results["i31aa"]["sv09"] = None
    #     print(f"Error calculating i31aa[sv09]: {str(e)}")

    # try:
    #     # Convert DataFrame to dictionary
    #     df_dict = (
    #         df.groupby(["CompanySize"])["total_publications"]
    #         .sum()
    #     ) / len(df)
    #     df_dict = df_dict.to_dict()
    #     # Post-processing to get desired format
    #     results["i31aa"]["sv15"] = df_dict
    # except Exception as e:
    #     results["i31aa"]["sv15"] = None
    #     print(f"Error calculating i31aa[sv15]: {str(e)}")
        

    return results
