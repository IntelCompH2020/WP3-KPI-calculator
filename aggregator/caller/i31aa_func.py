import pandas as pd


template = [
    {"$match": {"Publications": {"$exists": True, "$not": {"$size": 0}}}},
    {
        "$addFields": {
            "total_publications": {"$size": "$Publications"},
        }
    },
]

lookup = [
    {
        "$lookup": {
            "from": "SDGs_companies",
            "localField": "doi",
            "foreignField": "Publications.DOI",
            "as": "sdg",
        }
    },
    {"$set": {"sdg": {"$arrayElemAt": ["$sdg.SDGs", 0]}}},
]


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
    results["i31aa"] = {}

    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)

    try:
        frames = []  # list to store all dataframes

        for i in range(len(df)):
            df_pub = pd.DataFrame(df["Publications"][i]).explode("Topics")
            frames.append(df_pub)

        # concatenate all the dataframes in the list
        publications_df = pd.concat(frames, ignore_index=True)
        cross = pd.crosstab(publications_df["Year"], publications_df["Topics"])
        results["i31aa"]["sv02.01"] = (cross / len(df)).to_dict()
    except Exception as e:
        results["i31aa"]["sv02.01"] = None
        print(f"Error calculating i31aa[sv02.01]: {str(e)}")

    try:
        results["i31aa"]["sv09"] = (
            df.groupby("Country ISO code")["total_publications"].mean().to_dict()
        )
    except Exception as e:
        results["i31aa"]["sv09"] = None
        print(f"Error calculating i31aa[sv09]: {str(e)}")

    try:
        results["i31aa"]["sv15"] = (
            df.groupby("CompanySize")["total_publications"].mean().to_dict()
        )
    except Exception as e:
        results["i31aa"]["sv15"] = None
        print(f"Error calculating i31aa[sv15]: {str(e)}")

    try:
        documents = enco.aggregate(extra_aggr_param + template + lookup)
        df = pd.DataFrame(list(documents))
        df = df.sort_values(by=["total_publications"], ascending=False).reset_index(
            drop=True
        )
        df = df.head(100)
        results["i31aa"]["sv05"] = (
            df.groupby("Country ISO code")["total_publications"].mean().to_dict()
        )
    except Exception as e:
        results["i31aa"]["sv05"] = None
        print(f"Error calculating i31aa[sv05]: {str(e)}")

    return results
