import pandas as pd

# Define MongoDB template for filtering documents
template = [
    {
        "$match": {
            "ESG data": {"$exists": True},
            "ESG data.Sub_Metric_Title": {"$exists": True},
        }
    },
    {
        "$addFields": {
            "NumberOfESGdata": {"$size": "$ESG data"},
            "Year": "$ESG data.Year",
        }
    },
    {
        "$match": {
            "NumberOfESGdata": {"$ne": 0},
        }
    },
]


def ind_caller(enco, results, extra_aggr_param=[], spark_output=""):
    # Initialize a new dictionary for the results
    results["i42d"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df["TurnoverNumeric"] = pd.to_numeric(df["Turnover"], errors="coerce").fillna(0)
    df = df.sort_values(by=["TurnoverNumeric"], ascending=False).reset_index(drop=True)
    df = df.head(10)

    # Initialize an empty DataFrame to store the accumulated nonvaluecounts
    result_df_nonvalue = pd.DataFrame()
    # Initialize an empty DataFrame to store the accumulated counts
    result_df = pd.DataFrame()

    # Iterate through each iteration of the for loop
    for i in range(len(df)):
        counts_nonvalue = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]["Metric_Title"].value_counts()
        counts = pd.DataFrame(df["ESG data"][i])["Metric_Title"].value_counts()

        if i == 0:
            result_df = counts
            result_df_nonvalue = counts_nonvalue
        else:
            result_df = result_df.add(counts, fill_value=0)
            result_df_nonvalue = result_df_nonvalue.add(counts_nonvalue, fill_value=0)

    sorted_df = (100 * result_df_nonvalue.div(result_df)).sort_values(ascending=False)
    results["i42d"]["sv00"] = sorted_df.fillna(0).tail(5).to_dict()

    return results
