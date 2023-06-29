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
    results["i42e"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df["TurnoverNumeric"] = pd.to_numeric(df["Turnover"], errors="coerce").fillna(0)
    df = df.sort_values(by=["TurnoverNumeric"], ascending=False).reset_index(drop=True)
    df = df.head(10)

    for i in range(len(df)):
        df_ESG_nonvalue = pd.DataFrame(df["ESG data"][i])[
            pd.DataFrame(df["ESG data"][i])["Rank"].notna()
        ]
        numerator = df_ESG_nonvalue.pivot_table(
            index="Year", columns="Metric_Scope", aggfunc="count"
        )["Sub_Metric_Title"]
        df_ESG = pd.DataFrame(df["ESG data"][i])
        denominator = df_ESG.pivot_table(
            index="Year", columns="Metric_Scope", aggfunc="count"
        )["Sub_Metric_Title"]

        if i == 0:
            result_df = denominator
            result_df_nonvalue = numerator
        else:
            result_df = result_df.add(denominator, fill_value=0)
            result_df_nonvalue = result_df_nonvalue.add(numerator, fill_value=0)

    # Add the dictionary of results to the main results dictionary
    results["i42e"]["sv17.01"] = (
        (100 * result_df_nonvalue.div(result_df)).fillna(0).to_dict()
    )

    return results
