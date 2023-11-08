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


def ind_caller(enco, results, logging, extra_aggr_param=[], working_path=""):
    # Initialize a new dictionary for the results
    results["i42e"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))
    for i in range(len(df)):

        ESG = pd.DataFrame(df["ESG data"][i])
        df_ESG_nonvalue = ESG[ESG["Rank"].notna()]

        numerator = df_ESG_nonvalue.pivot_table(
            index="Year", columns="Metric_Scope", aggfunc="count"
        )["Sub_Metric_Title"]
        denominator = ESG.pivot_table(
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


    # Count distinct submetric. Count not null submetric per year and (Company ID).
    # Count not null submetric per year and (Company ID). / Count distinct submetric
    # average share (%) of non-null metrics per report = 
    # average (100* number of sub_metric_title with non-null values/total number of sub_metrics) per report
    
