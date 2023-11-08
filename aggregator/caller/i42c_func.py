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
    results["i42c"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Initialize an empty DataFrame to store the accumulated nonvaluecounts
    result_df_nonvalue = pd.DataFrame()
    # Initialize an empty DataFrame to store the accumulated counts
    result_df = pd.DataFrame()

    # Iterate through each iteration of the for loop
    for i in range(len(df)):

        ESG = pd.DataFrame(df["ESG data"][i])

        counts = ESG["Metric_Title"].value_counts()
        counts_nonvalue = ESG[ESG["Rank"].notna()]["Metric_Title"].value_counts()

        # Aligning the two Series, fill NaN in the second with 0
        aligned_counts, aligned_nonvalue = counts.align(counts_nonvalue, fill_value=0)

        if i == 0:
            result_df = aligned_counts
            result_df_nonvalue = aligned_nonvalue
        else:
            result_df = result_df.add(aligned_counts, fill_value=0)
            result_df_nonvalue = result_df_nonvalue.add(aligned_nonvalue, fill_value=0)

    sorted_df = (100 * result_df_nonvalue.div(result_df)).sort_values(ascending=False)
    results["i42c"]["sv00"] = sorted_df.fillna(0).head(20).to_dict()


    return results


# count all ESG reports. Count distinct metric titles. Count number of sub metric titles where Rank is not null
# 100 * Count number of metric titles where Rank is not null/ (count all ESG reports* (Count distinct submetric titles per metric title))
# scope 1 emission (metric title) --> 2 submetrics αρα περιμένω 2 τιμές.

# Most reported metrics: 
# Top 10 metrics by coverage (%) 
# (= top 10 sub_metric_title BY share of reports with of non-null value for that metric =
# 100* (# of ESG reports that have a non-null value for a submetric) / (total # of ESG reports with that sub-metric)

# include the metric_title name