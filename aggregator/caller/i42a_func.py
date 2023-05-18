import pandas as pd

# Define MongoDB template for filtering documents
template = [
    {
        "$match": {
            "ESG data": {"$exists": True},
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


def ind_caller(enco, results, extra_aggr_param):
    results["i42a"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    # Sort and select the top 10 rows based on TurnoverNumeric column
    df["TurnoverNumeric"] = pd.to_numeric(df["Turnover"], errors="coerce").fillna(0)
    df = df.sort_values(by=["TurnoverNumeric"], ascending=False).reset_index(drop=True)
    df = df.head(10)

    results["i42a"]["sv00"] = (
        df.groupby("company_name")["Year"].apply(lambda x: len(set(x.sum()))).to_dict()
    )

    return results
