import pandas as pd


template = [
    {
        "$addFields": {
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},
        }
    }
]


def ind_caller(enco, results, extra_aggr_param=[]):
    results["i30a"] = {}

    # Find documents and convert to dataframe
    documents = enco.aggregate(extra_aggr_param + template)
    df = pd.DataFrame(list(documents))

    df["TurnoverNumeric"] = pd.to_numeric(df["Turnover"], errors="coerce").fillna(0)
    df["EmployeesNumeric"] = pd.to_numeric(
        df["Number of employees"], errors="coerce"
    ).fillna(0)
    df = df[df["TurnoverNumeric"].notnull() & (df["EmployeesNumeric"] != 0)]
    df["RevenueByEmployee"] = df["TurnoverNumeric"] / df["EmployeesNumeric"]

    df = df.sort_values(by=["RevenueByEmployee"], ascending=False).reset_index(
        drop=True
    )
    df = df.head(100)

    results["i30a"]["sv00"] = df.set_index("company_name")[
        "RevenueByEmployee"
    ].to_dict()
    results["i30a"]["sv07"] = (
        df.groupby("NACE2dl")["RevenueByEmployee"].count().to_dict()
    )
    results["i30a"]["sv07b"] = (
        df.groupby("NACE4dl")["RevenueByEmployee"].count().to_dict()
    )
    results["i30a"]["sv09"] = (
        df.groupby("Country ISO code")["RevenueByEmployee"].count().to_dict()
    )

    return results
