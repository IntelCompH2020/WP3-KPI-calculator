import pandas as pd


template = [
    {
        "$addFields": {
            "NACE2dl": {"$concat": ["$NACE 2 digits", ":", "$NACE 2 digits label"]},
            "NACE4dl": {"$concat": ["$NACE 4 digits", ":", "$NACE 4 digits label"]},
        }
    }
]


def ind_caller(enco, results, extra_aggr_param=[], working_path=""):
    results["i30a"] = {}

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

    try:
        results["i30a"]["sv00"] = df.set_index("company_name")[
            "RevenueByEmployee"
        ].to_dict()
    except Exception as e:
        results["i30a"]["sv00"] = None
        print(f"Error calculating i30a[sv00]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby("company_name")[["NACE2dl", "RevenueByEmployee"]]
            .sum()
            .to_dict(orient="index")
        )
        # Post-processing to get desired format
        results["i30a"]["sv07"] = {
            k: {v["NACE2dl"]: v["RevenueByEmployee"]} for k, v in df_dict.items()
        }
    except Exception as e:
        results["i30a"]["sv07"] = None
        print(f"Error calculating i30a[sv07]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby("company_name")[["NACE4dl", "RevenueByEmployee"]]
            .sum()
            .to_dict(orient="index")
        )
        # Post-processing to get desired format
        results["i30a"]["sv07b"] = {
            k: {v["NACE4dl"]: v["RevenueByEmployee"]} for k, v in df_dict.items()
        }
    except Exception as e:
        results["i30a"]["sv07b"] = None
        print(f"Error calculating i30a[sv07b]: {str(e)}")

    try:
        # Convert DataFrame to dictionary
        df_dict = (
            df.groupby("company_name")[["Country ISO code", "RevenueByEmployee"]]
            .sum()
            .to_dict(orient="index")
        )
        # Post-processing to get desired format
        results["i30a"]["sv09"] = {
            k: {v["Country ISO code"]: v["RevenueByEmployee"]}
            for k, v in df_dict.items()
        }
    except Exception as e:
        results["i30a"]["sv09"] = None
        print(f"Error calculating i30a[sv09]: {str(e)}")

    return results
