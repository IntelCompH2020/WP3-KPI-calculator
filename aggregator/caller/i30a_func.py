from utils import uf


def i30a_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$addFields": {
                "TurnoverNumeric": {"$ifNull": [{"$toDouble": "$Turnover"}, 0]},
                "EmployeesNumeric": {
                    "$ifNull": [{"$toDouble": "$Number of employees"}, 1]
                },
            }
        },
        {
            "$match": {"Turnover": {"$ne": None}, "Number of employees": {"$ne": None}},
        },
        {
            "$addFields": {
                "RevenueByEmployee": {
                    "$divide": ["$TurnoverNumeric", "$EmployeesNumeric"]
                }
            }
        },
        {
            "$group": {
                "_id": field,
                "count": {
                    "$push": {
                        "company_name": "$company_name",
                        "revenue_by_employee": "$RevenueByEmployee",
                    }
                },
            }
        },
    ]


def ind_caller(enco, results, extra_aggr_param=[]):
    results["i30a"] = {}
    results["i30a"]["sv00"] = uf.top_companies(
        enco, ["all"], i30a_aggregation, 100, extra_aggr_param
    )
    results["i30a"]["sv07"] = uf.top_companies_nace(
        enco,
        ["$NACE 2 digits", "$NACE 2 digits label"],
        i30a_aggregation,
        100,
        extra_aggr_param,
    )
    results["i30a"]["sv07b"] = uf.top_companies_nace(
        enco,
        ["$NACE 4 digits", "$NACE 4 digits label"],
        i30a_aggregation,
        100,
        extra_aggr_param,
    )
    results["i30a"]["sv09"] = uf.top_companies(
        enco, ["$Country ISO code"], i30a_aggregation, 100, extra_aggr_param
    )

    return results
