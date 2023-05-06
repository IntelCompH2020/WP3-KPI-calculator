from utils import uf


def i31_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"Publications": {"$elemMatch": {"$exists": True}}}},
        {
            "$group": {
                "_id": field,
                "count": {
                    "$push": {
                        "company_name": "$company_name",
                        "Publications": {"$size": "$Publications"},
                    }
                },
            }
        },
    ]


def ind_caller(enco, results):
    results["i31"] = {}
    results["i31"]["sv00"] = uf.top_companies(enco, ["all"], i31_aggregation, 10)
    results["i31"]["sv07"] = uf.top_companies_nace(
        enco, ["$NACE 2 digits", "$NACE 2 digits label"], i31_aggregation, 100
    )
    results["i31"]["sv07b"] = uf.top_companies_nace(
        enco, ["$NACE 4 digits", "$NACE 4 digits label"], i31_aggregation, 100
    )
    results["i31"]["sv09"] = uf.top_companies(
        enco, ["$Country ISO code"], i31_aggregation, 100
    )

    return results
