from utils import uf


def i33_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"Trademarks": {"$elemMatch": {"$exists": True}}}},
        {
            "$group": {
                "_id": field,
                "count": {
                    "$push": {
                        "company_name": "$company_name",
                        "Trademarks": {"$size": "$Trademarks"},
                    }
                },
            }
        },
    ]


def ind_caller(enco, results):
    results["i33"] = {}
    results["i33"]["sv00"] = uf.top_companies(enco, ["all"], i33_aggregation, 100)
    results["i33"]["sv07"] = uf.top_companies_nace(
        enco, ["$NACE 2 digits", "$NACE 2 digits label"], i33_aggregation, 100
    )
    results["i33"]["sv07b"] = uf.top_companies_nace(
        enco, ["$NACE 4 digits", "$NACE 4 digits label"], i33_aggregation, 100
    )
    results["i33"]["sv09"] = uf.top_companies(
        enco, ["$Country ISO code"], i33_aggregation, 100
    )

    return results
