from utils import uf


def i32_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"Patents": {"$elemMatch": {"$exists": True}}}},
        {
            "$group": {
                "_id": field,
                "count": {
                    "$push": {
                        "company_name": "$company_name",
                        "Patents": {"$size": "$Patents"},
                    }
                },
            }
        },
    ]


def ind_caller(enco, results):
    results["i32"] = {}
    results["i32"]["sv00"] = uf.top_companies(enco, ["all"], i32_aggregation, 100)
    results["i32"]["sv07"] = uf.top_companies_nace(
        enco, ["$NACE 2 digits", "$NACE 2 digits label"], i32_aggregation, 100
    )
    results["i32"]["sv07b"] = uf.top_companies_nace(
        enco, ["$NACE 4 digits", "$NACE 4 digits label"], i32_aggregation, 100
    )
    results["i32"]["sv09"] = uf.top_companies(
        enco, ["$Country ISO code"], i32_aggregation, 100
    )

    return results
