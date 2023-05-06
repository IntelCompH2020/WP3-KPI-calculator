from utils import uf


def i32_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"Patents": {"$elemMatch": {"$exists": True}}}},
        {"$group": {"_id": field, "count": {"$sum": {"$size": "$Patents"}}}},
    ]


def ind_caller(enco, results):
    results["i32a"] = {}
    results["i32a"]["sv00"] = uf.secondary_view_comp(enco, ["all"], i32_aggregation)
    results["i32a"]["sv07"] = uf.secondary_view_comp_nace(
        enco, ["$NACE 2 digits", "$NACE 2 digits label"], i32_aggregation
    )
    results["i32a"]["sv07b"] = uf.secondary_view_comp_nace(
        enco, ["$NACE 4 digits", "$NACE 4 digits label"], i32_aggregation
    )
    results["i32a"]["sv09"] = uf.secondary_view_comp(
        enco, ["$Country ISO code"], i32_aggregation
    )

    return results
