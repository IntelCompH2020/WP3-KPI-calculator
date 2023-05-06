from utils import uf


def i31_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"Publications": {"$elemMatch": {"$exists": True}}}},
        {"$group": {"_id": field, "count": {"$sum": {"$size": "$Publications"}}}},
    ]


def ind_caller(enco, results):
    results["i31a"] = {}
    results["i31a"]["sv00"] = uf.secondary_view_comp(enco, ["all"], i31_aggregation)
    results["i31a"]["sv07"] = uf.secondary_view_comp_nace(
        enco, ["$NACE 2 digits", "$NACE 2 digits label"], i31_aggregation
    )
    results["i31a"]["sv07b"] = uf.secondary_view_comp_nace(
        enco, ["$NACE 4 digits", "$NACE 4 digits label"], i31_aggregation
    )
    results["i31a"]["sv09"] = uf.secondary_view_comp(
        enco, ["$Country ISO code"], i31_aggregation
    )

    return results
