from utils import uf


def i33_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"Trademarks": {"$elemMatch": {"$exists": True}}}},
        {"$group": {"_id": field, "count": {"$sum": {"$size": "$Trademarks"}}}},
    ]


def ind_caller(enco, results):
    results["i33a"] = {}
    results["i33a"]["sv00"] = uf.secondary_view_comp(enco, ["all"], i33_aggregation)
    results["i33a"]["sv07"] = uf.secondary_view_comp_nace(
        enco, ["$NACE 2 digits", "$NACE 2 digits label"], i33_aggregation
    )
    results["i33a"]["sv07b"] = uf.secondary_view_comp_nace(
        enco, ["$NACE 4 digits", "$NACE 4 digits label"], i33_aggregation
    )
    results["i33a"]["sv09"] = uf.secondary_view_comp(
        enco, ["$Country ISO code"], i33_aggregation
    )

    return results
