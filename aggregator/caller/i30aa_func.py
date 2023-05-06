from utils import uf


def i30aa_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i30aa_aggregation_nace2(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$NACE 2 digits", "$NACE 2 digits label"],
                "count": {"$sum": 1},
            }
        }
    ]


def i30aa_aggregation_nace4(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$NACE 4 digits", "$NACE 4 digits label"],
                "count": {"$sum": 1},
            }
        }
    ]


def ind_caller(enco, results):
    results["i30aa"] = {}
    results["i30aa"]["sv07"] = uf.inner_secondary_view_nace_cpc_companies(
        enco, "NACE 2 digits label", i30aa_aggregation_nace2
    )
    results["i30aa"]["sv07b"] = uf.inner_secondary_view_nace_cpc_companies(
        enco, "NACE 4 digits label", i30aa_aggregation_nace4
    )
    results["i30aa"]["sv09"] = uf.secondary_view(
        enco, "Country ISO code", i30aa_aggregation
    )

    return results
