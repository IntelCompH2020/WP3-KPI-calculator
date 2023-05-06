from utils import uf


def i12_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i12_aggregation_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i12_aggregation_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i12_aggregation_forward(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": "$" + field, "count": {"$sum": "$citations.forward"}}}
    ]


def i12_aggregation_forward_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.forward"},
            }
        }
    ]


def i12_aggregation_forward_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": "$citations.forward"},
            }
        }
    ]


def ind_caller(pat, results, extra_aggr_param=[]):
    results["i12"] = {}
    numerator = uf.secondary_view(
        pat, "appln_filing_year", i12_aggregation_forward, extra_aggr_param
    )
    denominator = uf.secondary_view(
        pat, "appln_filing_year", i12_aggregation, extra_aggr_param
    )
    results["i12"]["sv01"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv01"][k] = numerator[k] / denominator[k]

    numerator = uf.secondary_view(
        pat, "topic", i12_aggregation_forward, extra_aggr_param
    )
    denominator = uf.secondary_view(pat, "topic", i12_aggregation, extra_aggr_param)
    results["i12"]["sv02"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv02"][k] = numerator[k] / denominator[k]

    numerator = uf.secondary_view(
        pat, "category", i12_aggregation_forward, extra_aggr_param
    )
    denominator = uf.secondary_view(pat, "category", i12_aggregation, extra_aggr_param)
    results["i12"]["sv03"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv03"][k] = numerator[k] / denominator[k]

    numerator = uf.inner_secondary_view(
        pat, "participant.name", i12_aggregation_forward
    )
    denominator = uf.inner_secondary_view(pat, "participant.name", i12_aggregation)
    results["i12"]["sv06"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv06"][k] = numerator[k] / denominator[k]
    results["i12"]["sv06"]

    numerator = uf.inner_secondary_view_nace_cpc(
        pat, "nace.nace2_code", i12_aggregation_forward_nace, extra_aggr_param
    )
    denominator = uf.inner_secondary_view_nace_cpc(
        pat, "nace.nace2_code", i12_aggregation_nace, extra_aggr_param
    )
    results["i12"]["sv07"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv07"][k] = numerator[k] / denominator[k]
    numerator = uf.inner_secondary_view(
        pat, "participant.sector", i12_aggregation_forward, extra_aggr_param
    )
    denominator = uf.inner_secondary_view(
        pat, "participant.sector", i12_aggregation, extra_aggr_param
    )
    results["i12"]["sv08"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv08"][k] = numerator[k] / denominator[k]

    numerator = uf.inner_secondary_view(
        pat, "participant.country", i12_aggregation_forward, extra_aggr_param
    )
    denominator = uf.inner_secondary_view(
        pat, "participant.country", i12_aggregation, extra_aggr_param
    )
    full_set = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        full_set[k] = numerator[k] / denominator[k]

    results["i12"]["sv09"] = {}
    for k in full_set.keys():
        if k in uf.eu_members_code:
            results["i12"]["sv09"][
                uf.eu_members[uf.eu_members_code.index(k)]
            ] = full_set[k]

    numerator = uf.inner_secondary_view_nace_cpc(
        pat, "ipc.ipc_class", i12_aggregation_forward_cpc, extra_aggr_param
    )
    denominator = uf.inner_secondary_view_nace_cpc(
        pat, "ipc.ipc_class", i12_aggregation_cpc, extra_aggr_param
    )
    results["i12"]["sv13"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
    results["i12"]["sv13"][k] = numerator[k] / denominator[k]

    return results
