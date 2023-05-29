from utils import uf


def i13_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i13_aggregation_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i13_aggregation_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i13_aggregation_npl(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": "$" + field,
                "count": {"$sum": "$citations.non_patent_literature"},
            }
        }
    ]


def i13_aggregation_npl_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.non_patent_literature"},
            }
        }
    ]


def i13_aggregation_npl_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": "$citations.non_patent_literature"},
            }
        }
    ]


def ind_caller(pat, results, extra_aggr_param=[]):
    results["i13"] = {}

    try:
        numerator = uf.secondary_view(
            pat, "appln_filing_year", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "appln_filing_year", i13_aggregation, extra_aggr_param
        )
        results["i13"]["sv01"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv01"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv01"] = None
        print(f"Error calculating i13[sv01]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "topic", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.secondary_view(pat, "topic", i13_aggregation, extra_aggr_param)
        results["i13"]["sv02"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv02"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv02"] = None
        print(f"Error calculating i13[sv02]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "category", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "category", i13_aggregation, extra_aggr_param
        )
        results["i13"]["sv03"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv03"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv03"] = None
        print(f"Error calculating i13[sv03]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.name", i13_aggregation_npl
        )
        denominator = uf.inner_secondary_view(pat, "participant.name", i13_aggregation)
        results["i13"]["sv06"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv06"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv06"] = None
        print(f"Error calculating i13[sv06]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i13_aggregation_npl_nace
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i13_aggregation_nace
        )
        results["i13"]["sv07"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv07"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv07"] = None
        print(f"Error calculating i13[sv07]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.sector", i13_aggregation_npl
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.sector", i13_aggregation
        )
        results["i13"]["sv08"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv08"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv08"] = None
        print(f"Error calculating i13[sv08]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.country", i13_aggregation_npl
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.country", i13_aggregation
        )
        full_set = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            full_set[k] = numerator[k] / denominator[k]

        results["i13"]["sv09"] = {}
        for k in full_set.keys():
            if k in uf.eu_members_code:
                results["i13"]["sv09"][
                    uf.eu_members[uf.eu_members_code.index(k)]
                ] = full_set[k]
    except Exception as e:
        results["i13"]["sv09"] = None
        print(f"Error calculating i13[sv09]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i13_aggregation_npl_cpc, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i13_aggregation_cpc, extra_aggr_param
        )
        results["i13"]["sv13"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i13"]["sv13"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv13"] = None
        print(f"Error calculating i13[sv13]: {str(e)}")

    return results
