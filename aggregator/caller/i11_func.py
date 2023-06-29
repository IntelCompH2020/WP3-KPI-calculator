from utils import uf


def i11_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i11_aggregation_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": 1},
            }
        },
    ]


def i11_aggregation_ipc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i11_aggregation_backward(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": "$" + field, "count": {"$sum": "$citations.backward"}}}
    ]


def i11_aggregation_backward_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.backward"},
            }
        },
    ]


def i11_aggregation_backward_ipc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": "$citations.backward"},
            }
        }
    ]


def ind_caller(pat, results, extra_aggr_param=[], spark_output=""):
    results["i11"] = {}

    try:
        numerator = uf.secondary_view(
            pat, "appln_filing_year", i11_aggregation_backward, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "appln_filing_year", i11_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv01"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv01"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv01"] = None
        print(f"Error calculating i11[sv01]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "topic", i11_aggregation_backward, extra_aggr_param
        )
        denominator = uf.secondary_view(pat, "topic", i11_aggregation, extra_aggr_param)
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv02"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv02"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv02"] = None
        print(f"Error calculating i11[sv02]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "category", i11_aggregation_backward, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "category", i11_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv03"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv03"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv03"] = None
        print(f"Error calculating i11[sv03]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.name", i11_aggregation_backward, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.name", i11_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv06"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv06"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv06"] = None
        print(f"Error calculating i11[sv06]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i11_aggregation_backward_nace, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i11_aggregation_nace, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv07"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv07"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv07"] = None
        print(f"Error calculating i11[sv07]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.sector", i11_aggregation_backward, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.sector", i11_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv08"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv08"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv08"] = None
        print(f"Error calculating i11[sv08]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.country", i11_aggregation_backward, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.country", i11_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv09"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv09"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv09"] = None
        print(f"Error calculating i11[sv09]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i11_aggregation_backward_ipc, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i11_aggregation_ipc, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i11"]["sv13"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i11"]["sv13"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i11"]["sv13"] = None
        print(f"Error calculating i11[sv13]: {str(e)}")

    return results
