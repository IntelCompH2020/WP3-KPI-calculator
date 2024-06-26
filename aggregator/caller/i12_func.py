from utils import uf
import copy


def i12_aggregation(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [{"$group": {"_id": None, "count": {"$sum": 1}}}]
    else:
        return extra_aggr_param + [
            {"$group": {"_id": "$" + field, "count": {"$sum": 1}}}
        ]


def i12_aggregation_forward(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [
            {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
            {"$group": {"_id": None, "count": {"$sum": "$citations.forward"}}},
        ]
    else:
        return extra_aggr_param + [
            {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
            {"$group": {"_id": "$" + field, "count": {"$sum": "$citations.forward"}}},
        ]


def i12_aggregation_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": 1},
            }
        },
    ]


def i12_aggregation_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"cpc_labels": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i12_aggregation_forward_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nace": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.forward"},
            }
        },
    ]


def i12_aggregation_forward_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "cpc_labels": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": "$citations.forward"},
            }
        },
    ]


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i12"] = {}

    try:
        numerator = uf.secondary_view(
            pat, "all", i12_aggregation_forward, extra_aggr_param
        ).pop(None)
        denominator = uf.secondary_view(
            pat, "all", i12_aggregation, extra_aggr_param
        ).pop(None)

        average_FC = numerator/denominator
        results["i12"]["sv00"] = {}
        results["i12"]["sv00"]["average_forward_citations"] = average_FC
    except Exception as e:
        results["i12"]["sv00"] = None
        logging.error(f"Error calculating i12[sv00]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "appln_filing_year", i12_aggregation_forward, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "appln_filing_year", i12_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv01"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv01"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv01"] = None
        logging.error(f"Error calculating i12[sv01]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "topic", i12_aggregation_forward, extra_aggr_param
        )
        denominator = uf.secondary_view(pat, "topic", i12_aggregation, extra_aggr_param)
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv02"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv02"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv02"] = None
        logging.error(f"Error calculating i12[sv02]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "category", i12_aggregation_forward, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "category", i12_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv03"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv03"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv03"] = None
        logging.error(f"Error calculating i12[sv03]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.name", i12_aggregation_forward
        )
        denominator = uf.inner_secondary_view(pat, "participant.name", i12_aggregation)
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv06"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv06"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv06"] = None
        logging.error(f"Error calculating i12[sv06]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i12_aggregation_forward_nace, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i12_aggregation_nace, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv07"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv07"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv07"] = None
        logging.error(f"Error calculating i12[sv07]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.sector", i12_aggregation_forward, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.sector", i12_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv08"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv08"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv08"] = None
        logging.error(f"Error calculating i12[sv08]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.country", i12_aggregation_forward, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.country", i12_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv09"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv09"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv09"] = None
        logging.error(f"Error calculating i12[sv09]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i12_aggregation_forward_cpc, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i12_aggregation_cpc, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i12"]["sv13"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i12"]["sv13"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i12"]["sv13"] = None
        logging.error(f"Error calculating i12[sv13]: {str(e)}")

    return results
