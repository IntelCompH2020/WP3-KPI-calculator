from utils import uf
import copy


def i13_aggregation(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [{"$group": {"_id": None, "count": {"$sum": 1}}}]
    else:
        return extra_aggr_param + [
            {"$group": {"_id": "$" + field, "count": {"$sum": 1}}}
        ]



def i13_aggregation_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": 1},
            }
        },
    ]


def i13_aggregation_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"cpc_labels": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i13_aggregation_npl(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
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
            "$match": {
                "nace": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.non_patent_literature"},
            }
        },
    ]


def i13_aggregation_npl_cpc(field, extra_aggr_param):
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
                "count": {"$sum": "$citations.non_patent_literature"},
            }
        }
    ]


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i13"] = {}

    try:
        numerator = uf.secondary_view(
            pat, "appln_filing_year", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "appln_filing_year", i13_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv01"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv01"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv01"] = None
        logging.error(f"Error calculating i13[sv01]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "topic", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.secondary_view(pat, "topic", i13_aggregation, extra_aggr_param)
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv02"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv02"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv02"] = None
        logging.error(f"Error calculating i13[sv02]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            pat, "category", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "category", i13_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv03"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv03"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv03"] = None
        logging.error(f"Error calculating i13[sv03]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.name", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.name", i13_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv06"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv06"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv06"] = None
        logging.error(f"Error calculating i13[sv06]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i13_aggregation_npl_nace, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "nace.nace2_code", i13_aggregation_nace, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv07"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv07"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv07"] = None
        logging.error(f"Error calculating i13[sv07]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.sector", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.sector", i13_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv08"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv08"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv08"] = None
        logging.error(f"Error calculating i13[sv08]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            pat, "participant.country", i13_aggregation_npl, extra_aggr_param
        )
        denominator = uf.inner_secondary_view(
            pat, "participant.country", i13_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv09"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv09"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv09"] = None
        logging.error(f"Error calculating i13[sv09]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i13_aggregation_npl_cpc, extra_aggr_param
        )
        denominator = uf.inner_secondary_view_nace_cpc(
            pat, "ipc.ipc_class", i13_aggregation_cpc, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i13"]["sv13"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i13"]["sv13"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i13"]["sv13"] = None
        logging.error(f"Error calculating i13[sv13]: {str(e)}")

    return results
