from utils import uf
from caller import i01_func


def i08_aggregation(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [
            {"$group": {"_id": None, "count": {"$sum": "$fwci_score"}}}
        ]
    else:
        return extra_aggr_param + [
            {"$group": {"_id": "$" + field, "count": {"$sum": "$fwci_score"}}}
        ]


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results = i01_func.ind_caller(sci, results, extra_aggr_param, working_path)
    results["i08"] = {}

    try:
        numerator = uf.secondary_view(sci, "all", i08_aggregation, extra_aggr_param)
        numerator["total_publications"] = numerator.pop(None)
        denominator = results["i01"]["sv00"]
        results["i08"]["sv00"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv00"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv00"] = None
        print(f"Error calculating i08[sv00]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            sci, "pub_year", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv01"]
        results["i08"]["sv01"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv01"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv01"] = None
        print(f"Error calculating i08[sv01]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "topic", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv02"]
        results["i08"]["sv02"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv02"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv02"] = None
        print(f"Error calculating i08[sv02]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            sci, "category", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv03"]
        results["i08"]["sv03"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv03"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv03"] = None
        print(f"Error calculating i08[sv03]: {str(e)}")

    try:
        numerator = uf.sdg_aggregation(sci, i08_aggregation, extra_aggr_param)
        denominator = results["i01"]["sv05"]
        results["i08"]["sv05"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv05"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv05"] = None
        print(f"Error calculating i08[sv05]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv06"]
        results["i08"]["sv06"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv06"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv06"] = None
        print(f"Error calculating i08[sv06]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "affiliations.country", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv09"]
        results["i08"]["sv09"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv09"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv09"] = None
        print(f"Error calculating i08[sv09]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            sci,
            "published_venue",
            i08_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
        denominator = results["i01"]["sv10"]
        results["i08"]["sv10"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv10"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv10"] = None
        print(f"Error calculating i08[sv10]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            sci, "publisher", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv11"]
        results["i08"]["sv11"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i08"]["sv11"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv11"] = None
        print(f"Error calculating i08[sv11]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "funders.funder", i08_aggregation, extra_aggr_param
        )
        denominator = results["i01"]["sv12"]
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        results["i08"]["sv12"] = {}
        for k in numerator.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            results["i08"]["sv12"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i08"]["sv12"] = None
        print(f"Error calculating i08[sv12]: {str(e)}")

    return results
