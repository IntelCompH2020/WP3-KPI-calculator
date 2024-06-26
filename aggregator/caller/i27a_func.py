from utils import uf

import copy

def i27a_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "is_meso_interdisciplinary": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i27a"] = {}

    try:
        results["i27a"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i27a"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    try:
        results["i27a"]["sv03"] = uf.secondary_view(
            sci, "category", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv03"] = None
        print(f"Error calculating sv03: {str(e)}")

    try:
        results["i27a"]["sv05"] = uf.sdg_aggregation(
            sci, i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv05"] = None
        print(f"Error calculating sv05: {str(e)}")

    try:
        results["i27a"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
        results["i27a"]["sv09"] = {}
        for k in full_set.keys():
            results["i27a"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i27a"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i27a"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i27a_aggregation,
            uf.journal_filter + copy.deepcopy(extra_aggr_param),
        )
    except Exception as e:
        results["i27a"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i27a"]["sv11"] = uf.secondary_view(
            sci, "publisher", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i27a"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i27a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27a"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
