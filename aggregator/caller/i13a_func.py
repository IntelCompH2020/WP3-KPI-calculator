import glob
import json

import copy
from utils import uf


def i13a_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "patent_cited": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]

def i13a_emerging_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "patent_cited": {"$eq": True},
                "fos_prediction.is emerging": {"$eq": True},
            }
        },
    ]



def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i13a"] = {}


    try:
        results["i13a"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i13a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")


    try:
        results["i13a"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i13a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")


    try:
        results["i13a"]["sv05"] = uf.sdg_aggregation(
            sci, i13a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv05"] = None
        print(f"Error calculating sv05: {str(e)}")

    try:
        results["i13a"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i13a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")


    full_set = uf.inner_secondary_view(
        sci, "affiliations.country", i13a_aggregation, copy.deepcopy(extra_aggr_param)
    )
    results["i13a"]["sv09"] = {}
    for k in full_set.keys():
        results["i13a"]["sv09"][k] = full_set[k]

    try:
        results["i13a"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i13a_aggregation,
            uf.journal_filter + copy.deepcopy(extra_aggr_param),
        )
    except Exception as e:
        results["i13a"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i13a"]["sv11"] = uf.secondary_view(
            sci, "publisher", i13a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i13a"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i13a_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    try:
        results["i13a"]["sv25"] = uf.inner_secondary_view_pd(
            sci, "emerging_topic", i13a_emerging_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i13a"]["sv25"] = None
        print(f"Error calculating sv25: {str(e)}")

    return results

