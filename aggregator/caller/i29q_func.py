from utils import uf
import copy

def i29q_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nr_citations": {"$gt": 0},
                "fos_prediction.is emerging": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": "$nr_citations"}}},
    ]

def i29q_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$pub_year", "$" + field], "count": {"$sum": 1}}}
    ]


def i29q_aggregation_fos(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nr_citations": {"$gt": 0},
                "fos_prediction.is emerging": {"$eq": True},
            }
        },
    ]

def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i29q"] = {}

    try:
        full_set = uf.inner_secondary_view_per_year(
            sci, "fos_prediction.L5", i29q_aggregation_per_year, copy.deepcopy(extra_aggr_param), first_year=2000
        )
        results["i29q"]["sv25.01"] = {}
        for k in full_set.keys():
            results["i29q"]["sv25.01"][k] = full_set[k]
    except Exception as e:
        results["i29q"]["sv25.01"] = None
        print(f"Error calculating i29q[sv25.01]: {str(e)}")
        logging.error(f"Error calculating i29q[sv25.01]: {str(e)}")

    try:
        results["i29q"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i29q_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29q"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i29q_aggregation, copy.deepcopy(extra_aggr_param)
        )
        results["i29q"]["sv09"] = {}
        for k in full_set.keys():
            results["i29q"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i29q"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i29q"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i29q_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29q"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    try:
        results["i29q"]["sv25"] = uf.inner_secondary_view_pd(
            sci, "emerging_topic", i29q_aggregation_fos, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29q"]["sv25"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
