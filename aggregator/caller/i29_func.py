from utils import uf
import copy


# Top countries by number of publications in emerging topics (i29_sv09) (Done)
# Funders by number of publications in emerging topics (i29_sv12) (Done)

def i29_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "fos_prediction.is emerging": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]

def i29_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$pub_year", "$" + field], "count": {"$sum": 1}}}
    ]

def i29_aggregation_fos(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "fos_prediction.is emerging": {"$eq": True},
            }
        },
    ]


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i29"] = {}

    try:
        full_set = uf.inner_secondary_view_pd_per_year(
            sci, "emerging_topic", i29_aggregation_fos, copy.deepcopy(extra_aggr_param), first_year=2000
        )
        results["i29"]["sv25.01"] = {}
        for k in full_set.keys():
            results["i29"]["sv25.01"][k] = full_set[k]
    except Exception as e:
        results["i29"]["sv25.01"] = None
        print(f"Error calculating i29[sv25.01]: {str(e)}")
        logging.error(f"Error calculating i29[sv25.01]: {str(e)}")

    try:
        results["i29"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i29_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i29"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i29_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i29_aggregation, copy.deepcopy(extra_aggr_param)
        )
        results["i29"]["sv09"] = {}
        for k in full_set.keys():
            results["i29"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i29"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i29"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i29_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    try:
        results["i29"]["sv25"] = uf.inner_secondary_view_pd(
            sci, "emerging_topic", i29_aggregation_fos, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i29"]["sv25"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
