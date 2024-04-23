from utils import uf
import copy


def i28_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "affiliations.is_eu_member": {"$eq": True},
                "is_meso_interdisciplinary": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": "$nr_citations"}}},
    ]


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i28"] = {}
    results["i28"]["sv06"] = {}

    temp = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i28_aggregation, copy.deepcopy(extra_aggr_param)
    )
    for key in temp.keys():
        if temp[key] != 0:
            results["i28"]["sv06"][key] = temp[key]
        results["i28"]["sv06"]

    try:
        results["i28"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i28_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i28"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")


    return results
