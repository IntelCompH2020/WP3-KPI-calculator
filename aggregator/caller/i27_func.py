from utils import uf
import copy


def i27_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "affiliations.is_eu_member": {"$eq": True},
                "is_meso_interdisciplinary": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i27"] = {}

    results["i27"]["sv06"] = {}

    temp = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i27_aggregation, copy.deepcopy(extra_aggr_param)
    )
    for key in temp.keys():
        if temp[key] != 0:
            results["i27"]["sv06"][key] = temp[key]
        results["i27"]["sv06"]

    return results
