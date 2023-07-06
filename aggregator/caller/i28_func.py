from utils import uf


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
    results["i28"]["sv00"] = {}

    temp = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i28_aggregation
    )
    for key in temp.keys():
        if temp[key] != 0:
            results["i28"]["sv00"][key] = temp[key]
        results["i28"]["sv00"]

    return results
