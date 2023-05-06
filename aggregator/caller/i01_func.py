from utils import uf


def i01_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i01"] = {}

    results["i01"]["sv01"] = uf.secondary_view(
        sci, "pub_year", i01_aggregation, extra_aggr_param
    )
    results["i01"]["sv02"] = uf.inner_secondary_view(
        sci, "topic", i01_aggregation, extra_aggr_param
    )
    results["i01"]["sv03"] = uf.secondary_view(
        sci, "category", i01_aggregation, extra_aggr_param
    )
    results["i01"]["sv05"] = uf.sdg_aggregation(sci, i01_aggregation, extra_aggr_param)
    results["i01"]["sv06"] = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i01_aggregation, extra_aggr_param
    )
    full_set = uf.inner_secondary_view(
        sci, "affiliations.country", i01_aggregation, extra_aggr_param
    )
    results["i01"]["sv09"] = {}
    for k in full_set.keys():
        results["i01"]["sv09"][k] = full_set[k]
    results["i01"]["sv10"] = uf.secondary_view(
        sci, "published_venue", i01_aggregation, uf.journal_filter + extra_aggr_param
    )
    results["i01"]["sv11"] = uf.secondary_view(
        sci, "publisher", i01_aggregation, extra_aggr_param
    )
    results["i01"]["sv12"] = uf.inner_secondary_view(
        sci, "funders.funder", i01_aggregation, extra_aggr_param
    )

    return results
