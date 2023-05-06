from utils import uf


def i19_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "is_international": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i19"] = {}
    results["i19"]["sv01"] = uf.secondary_view(
        sci, "pub_year", i19_aggregation, extra_aggr_param
    )
    results["i19"]["sv02"] = uf.inner_secondary_view(
        sci, "topic", i19_aggregation, extra_aggr_param
    )
    results["i19"]["sv03"] = uf.secondary_view(
        sci, "category", i19_aggregation, extra_aggr_param
    )
    results["i19"]["sv05"] = uf.sdg_aggregation(sci, i19_aggregation, extra_aggr_param)
    results["i19"]["sv06"] = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i19_aggregation, extra_aggr_param
    )
    full_set = uf.inner_secondary_view(
        sci, "affiliations.country", i19_aggregation, extra_aggr_param
    )
    results["i19"]["sv09"] = {}
    for k in full_set.keys():
        if k in uf.eu_members:
            results["i19"]["sv09"][k] = full_set[k]
    results["i19"]["sv10"] = uf.secondary_view(
        sci, "published_venue", i19_aggregation, uf.journal_filter + extra_aggr_param
    )
    results["i19"]["sv11"] = uf.secondary_view(
        sci, "publisher", i19_aggregation, extra_aggr_param
    )
    results["i19"]["sv12"] = uf.inner_secondary_view(
        sci, "funders.funder", i19_aggregation, extra_aggr_param
    )

    return results
