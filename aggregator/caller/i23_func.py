from utils import uf


def i23_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "is_open_access": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i23"] = {}
    results["i23"]["sv01"] = uf.secondary_view(
        sci, "pub_year", i23_aggregation, extra_aggr_param
    )
    results["i23"]["sv02"] = uf.inner_secondary_view(
        sci, "topic", i23_aggregation, extra_aggr_param
    )
    results["i23"]["sv03"] = uf.secondary_view(
        sci, "category", i23_aggregation, extra_aggr_param
    )
    results["i23"]["sv05"] = uf.sdg_aggregation(sci, i23_aggregation, extra_aggr_param)
    results["i23"]["sv06"] = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i23_aggregation, extra_aggr_param
    )
    full_set = uf.inner_secondary_view(
        sci, "affiliations.country", i23_aggregation, extra_aggr_param
    )
    results["i23"]["sv09"] = {}
    for k in full_set.keys():
        if k in uf.eu_members:
            results["i23"]["sv09"][k] = full_set[k]
    results["i23"]["sv10"] = uf.secondary_view(
        sci, "published_venue", i23_aggregation, uf.journal_filter + extra_aggr_param
    )
    results["i23"]["sv11"] = uf.secondary_view(
        sci, "publisher", i23_aggregation, extra_aggr_param
    )
    results["i23"]["sv12"] = uf.inner_secondary_view(
        sci, "funders.funder", i23_aggregation, extra_aggr_param
    )

    return results
