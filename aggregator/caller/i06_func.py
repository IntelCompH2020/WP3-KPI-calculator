from utils import uf


def i06_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nr_citations": {"$gt": 0},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": "$nr_citations"}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i06"] = {}
    results["i06"]["sv01"] = uf.secondary_view(
        sci, "pub_year", i06_aggregation, extra_aggr_param
    )
    results["i06"]["sv02"] = uf.inner_secondary_view(
        sci, "topic", i06_aggregation, extra_aggr_param
    )
    results["i06"]["sv03"] = uf.secondary_view(
        sci, "category", i06_aggregation, extra_aggr_param
    )
    results["i06"]["sv05"] = uf.sdg_aggregation(sci, i06_aggregation, extra_aggr_param)
    results["i06"]["sv06"] = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i06_aggregation, extra_aggr_param
    )
    full_set = uf.inner_secondary_view(sci, "affiliations.country", i06_aggregation)
    results["i06"]["sv09"] = {}
    for k in full_set.keys():
        results["i06"]["sv09"][k] = full_set[k]
    results["i06"]["sv10"] = uf.secondary_view(
        sci, "published_venue", i06_aggregation, uf.journal_filter + extra_aggr_param
    )
    results["i06"]["sv11"] = uf.secondary_view(
        sci, "publisher", i06_aggregation, extra_aggr_param
    )
    results["i06"]["sv12"] = uf.inner_secondary_view(
        sci, "funders.funder", i06_aggregation, extra_aggr_param
    )

    return results
