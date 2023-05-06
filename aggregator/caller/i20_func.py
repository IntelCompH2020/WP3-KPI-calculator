from utils import uf


def i20_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "is_international": {"$eq": True},
                "nr_citations": {"$gt": 0},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i20"] = {}
    results["i20"]["sv01"] = uf.secondary_view(
        sci, "pub_year", i20_aggregation, extra_aggr_param
    )

    results["i20"]["sv02"] = uf.inner_secondary_view(
        sci, "topic", i20_aggregation, extra_aggr_param
    )
    results["i20"]["sv03"] = uf.secondary_view(
        sci, "category", i20_aggregation, extra_aggr_param
    )
    results["i20"]["sv05"] = uf.sdg_aggregation(sci, i20_aggregation, extra_aggr_param)
    results["i20"]["sv06"] = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i20_aggregation, extra_aggr_param
    )
    full_set = uf.inner_secondary_view(
        sci, "affiliations.country", i20_aggregation, extra_aggr_param
    )
    results["i20"]["sv09"] = {}
    for k in full_set.keys():
        # if k in eu_members:
        results["i20"]["sv09"][k] = full_set[k]
    results["i20"]["sv10"] = uf.secondary_view(
        sci, "published_venue", i20_aggregation, uf.journal_filter + extra_aggr_param
    )
    results["i20"]["sv11"] = uf.secondary_view(
        sci, "publisher", i20_aggregation, extra_aggr_param
    )
    results["i20"]["sv12"] = uf.inner_secondary_view(
        sci, "funders.funder", i20_aggregation, extra_aggr_param
    )

    return results
