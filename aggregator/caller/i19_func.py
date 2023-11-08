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


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i19"] = {}
    print(extra_aggr_param)
    try:
        results["i19"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv01"] = None
        print(f"Error calculating i19[sv01]: {str(e)}")

    try:
        results["i19"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv02"] = None
        print(f"Error calculating i19[sv02]: {str(e)}")

    try:
        results["i19"]["sv03"] = uf.secondary_view(
            sci, "category", i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv03"] = None
        print(f"Error calculating i19[sv03]: {str(e)}")

    try:
        results["i19"]["sv05"] = uf.sdg_aggregation(
            sci, i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv05"] = None
        print(f"Error calculating i19[sv05]: {str(e)}")

    try:
        results["i19"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv06"] = None
        print(f"Error calculating i19[sv06]: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i19_aggregation, extra_aggr_param
        )
        results["i19"]["sv09"] = {}
        for k in full_set.keys():
            # if k in uf.eu_members:
                results["i19"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i19"]["sv09"] = None
        print(f"Error calculating i19[sv09]: {str(e)}")

    try:
        results["i19"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i19_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i19"]["sv10"] = None
        print(f"Error calculating i19[sv10]: {str(e)}")

    try:
        results["i19"]["sv11"] = uf.secondary_view(
            sci, "publisher", i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv11"] = None
        print(f"Error calculating i19[sv11]: {str(e)}")

    try:
        results["i19"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i19_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i19"]["sv12"] = None
        print(f"Error calculating i19[sv12]: {str(e)}")

    return results
