from utils import uf


def i01_aggregation(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [{"$group": {"_id": None, "count": {"$sum": 1}}}]
    else:
        return extra_aggr_param + [
            {"$group": {"_id": "$" + field, "count": {"$sum": 1}}}
        ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i01"] = {}

    try:
        results["i01"]["sv00"] = uf.secondary_view(
            sci, "all", i01_aggregation, extra_aggr_param
        )
        results["i01"]["sv00"]["total_publications"] = results["i01"]["sv00"].pop(None)
    except Exception as e:
        results["i01"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i01"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i01"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    try:
        results["i01"]["sv03"] = uf.secondary_view(
            sci, "category", i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv03"] = None
        print(f"Error calculating sv03: {str(e)}")

    try:
        results["i01"]["sv05"] = uf.sdg_aggregation(
            sci, i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv05"] = None
        print(f"Error calculating sv05: {str(e)}")

    try:
        results["i01"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    full_set = uf.inner_secondary_view(
        sci, "affiliations.country", i01_aggregation, extra_aggr_param
    )
    results["i01"]["sv09"] = {}
    for k in full_set.keys():
        results["i01"]["sv09"][k] = full_set[k]

    try:
        results["i01"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i01_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i01"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i01"]["sv11"] = uf.secondary_view(
            sci, "publisher", i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i01"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i01_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i01"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
