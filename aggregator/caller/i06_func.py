from utils import uf


def i06_aggregation(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [
            {
                "$match": {
                    "nr_citations": {"$gt": 0},
                }
            },
            {"$group": {"_id": None, "count": {"$sum": "$nr_citations"}}},
        ]
    else:
        return extra_aggr_param + [
            {
                "$match": {
                    "nr_citations": {"$gt": 0},
                }
            },
            {"$group": {"_id": "$" + field, "count": {"$sum": "$nr_citations"}}},
        ]


def ind_caller(sci, results, extra_aggr_param=[], working_path=""):
    results["i06"] = {}

    try:
        results["i06"]["sv00"] = uf.secondary_view(
            sci, "all", i06_aggregation, extra_aggr_param
        )
        results["i06"]["sv00"]["total_publications"] = results["i06"]["sv00"].pop(None)
    except Exception as e:
        results["i06"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i06"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i06"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    try:
        results["i06"]["sv03"] = uf.secondary_view(
            sci, "category", i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv03"] = None
        print(f"Error calculating sv03: {str(e)}")

    try:
        results["i06"]["sv05"] = uf.sdg_aggregation(
            sci, i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv05"] = None
        print(f"Error calculating sv05: {str(e)}")

    try:
        results["i06"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(sci, "affiliations.country", i06_aggregation)
        results["i06"]["sv09"] = {}
        for k in full_set.keys():
            results["i06"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i06"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i06"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i06_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i06"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i06"]["sv11"] = uf.secondary_view(
            sci, "publisher", i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i06"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i06_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i06"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
