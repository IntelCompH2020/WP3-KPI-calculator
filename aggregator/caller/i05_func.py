from utils import uf


def i05_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nr_citations": {"$gt": 0},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i05"] = {}

    try:
        results["i05"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i05"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    try:
        results["i05"]["sv03"] = uf.secondary_view(
            sci, "category", i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv03"] = None
        print(f"Error calculating sv03: {str(e)}")

    try:
        results["i05"]["sv05"] = uf.sdg_aggregation(
            sci, i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv05"] = None
        print(f"Error calculating sv05: {str(e)}")

    try:
        results["i05"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i05_aggregation, extra_aggr_param
        )
        results["i05"]["sv09"] = {}
        for k in full_set.keys():
            results["i05"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i05"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i05"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i05_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i05"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i05"]["sv11"] = uf.secondary_view(
            sci, "publisher", i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i05"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i05_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i05"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
