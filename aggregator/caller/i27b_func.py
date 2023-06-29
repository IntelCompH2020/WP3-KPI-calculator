from utils import uf


def i27b_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "affiliations.is_eu_member": {"$eq": True},
                "is_meso_interdisciplinary": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$avg": "$fwci_score"}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[], spark_output=""):
    results["i27b"] = {}

    try:
        results["i27b"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i27b"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    try:
        results["i27b"]["sv03"] = uf.secondary_view(
            sci, "category", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv03"] = None
        print(f"Error calculating sv03: {str(e)}")

    try:
        results["i27b"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    try:
        results["i27b"]["sv09"] = uf.inner_secondary_view(
            sci, "affiliations.country", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i27b"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i27b_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i27b"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i27b"]["sv11"] = uf.secondary_view(
            sci, "publisher", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i27b"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i27b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i27b"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
