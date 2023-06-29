from utils import uf


def i23b_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "is_published_open_access": {"$eq": True},
                "pub_type": {"$eq": "Journal"},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[], spark_output=""):
    results["i23b"] = {}

    try:
        results["i23b"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv01"] = None
        print(f"Error calculating i23b[sv01]: {str(e)}")

    try:
        results["i23b"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv02"] = None
        print(f"Error calculating i23b[sv02]: {str(e)}")

    try:
        results["i23b"]["sv03"] = uf.secondary_view(
            sci, "category", i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv03"] = None
        print(f"Error calculating i23b[sv03]: {str(e)}")

    try:
        results["i23b"]["sv05"] = uf.sdg_aggregation(
            sci, i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv05"] = None
        print(f"Error calculating i23b[sv05]: {str(e)}")

    try:
        results["i23b"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv06"] = None
        print(f"Error calculating i23b[sv06]: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i23b_aggregation, extra_aggr_param
        )
        results["i23b"]["sv09"] = {}
        for k in full_set.keys():
            if k in uf.eu_members:
                results["i23b"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i23b"]["sv09"] = None
        print(f"Error calculating i23b[sv09]: {str(e)}")

    try:
        results["i23b"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i23b_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i23b"]["sv10"] = None
        print(f"Error calculating i23b[sv10]: {str(e)}")

    try:
        results["i23b"]["sv11"] = uf.secondary_view(
            sci, "publisher", i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv11"] = None
        print(f"Error calculating i23b[sv11]: {str(e)}")

    try:
        results["i23b"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i23b_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i23b"]["sv12"] = None
        print(f"Error calculating i23b[sv12]: {str(e)}")

    return results
