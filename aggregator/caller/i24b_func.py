from utils import uf


def i24b_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "affiliations.is_eu_member": {"$eq": True},
                "is_open_access": {"$eq": True},
                "nr_citations": {"$gt": 0},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": 1}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[], spark_output=""):
    results["i24b"] = {}

    try:
        results["i24b"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i24b_aggregation, uf.journal_filter + extra_aggr_param
        )
    except Exception as e:
        results["i24b"]["sv01"] = None
        print(f"Error calculating i24b[sv01]: {str(e)}")

    try:
        results["i24b"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i24b_aggregation, uf.journal_filter + extra_aggr_param
        )
    except Exception as e:
        results["i24b"]["sv02"] = None
        print(f"Error calculating i24b[sv02]: {str(e)}")

    try:
        results["i24b"]["sv03"] = uf.secondary_view(
            sci, "category", i24b_aggregation, uf.journal_filter + extra_aggr_param
        )
    except Exception as e:
        results["i24b"]["sv03"] = None
        print(f"Error calculating i24b[sv03]: {str(e)}")

    try:
        results["i24b"]["sv05"] = uf.sdg_aggregation(
            sci, i24b_aggregation, uf.journal_filter + extra_aggr_param
        )
    except Exception as e:
        results["i24b"]["sv05"] = None
        print(f"Error calculating i24b[sv05]: {str(e)}")

    try:
        results["i24b"]["sv06"] = uf.inner_secondary_view(
            sci,
            "affiliations.affiliation_name",
            i24b_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i24b"]["sv06"] = None
        print(f"Error calculating i24b[sv06]: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci,
            "affiliations.country",
            i24b_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
        results["i24b"]["sv09"] = {}
        for k in full_set.keys():
            if k in uf.eu_members:
                results["i24b"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i24b"]["sv09"] = None
        print(f"Error calculating i24b[sv09]: {str(e)}")

    try:
        results["i24b"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i24b_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i24b"]["sv10"] = None
        print(f"Error calculating i24b[sv10]: {str(e)}")

    try:
        results["i24b"]["sv11"] = uf.secondary_view(
            sci, "publisher", i24b_aggregation, uf.journal_filter + extra_aggr_param
        )
    except Exception as e:
        results["i24b"]["sv11"] = None
        print(f"Error calculating i24b[sv11]: {str(e)}")

    try:
        results["i24b"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i24b_aggregation, uf.journal_filter
        )
    except Exception as e:
        results["i24b"]["sv12"] = None
        print(f"Error calculating i24b[sv12]: {str(e)}")

    return results
