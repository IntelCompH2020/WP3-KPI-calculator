from utils import uf


def i24_aggregation(field, extra_aggr_param):
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


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i24"] = {}

    try:
        results["i24"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv01"] = None
        print(f"Error calculating i24[sv01]: {str(e)}")

    try:
        results["i24"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv02"] = None
        print(f"Error calculating i24[sv02]: {str(e)}")

    try:
        results["i24"]["sv03"] = uf.secondary_view(
            sci, "category", i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv03"] = None
        print(f"Error calculating i24[sv03]: {str(e)}")

    try:
        results["i24"]["sv05"] = uf.sdg_aggregation(
            sci, i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv05"] = None
        print(f"Error calculating i24[sv05]: {str(e)}")

    try:
        results["i24"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv06"] = None
        print(f"Error calculating i24[sv06]: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i24_aggregation, extra_aggr_param
        )

        results["i24"]["sv09"] = {}
        for k in full_set.keys():
            if k in uf.eu_members:
                results["i24"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i24"]["sv09"] = None
        print(f"Error calculating i24[sv09]: {str(e)}")

    try:
        results["i24"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i24_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
    except Exception as e:
        results["i24"]["sv10"] = None
        print(f"Error calculating i24[sv10]: {str(e)}")

    try:
        results["i24"]["sv11"] = uf.secondary_view(
            sci, "publisher", i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv11"] = None
        print(f"Error calculating i24[sv11]: {str(e)}")

    try:
        results["i24"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i24_aggregation, extra_aggr_param
        )
    except Exception as e:
        results["i24"]["sv12"] = None
        print(f"Error calculating i24[sv12]: {str(e)}")

    return results
