from utils import uf
import copy


def i27aa_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nr_citations": {"$gt": 0},
                "is_meso_interdisciplinary": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": "$nr_citations"}}},
    ]


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i27aa"] = {}

    try:
        results["i27aa"]["sv01"] = uf.secondary_view(
            sci, "pub_year", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv01"] = None
        print(f"Error calculating sv01: {str(e)}")

    try:
        results["i27aa"]["sv02"] = uf.inner_secondary_view(
            sci, "topic", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    try:
        results["i27aa"]["sv03"] = uf.secondary_view(
            sci, "category", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv03"] = None
        print(f"Error calculating sv03: {str(e)}")

    try:
        results["i27aa"]["sv05"] = uf.sdg_aggregation(
            sci, i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv05"] = None
        print(f"Error calculating sv05: {str(e)}")

    try:
        results["i27aa"]["sv06"] = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv06"] = None
        print(f"Error calculating sv06: {str(e)}")

    try:
        full_set = uf.inner_secondary_view(
            sci, "affiliations.country", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
        results["i27aa"]["sv09"] = {}
        for k in full_set.keys():
            results["i27aa"]["sv09"][k] = full_set[k]
    except Exception as e:
        results["i27aa"]["sv09"] = None
        print(f"Error calculating sv09: {str(e)}")

    try:
        results["i27aa"]["sv10"] = uf.secondary_view(
            sci,
            "published_venue",
            i27aa_aggregation,
            uf.journal_filter + copy.deepcopy(extra_aggr_param),
        )
    except Exception as e:
        results["i27aa"]["sv10"] = None
        print(f"Error calculating sv10: {str(e)}")

    try:
        results["i27aa"]["sv11"] = uf.secondary_view(
            sci, "publisher", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv11"] = None
        print(f"Error calculating sv11: {str(e)}")

    try:
        results["i27aa"]["sv12"] = uf.inner_secondary_view(
            sci, "funders.funder", i27aa_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i27aa"]["sv12"] = None
        print(f"Error calculating sv12: {str(e)}")

    return results
