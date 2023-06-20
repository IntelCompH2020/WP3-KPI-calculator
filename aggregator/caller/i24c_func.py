from utils import uf


def i24c_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "affiliations.is_eu_member": {"$eq": True},
                "pub_year": {"$gte": 2014},
                "pub_type": {"$eq": "Journal"},
                "is_open_access": {"$eq": True},
            }
        },
        {"$group": {"_id": "$" + field, "count": {"$sum": "$fwci_score"}}},
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i24c"] = {}

    try:
        numerator = uf.secondary_view(
            sci, "pub_year", i24c_aggregation, extra_aggr_param
        )
        denominator = results["i24b"]["sv01"]
        results["i24c"]["sv01"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv01"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv01"] = None
        print(f"Error calculating i24c[sv01]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "topic", i24c_aggregation, extra_aggr_param
        )
        denominator = results["i24b"]["sv02"]
        results["i24c"]["sv02"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv02"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv02"] = None
        print(f"Error calculating i24c[sv02]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            sci, "category", i24c_aggregation, extra_aggr_param
        )
        denominator = results["i24b"]["sv03"]
        results["i24c"]["sv03"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv03"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv03"] = None
        print(f"Error calculating i24c[sv03]: {str(e)}")

    try:
        numerator = uf.sdg_aggregation(sci, i24c_aggregation, extra_aggr_param)
        denominator = results["i24b"]["sv05"]
        results["i24c"]["sv05"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv05"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv05"] = None
        print(f"Error calculating i24c[sv05]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "affiliations.affiliation_name", i24c_aggregation, extra_aggr_param
        )
        denominator = results["i24b"]["sv06"]
        results["i24c"]["sv06"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv06"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv06"] = None
        print(f"Error calculating i24c[sv06]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "affiliations.country", i24c_aggregation, extra_aggr_param
        )
        denominator = results["i24b"]["sv09"]
        results["i24c"]["sv09"] = {}
        for k in numerator.keys():
            if k not in denominator:
                continue
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv09"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv09"] = None
        print(f"Error calculating i24c[sv09]: {str(e)}")

    try:
        numerator = uf.secondary_view(
            sci,
            "published_venue",
            i24c_aggregation,
            uf.journal_filter + extra_aggr_param,
        )
        denominator = results["i24b"]["sv10"]
        results["i24c"]["sv10"] = {}
        for k in numerator.keys():
            if k not in denominator:
                continue
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv10"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv10"] = None
        print(f"Error calculating i24c[sv10]: {str(e)}")

    try:
        numerator = uf.secondary_view(sci, "publisher", i24c_aggregation)
        denominator = results["i24b"]["sv11"]
        results["i24c"]["sv11"] = {}
        for k in numerator.keys():
            if k not in denominator:
                continue
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv11"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv11"] = None
        print(f"Error calculating i24c[sv11]: {str(e)}")

    try:
        numerator = uf.inner_secondary_view(
            sci, "funders.funder", i24c_aggregation, extra_aggr_param
        )
        denominator = results["i24b"]["sv12"]
        results["i24c"]["sv12"] = {}
        for k in numerator.keys():
            if denominator[k] == 0:
                denominator[k] = 1
            results["i24c"]["sv12"][k] = numerator[k] / denominator[k]
    except Exception as e:
        results["i24c"]["sv12"] = None
        print(f"Error calculating i24c[sv12]: {str(e)}")

    return results