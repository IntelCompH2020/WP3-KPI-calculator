from utils import uf


def i02_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$pub_year", "$" + field], "count": {"$sum": 1}}}
    ]


def get_annual_growth_from_per_year(per_year):
    for k, v in per_year.items():
        first_year = list(v.keys())[0]
        final_year = list(v.keys())[-1]
        value_first_year = list(v.values())[0]
        value_final_year = sum(v.values())
        if value_first_year == 0:
            value_first_year = 1
        value = (
            (value_final_year / value_first_year) ** 1 / (final_year - first_year + 1)
        ) - 1
        per_year[k] = value
    return per_year


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i02"] = {}
    temp = uf.inner_secondary_view_per_year(
        sci, "topic", i02_aggregation, extra_aggr_param, first_year=2014
    )
    temp = {k: v for k, v in temp.copy().items() if k is not None and not v == "null"}
    temp = dict(sorted(temp.items()))
    results["i02"]["sv02"] = get_annual_growth_from_per_year(temp)

    temp = uf.secondary_view_per_year(
        sci, "category", i02_aggregation, extra_aggr_param, first_year=2014
    )
    temp = {k: v for k, v in temp.copy().items() if k is not None and not v == "null"}
    temp = dict(sorted(temp.items()))
    results["i02"]["sv03"] = get_annual_growth_from_per_year(temp)

    temp = uf.inner_secondary_view_per_year(
        sci,
        "affiliations.affiliation_name",
        i02_aggregation,
        extra_aggr_param,
        first_year=2014,
    )
    temp = {k: v for k, v in temp.copy().items() if k is not None and not v == "null"}
    temp = dict(sorted(temp.items()))
    results["i02"]["sv06"] = get_annual_growth_from_per_year(temp)

    temp = uf.inner_secondary_view_per_year(
        sci, "affiliations.country", i02_aggregation, extra_aggr_param, first_year=2014
    )
    temp = {k: v for k, v in temp.copy().items() if k is not None and not v == "null"}
    temp = dict(sorted(temp.items()))
    full_set = get_annual_growth_from_per_year(temp)
    results["i02"]["sv09"] = {}
    for k in full_set.keys():
        results["i02"]["sv09"][k] = full_set[k]

    temp = uf.secondary_view_per_year(
        sci,
        "published_venue",
        i02_aggregation,
        uf.journal_filter + extra_aggr_param,
        first_year=2014,
    )
    temp = {k: v for k, v in temp.copy().items() if k is not None and not v == "null"}
    temp = dict(sorted(temp.items()))
    results["i02"]["sv10"] = get_annual_growth_from_per_year(temp)

    temp = uf.secondary_view_per_year(
        sci, "publisher", i02_aggregation, extra_aggr_param, first_year=2014
    )
    temp = {k: v for k, v in temp.copy().items() if k is not None and not v == "null"}
    temp = dict(sorted(temp.items()))
    results["i02"]["sv11"] = get_annual_growth_from_per_year(temp)

    return results
