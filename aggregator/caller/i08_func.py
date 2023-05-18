from utils import uf

# from caller import i01_func


def i08_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": "$" + field, "count": {"$sum": "$fwci_score"}}}
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    # results = i01_func.ind_caller(sci, results, extra_aggr_param)

    results["i08"] = {}
    numerator = uf.secondary_view(sci, "pub_year", i08_aggregation, extra_aggr_param)
    denominator = results["i01"]["sv01"]
    results["i08"]["sv01"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv01"][k] = numerator[k] / denominator[k]

    numerator = uf.inner_secondary_view(sci, "topic", i08_aggregation, extra_aggr_param)
    denominator = results["i01"]["sv02"]
    results["i08"]["sv02"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv02"][k] = numerator[k] / denominator[k]

    numerator = uf.secondary_view(sci, "category", i08_aggregation, extra_aggr_param)
    denominator = results["i01"]["sv03"]
    results["i08"]["sv03"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv03"][k] = numerator[k] / denominator[k]

    numerator = uf.sdg_aggregation(sci, i08_aggregation, extra_aggr_param)
    denominator = results["i01"]["sv05"]
    results["i08"]["sv05"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv05"][k] = numerator[k] / denominator[k]

    numerator = uf.inner_secondary_view(
        sci, "affiliations.affiliation_name", i08_aggregation, extra_aggr_param
    )
    denominator = results["i01"]["sv06"]
    results["i08"]["sv06"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv06"][k] = numerator[k] / denominator[k]

    numerator = uf.inner_secondary_view(
        sci, "affiliations.country", i08_aggregation, extra_aggr_param
    )
    denominator = results["i01"]["sv09"]
    results["i08"]["sv09"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv09"][k] = numerator[k] / denominator[k]

    numerator = uf.secondary_view(
        sci, "published_venue", i08_aggregation, uf.journal_filter + extra_aggr_param
    )
    denominator = results["i01"]["sv10"]
    results["i08"]["sv10"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv10"][k] = numerator[k] / denominator[k]

    numerator = uf.secondary_view(sci, "publisher", i08_aggregation, extra_aggr_param)
    denominator = results["i01"]["sv11"]
    results["i08"]["sv11"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv11"][k] = numerator[k] / denominator[k]

    numerator = uf.inner_secondary_view(
        sci, "funders.funder", i08_aggregation, extra_aggr_param
    )
    denominator = results["i01"]["sv12"]
    results["i08"]["sv12"] = {}
    for k in numerator.keys():
        if denominator[k] == 0:
            denominator[k] = 1
        results["i08"]["sv12"][k] = numerator[k] / denominator[k]

    return results
