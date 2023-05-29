from utils import uf


def i03_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i03_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$pub_year", "$" + field], "count": {"$sum": 1}}}
    ]


def ind_caller(sci, results, extra_aggr_param=[]):
    results["i03"] = {}
    try:
        res = uf.inner_secondary_view_per_year(
            sci,
            "topic",
            i03_aggregation_per_year,
            extra_aggr_param,
        )
        total = {}
        for topic in res.keys():
            for i in res[topic].keys():
                if i not in total.keys():
                    total[i] = res[topic][i]
                else:
                    total[i] += res[topic][i]
        for topic in res.keys():
            for i in res[topic].keys():
                res[topic][i] /= total[i]
        results["i03"]["sv02"] = res
    except Exception as e:
        results["i03"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    return results
