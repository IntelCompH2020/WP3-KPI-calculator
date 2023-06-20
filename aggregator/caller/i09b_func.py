from utils import uf


def i09b_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$appln_filing_year", "$" + field], "count": {"$sum": 1}}}
    ]


def ind_caller(pat, results, extra_aggr_param=[]):
    results["i09b"] = {}

    try:
        res = uf.inner_secondary_view_per_year(
            pat, "topic", i09b_aggregation_per_year, extra_aggr_param
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
                if total[i] == 0:
                    total[i] = 1
                res[topic][i] /= total[i]

        results["i09b"]["sv02"] = res

    except Exception as e:
        results["i09b"]["sv02"] = None
        print(f"Error calculating i09b[sv02]: {str(e)}")

    return results
