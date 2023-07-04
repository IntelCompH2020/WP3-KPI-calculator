from utils import uf


def i09b_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$appln_filing_year", "$" + field], "count": {"$sum": 1}}}
    ]


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i09b"] = {}

    try:
        res = uf.inner_secondary_view_per_year(
            pat, "topic", i09b_aggregation_per_year, extra_aggr_param
        )
        new_res = {}
        for key, sub_dict in res.items():
            new_sub_dict = {k: v for k, v in sub_dict.items() if v != 0}
            new_res[key] = new_sub_dict

        total = {}
        for topic in new_res.keys():
            for i in new_res[topic].keys():
                if i not in total.keys():
                    total[i] = new_res[topic][i]
                else:
                    total[i] += new_res[topic][i]

        for topic in new_res.keys():
            for i in new_res[topic].keys():
                new_res[topic][i] /= total[i]

        results["i09b"]["sv02"] = new_res

    except Exception as e:
        results["i09b"]["sv02"] = None
        logging.error(f"Error calculating i09b[sv02]: {str(e)}")

    return results
