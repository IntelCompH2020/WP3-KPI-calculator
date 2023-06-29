from utils import uf


def i03_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i03_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$pub_year", "$" + field], "count": {"$sum": 1}}}
    ]


def ind_caller(sci, results, extra_aggr_param=[], working_path=""):
    results["i03"] = {}

    try:
        res = uf.inner_secondary_view_per_year(
            sci,
            "topic",
            i03_aggregation_per_year,
            extra_aggr_param,
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

        results["i03"]["sv02"] = res

    except Exception as e:
        results["i03"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    return results
