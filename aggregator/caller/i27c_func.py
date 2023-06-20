from caller import i27a_func
from caller import i27b_func


def ind_caller(sci, results, extra_aggr_param=[]):
    results = i27a_func.ind_caller(sci, results, extra_aggr_param)
    results = i27b_func.ind_caller(sci, results, extra_aggr_param)

    # Remove empty dictionaries from results
    results["i27a"] = {k: v for k, v in results["i27a"].items() if v}
    results["i27b"] = {k: v for k, v in results["i27b"].items() if v}

    results["i27c"] = {}

    for sv in results["i27b"].keys():
        if sv in results["i27a"].keys():
            results["i27c"][sv] = {}
            for key in results["i27b"][sv].keys():
                try:
                    if (
                        key in results["i27a"][sv].keys()
                        and results["i27a"][sv][key] > 0
                    ):
                        results["i27c"][sv][key] = (
                            results["i27b"][sv][key] / results["i27a"][sv][key]
                        )
                except Exception as e:
                    print(f"Error calculating i27c[{sv}][{key}]: {str(e)}")

    return results
