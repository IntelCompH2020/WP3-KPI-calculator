from caller import i29_func
from caller import i29q_func
import copy


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results = i29_func.ind_caller(sci, results, logging, copy.deepcopy(extra_aggr_param), working_path)
    results = i29q_func.ind_caller(sci, results, logging, copy.deepcopy(extra_aggr_param), working_path)

    # Remove empty dictionaries from results
    results["i29"] = {k: v for k, v in results["i29"].items() if v}
    results["i29q"] = {k: v for k, v in results["i29q"].items() if v}

    results["i29a"] = {}

    for sv in results["i29q"].keys():
        if sv in results["i29"].keys():
            results["i29a"][sv] = {}
            for key in results["i29q"][sv].keys():
                try:
                    if key in results["i29"][sv].keys() and results["i29"][sv][key] > 0:
                        results["i29a"][sv][key] = (
                            results["i29q"][sv][key] / results["i29"][sv][key]
                        )
                except Exception as e:
                    print(f"Error calculating i29a[{sv}][{key}]: {str(e)}")

    return results
