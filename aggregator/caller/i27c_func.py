from caller import i27a_func
from caller import i27aa_func
import copy


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results = i27a_func.ind_caller(sci, results, logging, copy.deepcopy(extra_aggr_param), working_path)
    results = i27aa_func.ind_caller(sci, results, logging, copy.deepcopy(extra_aggr_param), working_path)

    # Remove empty dictionaries from results
    results["i27a"] = {k: v for k, v in results["i27a"].items() if v}
    results["i27aa"] = {k: v for k, v in results["i27aa"].items() if v}

    results["i27c"] = {}

    for sv in results["i27aa"].keys():
        if sv in results["i27a"].keys():
            results["i27c"][sv] = {}
            for key in results["i27aa"][sv].keys():
                try:
                    if key in results["i27a"][sv].keys() and results["i27a"][sv][key] > 0:
                        results["i27c"][sv][key] = (
                            results["i27aa"][sv][key] / results["i27a"][sv][key]
                        )
                except Exception as e:
                    print(f"Error calculating i27c[{sv}][{key}]: {str(e)}")

    return results
