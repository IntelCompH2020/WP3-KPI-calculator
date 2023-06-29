from caller import i01_func
from caller import i06_func


def ind_caller(sci, results, extra_aggr_param=[], working_path=""):
    results = i01_func.ind_caller(sci, results, extra_aggr_param, working_path)
    results = i06_func.ind_caller(sci, results, extra_aggr_param, working_path)

    # Remove empty dictionaries from results
    results["i01"] = {k: v for k, v in results["i01"].items() if v}
    results["i06"] = {k: v for k, v in results["i06"].items() if v}

    results["i07"] = {}

    for sv in results["i06"].keys():
        if sv in results["i01"].keys():
            results["i07"][sv] = {}
            for key in results["i06"][sv].keys():
                try:
                    if key in results["i01"][sv].keys() and results["i01"][sv][key] > 0:
                        results["i07"][sv][key] = (
                            results["i06"][sv][key] / results["i01"][sv][key]
                        )
                except Exception as e:
                    print(f"Error calculating i07[{sv}][{key}]: {str(e)}")

    return results
