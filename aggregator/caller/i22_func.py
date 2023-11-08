from caller import i19_func
from caller import i20_func


def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results = i19_func.ind_caller(sci, results, logging, extra_aggr_param, working_path)
    results = i20_func.ind_caller(sci, results, logging, extra_aggr_param, working_path)

    # Remove empty dictionaries from results
    results["i19"] = {k: v for k, v in results["i19"].items() if v}
    results["i20"] = {k: v for k, v in results["i20"].items() if v}

    results["i22"] = {}
    try:
        for sv in results["i20"].keys():
            if sv in results["i19"].keys():
                results["i22"][sv] = {}
                for key in results["i20"][sv].keys():
                    if key in results["i19"][sv].keys() and results["i19"][sv][key] > 0:
                        results["i22"][sv][key] = (
                            results["i20"][sv][key] / results["i19"][sv][key]
                        )

    except Exception as e:
        results["i22"] = None
        print(f"Error calculating i22: {str(e)}")

    return results
