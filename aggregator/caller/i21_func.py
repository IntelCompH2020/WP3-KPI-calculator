from caller import i01_func
from caller import i20_func


def ind_caller(sci, results, extra_aggr_param=[], working_path=""):
    results = i01_func.ind_caller(sci, results, extra_aggr_param)
    results = i20_func.ind_caller(sci, results, extra_aggr_param)

    # Remove empty dictionaries from results
    results["i01"] = {k: v for k, v in results["i01"].items() if v}
    results["i20"] = {k: v for k, v in results["i20"].items() if v}

    results["i21"] = {}
    try:
        for sv in results["i20"].keys():
            if sv in results["i01"].keys():
                results["i21"][sv] = {}
                for key in results["i20"][sv].keys():
                    if key in results["i01"][sv].keys() and results["i01"][sv][key] > 0:
                        results["i21"][sv][key] = (
                            results["i20"][sv][key] / results["i01"][sv][key]
                        )
    except Exception as e:
        results["i21"] = None
        print(f"Error calculating i21: {str(e)}")

    return results
