def ind_caller(sci, results):
    results["i21"] = {}
    for sv in results["i20"].keys():
        if sv in results["i01"].keys():
            results["i21"][sv] = {}
            for key in results["i20"][sv].keys():
                if key in results["i01"][sv].keys() and results["i01"][sv][key] > 0:
                    results["i21"][sv][key] = (
                        results["i20"][sv][key] / results["i01"][sv][key]
                    )

    return results
