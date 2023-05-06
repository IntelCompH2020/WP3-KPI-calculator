def ind_caller(sci, results):
    results["i07"] = {}
    for sv in results["i06"].keys():
        if sv in results["i01"].keys():
            results["i07"][sv] = {}
            for key in results["i06"][sv].keys():
                if key in results["i01"][sv].keys() and results["i01"][sv][key] > 0:
                    results["i07"][sv][key] = (
                        results["i06"][sv][key] / results["i01"][sv][key]
                    )
    return results
