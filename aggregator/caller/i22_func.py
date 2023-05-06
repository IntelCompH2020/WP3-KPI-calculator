def ind_caller(sci, results):
    results["i22"] = {}
    for sv in results["i20"].keys():
        if sv in results["i19"].keys():
            results["i22"][sv] = {}
            for key in results["i20"][sv].keys():
                if key in results["i19"][sv].keys() and results["i19"][sv][key] > 0:
                    results["i22"][sv][key] = (
                        results["i20"][sv][key] / results["i19"][sv][key]
                    )

    return results
