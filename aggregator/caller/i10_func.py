from caller import i09_func


def get_3_year_average_growth(data):
    min_year = min(data.keys())
    max_year = max(data.keys())
    res = {}
    for i in range(min_year + 2, max_year + 1):
        first_year = i - 2
        final_year = i
        for year in range(final_year, final_year - 2, -1):
            if year not in data:
                data[year] = 0
        value_first_year = data[first_year]
        value_final_year = sum(
            data[year] for year in range(final_year, final_year - 2, -1)
        )
        if value_first_year == 0:
            value_first_year = 1
        res[i] = ((value_final_year / value_first_year) ** (1 / 3)) - 1
    return res


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results = i09_func.ind_caller(pat, results, extra_aggr_param)
    results["i10"] = {}

    try:
        results["i10"]["sv01"] = get_3_year_average_growth(results["i09"]["sv01"])

        for sv in results["i09"].keys():
            if sv == "sv01":
                continue
            results["i10"][sv] = {}
            for value in results["i09"][sv]:
                results["i10"][sv][value] = get_3_year_average_growth(
                    results["i09"][sv][value]
                )

    except Exception as e:
        results["i10"] = None
        logging.error(f"Error calculating i10: {str(e)}")

    return results
