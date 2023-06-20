import glob
import json

# /media/datalake/patstat_2021b/output/i13a-energy/


def ind_caller(pat, results, extra_aggr_param=[]):
    results["i13a"] = {}

    try:
        path = "/media/datalake/patstat_2021b/output/i13a-energy/sv00-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv00"] = {}
        for d in data:
            results["i13a"]["sv00"][d["doi"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv00"] = None
        print(f"Error calculating i13a[sv00]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i13a-energy/sv01-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv01"] = {}
        for d in data:
            results["i13a"]["sv01"][d["year"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv01"] = None
        print(f"Error calculating i13a[sv01]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i13a-energy/sv02-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv02"] = {}
        for d in data:
            results["i13a"]["sv02"][d["topic"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv02"] = None
        print(f"Error calculating i13a[sv02]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i13a-energy/sv05-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv05"] = {}
        for d in data:
            results["i13a"]["sv05"][d["sdg_prediction"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv05"] = None
        print(f"Error calculating i13a[sv05]: {str(e)}")

    # try:
    #     path = '/media/datalake/patstat_2021b/output/i13a-Agrofood/sv09-EU/'
    #     json_files = glob.glob(path + '*.json')
    #     data = []
    #     with open(json_files[0], 'r') as f:
    #         for line in f:
    #             data.append(json.loads(line))
    #     results["i13a"]["sv09"] = {}
    #     print(data[0])
    #     for d in data:
    #         results['i13a']['sv09'][d['country']] = d['patents_citing']
    # except Exception as e:
    #     results["i13a"]["sv09"] = None
    #     print(f"Error calculating i13a[sv09]: {str(e)}")

    # try:
    #     path = '/media/datalake/patstat_2021b/output/i13a-Agrofood/sv10-EU/'
    #     json_files = glob.glob(path + '*.json')
    #     data = []
    #     with open(json_files[0], 'r') as f:
    #         for line in f:
    #             data.append(json.loads(line))
    #     results["i13a"]["sv10"] = {}
    #     print(data[0])
    #     for d in data:
    #         results['i13a']['sv10'][d['cpc']] = d['patents_citing']
    # except Exception as e:
    #     results["i13a"]["sv10"] = None
    #     print(f"Error calculating i13a[sv10]: {str(e)}")

    return results
