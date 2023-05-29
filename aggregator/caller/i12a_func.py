import glob
import json


def ind_caller(pat, results, extra_aggr_param=[]):
    results["i12a"] = {}

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv00-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv00"] = {}
        for d in data:
            results["i12a"]["sv00"][d["priority_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv00"] = None
        print(f"Error calculating i12a[sv00]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv01-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv01"] = {}
        for d in data:
            results["i12a"]["sv01"][d["priority_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv01"] = None
        print(f"Error calculating i12a[sv01]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv02-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv02"] = {}
        for d in data:
            results["i12a"]["sv02"][d["topic"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv02"] = None
        print(f"Error calculating i12a[sv02]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv06-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv06"] = {}
        for d in data:
            results["i12a"]["sv06"][d["name"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv06"] = None
        print(f"Error calculating i12a[sv06]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv07-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv07"] = {}
        for d in data:
            results["i12a"]["sv07"][d["nace2_code"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv07"] = None
        print(f"Error calculating i12a[sv07]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv08-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv08"] = {}
        for d in data:
            results["i12a"]["sv08"][d["sector"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv08"] = None
        print(f"Error calculating i12a[sv08]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv09-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv09"] = {}
        for d in data:
            results["i12a"]["sv09"][d["country"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv09"] = None
        print(f"Error calculating i12a[sv09]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Agrofood/sv13-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv13"] = {}
        for d in data:
            results["i12a"]["sv13"][d["cpc"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv13"] = None
        print(f"Error calculating i12a[sv13]: {str(e)}")

    return results
