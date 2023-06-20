import glob
import json


def ind_caller(pat, results, extra_aggr_param=[]):
    results["i12a"] = {}

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv00-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv00"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv00"]:
                results["i12a"]["sv00"][d["tag"]] = {}
            results["i12a"]["sv00"][d["tag"]][d["priority_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv00"] = None
        print(f"Error calculating i12a[sv00]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv01-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv01"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv01"]:
                results["i12a"]["sv01"][d["tag"]] = {}
            results["i12a"]["sv01"][d["tag"]][d["priority_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv01"] = None
        print(f"Error calculating i12a[sv01]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv02-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv02"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv02"]:
                results["i12a"]["sv02"][d["tag"]] = {}
            results["i12a"]["sv02"][d["tag"]][d["topic"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv02"] = None
        print(f"Error calculating i12a[sv02]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv06-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv06"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv06"]:
                results["i12a"]["sv06"][d["tag"]] = {}
            results["i12a"]["sv06"][d["name"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv06"] = None
        print(f"Error calculating i12a[sv06]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv07-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv07"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv07"]:
                results["i12a"]["sv07"][d["tag"]] = {}
            results["i12a"]["sv07"][d["nace2_code"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv07"] = None
        print(f"Error calculating i12a[sv07]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv08-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv08"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv08"]:
                results["i12a"]["sv08"][d["tag"]] = {}
            results["i12a"]["sv08"][d["sector"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv08"] = None
        print(f"Error calculating i12a[sv08]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv09-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv09"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv09"]:
                results["i12a"]["sv09"][d["tag"]] = {}
            results["i12a"]["sv09"][d["country"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv09"] = None
        print(f"Error calculating i12a[sv09]: {str(e)}")

    try:
        path = "/media/datalake/patstat_2021b/output/i12a-Energy/sv13-EU/"
        json_files = glob.glob(path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv13"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv13"]:
                results["i12a"]["sv13"][d["tag"]] = {}
            results["i12a"]["sv13"][d["cpc"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv13"] = None
        print(f"Error calculating i12a[sv13]: {str(e)}")

    return results
