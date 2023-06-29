import glob
import json

# from utils import uf


def ind_caller(pat, results, extra_aggr_param=[], working_path=""):
    results["i12a"] = {}

    # dg = uf.dg
    # pv = uf.pv
    # if dg == "dg01" and pv == "pv01":
    #     path = "/media/datalake/patstat_2022b/output/i12a-Energy/"
    #     space = "-EU/"
    # elif dg == "dg02" and pv == "pv01":
    #     path = "/media/datalake/patstat_2022b/output/i12a-Energy/"
    #     space = "-GR/"
    # elif dg == "dg01" and pv == "pv02":
    #     path = "/media/datalake/patstat_2022b/output/i12a-Agrifood/"
    #     space = "-EU/"
    # elif dg == "dg02" and pv == "pv02":
    #     path = "/media/datalake/patstat_2022b/output/i12a-Agrifood/"
    #     space = "-GR/"

    path = working_path + "i12a/"
    space = ""

    try:
        sv_path = path + "sv00" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv00-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv00"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv00"]:
                results["i12a"]["sv00"][d["tag"]] = {}
            results["i12a"]["sv00"][d["tag"]][d["appln_filing_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv00"] = None
        print(f"Error calculating i12a[sv00]: {str(e)}")

    try:
        sv_path = path + "sv01" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv01-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv01"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv01"]:
                results["i12a"]["sv01"][d["tag"]] = {}
            results["i12a"]["sv01"][d["tag"]][d["appln_filing_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv01"] = None
        print(f"Error calculating i12a[sv01]: {str(e)}")

    try:
        sv_path = path + "sv02" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv02-EU/"
        json_files = glob.glob(sv_path + "*.json")
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
        sv_path = path + "sv06" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv06-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv06"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv06"]:
                results["i12a"]["sv06"][d["tag"]] = {}
            results["i12a"]["sv06"][d["tag"]][d["name"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv06"] = None
        print(f"Error calculating i12a[sv06]: {str(e)}")

    try:
        sv_path = path + "sv07" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv07-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv07"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv07"]:
                results["i12a"]["sv07"][d["tag"]] = {}
            results["i12a"]["sv07"][d["tag"]][d["nace"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv07"] = None
        print(f"Error calculating i12a[sv07]: {str(e)}")

    try:
        sv_path = path + "sv08" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv08-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv08"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv08"]:
                results["i12a"]["sv08"][d["tag"]] = {}
            try:
                results["i12a"]["sv08"][d["tag"]][d["sector"]] = d["count"]
            except Exception as e:
                print(f"Error calculating i12a[sv08]: {str(e)}")
    except Exception as e:
        results["i12a"]["sv08"] = None
        print(f"Error calculating i12a[sv08]: {str(e)}")

    try:
        sv_path = path + "sv09" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv09-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv09"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv09"]:
                results["i12a"]["sv09"][d["tag"]] = {}
            results["i12a"]["sv09"][d["tag"]][d["country"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv09"] = None
        print(f"Error calculating i12a[sv09]: {str(e)}")

    try:
        sv_path = path + "sv13" + space
        # path = "/media/datalake/stiviewer/temp/i12a/sv13-EU/"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i12a"]["sv13"] = {}
        for d in data:
            if d["tag"] not in results["i12a"]["sv13"]:
                results["i12a"]["sv13"][d["tag"]] = {}
            results["i12a"]["sv13"][d["tag"]][d["classification"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv13"] = None
        print(f"Error calculating i12a[sv13]: {str(e)}")

    return results
