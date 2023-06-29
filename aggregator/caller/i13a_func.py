import glob
import json

# from utils import uf

# /media/datalake/patstat_2021b/output/i13a-energy/


def ind_caller(pat, results, extra_aggr_param=[], working_path=""):
    results["i13a"] = {}

    # dg = uf.dg
    # pv = uf.pv
    # if dg == "dg01" and pv == "pv01":
    #     path = "/media/datalake/patstat_2022b/output/i13a-Energy/"
    #     space = "-EU/"
    # elif dg == "dg02" and pv == "pv01":
    #     path = "/media/datalake/patstat_2022b/output/i13a-Energy/"
    #     space = "-GR/"
    # elif dg == "dg01" and pv == "pv02":
    #     path = "/media/datalake/patstat_2022b/output/i13a-Agrifood/"
    #     space = "-EU/"
    # elif dg == "dg02" and pv == "pv02":
    #     path = "/media/datalake/patstat_2022b/output/i13a-Agrifood/"
    #     space = "-GR/"

    path = working_path + "i13a/"
    space = ""

    try:
        sv_path = path + "sv00" + space
        json_files = glob.glob(sv_path + "*.json")
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
        sv_path = path + "sv01" + space
        json_files = glob.glob(sv_path + "*.json")
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
        sv_path = path + "sv02" + space
        json_files = glob.glob(sv_path + "*.json")
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
        sv_path = path + "sv05" + space
        json_files = glob.glob(sv_path + "*.json")
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

    try:
        sv_path = path + "sv09" + space
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv09"] = {}
        for d in data:
            results["i13a"]["sv09"][d["affiliation_country_iso_code"]] = d[
                "patents_citing"
            ]
    except Exception as e:
        results["i13a"]["sv09"] = None
        print(f"Error calculating i13a[sv09]: {str(e)}")

    try:
        sv_path = path + "sv10" + space
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv10"] = {}
        for d in data:
            results["i13a"]["sv10"][d["published_venue"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv10"] = None
        print(f"Error calculating i13a[sv10]: {str(e)}")

    return results
