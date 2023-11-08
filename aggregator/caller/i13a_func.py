import glob
import json

from utils import uf

# /media/datalake/patstat_2021b/output/i13a-energy/


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i13a"] = {}

    dg = uf.dg
    pv = uf.pv
    if dg == "dg01" and pv == "pv01":
        path = "/media/datalake/patstat_2022b/output/i13a-Energy/"
        space = "-EU/"
    elif dg == "dg02" and pv == "pv01":
        path = "/media/datalake/patstat_2022b/output/i13a-Energy/"
        space = "-GR/"
    elif dg == "dg01" and pv == "pv02":
        path = "/media/datalake/patstat_2022b/output/i13a-Agrifood/"
        space = "-EU/"
    elif dg == "dg02" and pv == "pv02":
        path = "/media/datalake/patstat_2022b/output/i13a-Agrifood/"
        space = "-GR/"

    # path = working_path + "i13a/"

    try:
        sv_path = path + f"sv00{space}"
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv00"] = {}
        total = 0
        for d in data:
            total += d["patents_citing"]
        results["i13a"]["sv00"]["total_publications"] = total
    except Exception as e:
        results["i13a"]["sv00"] = None
        logging.error(f"Error calculating i13a[sv00]: {str(e)}")

    try:
        sv_path = path + f"sv01{space}"
        print(sv_path)
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv01"] = {}
        for d in data:
            if d["year"] < 2014:
                continue
            results["i13a"]["sv01"][d["year"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv01"] = None
        logging.error(f"Error calculating i13a[sv01]: {str(e)}")

    try:
        sv_path = path + f"sv02{space}"
        print(sv_path)
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
        logging.error(f"Error calculating i13a[sv02]: {str(e)}")

    try:
        sv_path = path + f"sv05{space}"
        print(sv_path)
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
        logging.error(f"Error calculating i13a[sv05]: {str(e)}")

    try:
        sv_path = path + f"sv06{space}"
        print(sv_path)
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv06"] = {}
        for d in data:
            results["i13a"]["sv06"][d["company"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv06"] = None
        logging.error(f"Error calculating i13a[sv06]: {str(e)}")

    try:
        sv_path = path + f"sv09{space}"
        print(sv_path)
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
        logging.error(f"Error calculating i13a[sv09]: {str(e)}")

    try:
        sv_path = path + f"sv10{space}"
        print(sv_path)
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
        logging.error(f"Error calculating i13a[sv10]: {str(e)}")

    try:
        sv_path = path + f"sv12{space}"
        print(sv_path)
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        results["i13a"]["sv12"] = {}
        for d in data:
            results["i13a"]["sv12"][d["funder"]] = d["patents_citing"]
    except Exception as e:
        results["i13a"]["sv12"] = None
        logging.error(f"Error calculating i13a[sv12]: {str(e)}")

    return results
