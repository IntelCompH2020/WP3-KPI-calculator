import glob
import json
import os

from utils import uf
import copy


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i12a"] = {}

    dg = uf.dg
    pv = uf.pv
    if dg == "dg01" and pv == "pv01":
        path = "/media/datalake/patstat_2022b/output/i12a-Energy/"
        space = "-EU/"
    elif dg == "dg02" and pv == "pv01":
        path = "/media/datalake/patstat_2022b/output/i12a-Energy/"
        space = "-GR/"
    elif dg == "dg01" and pv == "pv02":
        path = "/media/datalake/patstat_2022b/output/i12a-Agrifood/"
        space = "-EU/"
    elif dg == "dg02" and pv == "pv02":
        path = "/media/datalake/patstat_2022b/output/i12a-Agrifood/"
        space = "-GR/"


    try:
        results["i12a"]["sv00"] = {}
        sv_path = path + f"sv00{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv00"]:
                results["i12a"]["sv00"][d["tag"]] = {}
            results["i12a"]["sv00"][d["tag"]][d["appln_filing_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv00"] = None
        logging.error(f"Error calculating i12a[sv00]: {str(e)}")

    try:
        results["i12a"]["sv01"] = {}
        sv_path = path + f"sv01{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv01"]:
                results["i12a"]["sv01"][d["tag"]] = {}
            results["i12a"]["sv01"][d["tag"]][d["appln_filing_year"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv01"] = None
        logging.error(f"Error calculating i12a[sv01]: {str(e)}")

    try:
        results["i12a"]["sv02"] = {}
        sv_path = path + f"sv02{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv02"]:
                results["i12a"]["sv02"][d["tag"]] = {}
            results["i12a"]["sv02"][d["tag"]][d["topic"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv02"] = None
        logging.error(f"Error calculating i12a[sv02]: {str(e)}")

    try:
        results["i12a"]["sv06"] = {}
        sv_path = path + f"sv06{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv06"]:
                results["i12a"]["sv06"][d["tag"]] = {}
            results["i12a"]["sv06"][d["tag"]][d["name"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv06"] = None
        logging.error(f"Error calculating i12a[sv06]: {str(e)}")

    try:
        results["i12a"]["sv07"] = {}
        sv_path = path + f"sv07{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv07"]:
                results["i12a"]["sv07"][d["tag"]] = {}
            results["i12a"]["sv07"][d["tag"]][d["nace"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv07"] = None
        logging.error(f"Error calculating i12a[sv07]: {str(e)}")

    try:
        results["i12a"]["sv08"] = {}
        sv_path = path + f"sv08{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv08"]:
                results["i12a"]["sv08"][d["tag"]] = {}
            try:
                results["i12a"]["sv08"][d["tag"]][d["sector"]] = d["count"]
            except Exception as e:
                print(f"Error calculating i12a[sv08]: {str(e)}")
    except Exception as e:
        results["i12a"]["sv08"] = None
        logging.error(f"Error calculating i12a[sv08]: {str(e)}")

    try:
        results["i12a"]["sv09"] = {}
        sv_path = path + f"sv09{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv09"]:
                results["i12a"]["sv09"][d["tag"]] = {}
            results["i12a"]["sv09"][d["tag"]][d["country"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv09"] = None
        logging.error(f"Error calculating i12a[sv09]: {str(e)}")

    try:
        results["i12a"]["sv13"] = {}
        sv_path = path + f"sv13{space}"
        if os.path.exists(sv_path):
            json_files = glob.glob(sv_path + "*.json")
        else:
            raise FileNotFoundError(f"Directory does not exist: {sv_path}")
        json_files = glob.glob(sv_path + "*.json")
        data = []
        with open(json_files[0]) as f:
            for line in f:
                data.append(json.loads(line))
        for d in data:
            if d["tag"] not in results["i12a"]["sv13"]:
                results["i12a"]["sv13"][d["tag"]] = {}
            results["i12a"]["sv13"][d["tag"]][d["classification"]] = d["count"]
    except Exception as e:
        results["i12a"]["sv13"] = None
        logging.error(f"Error calculating i12a[sv13]: {str(e)}")

    return results


