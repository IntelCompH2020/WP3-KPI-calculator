import pandas as pd
import os
import logging
import json
from utils import uf


def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):

    dg = uf.dg
    pv = uf.pv
    if dg == "dg01" and pv == "pv01":
        path = "regulations/EU_energy.json"
    elif dg == "dg02" and pv == "pv01":
        path = "regulations/Greece_energy.json"
    elif dg == "dg01" and pv == "pv02":
        path = "regulations/EU_agriculture.json"
    elif dg == "dg02" and pv == "pv02":
        path = "regulations/Greece_agriculture.json"
        
    # Load a json into a pandas dataframe
    df = pd.read_json(path)

    # Display the dataframe
    results["i44"] = {}

    results["i44"]["sv00"] = {"total_regulations" : len(df)}
    results["i44"]["sv01"] = df.groupby('year')['id'].count().to_dict()
    results["i44"]["sv19"] = df.groupby('type_of_regulation')['id'].count().to_dict()
    results["i44"]["sv02"] = df.explode('topic').groupby('topic')['id'].count().to_dict()
    results["i44"]["sv03"] = df.explode('category').groupby('category')['id'].count().to_dict()
    results["i44"]["sv06"] = df.explode('organization').groupby('organization')['id'].count().to_dict()

    return results
