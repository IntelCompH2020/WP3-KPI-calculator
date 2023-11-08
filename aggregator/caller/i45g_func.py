import pandas as pd

def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):

    results['i45g'] = {}
    
    path = "/home/gkou/dev/aggregator/aggregator/utils/green_skills_excel/greenskills.ods"

    # Read Excel file into a pandas DataFrame
    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, "i45g")

    results['i45g']['sv24'] = df.set_index('key')['value'].to_dict()

    return results
