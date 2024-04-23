import pandas as pd

def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):

    results['i45b'] = {}
    
    path = "utils/green_skills_excel/greenskills.ods"

    # Read Excel file into a pandas DataFrame
    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, "i45b")

    results['i45b']['sv01'] = df.set_index('key')['value'].to_dict()

    return results
