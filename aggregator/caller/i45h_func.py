import pandas as pd

def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):

    results['i45h'] = {}
    
    path = "utils/green_skills_excel/greenskills.ods"

    # Read Excel file into a pandas DataFrame
    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, "i45h")
    # Convert DataFrame to desired dictionary format
    result_dict = {
        category: group.set_index('key')['value'].to_dict()
        for category, group in df.groupby('region')
    }

    results['i45h']['sv24'] = result_dict

    return results
