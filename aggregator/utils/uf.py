
import pandas as pd
from collections import Counter

eu_members = [
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czechia",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
]

eu_members_code = [
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
]

# HARD CODED
dg = "dg01"
pv = "pv01"
# HARD CODED

journal_filter = [{"$match": {"pub_type": {"$eq": "Journal"}}}]

pat_organisations = [
    {"$match": {"participant.sector": {"$nin": ["", "INDIVIDUAL", "UNKNOWN"]}}}
]

def secondary_view(col, field, aggregation, extra_aggr_param=[]):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for i in agg:
        result[i["_id"]] = i["count"]

    return result


def secondary_view_comp(col, field, aggregation, extra_aggr_param=[]):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for i in agg:
        result[i["_id"][0]] = i["count"]

    return result


def secondary_view_comp_nace(col, field, aggregation, extra_aggr_param=[]):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for i in agg:
        nace_codes = i["_id"][0]
        nace_labels = i["_id"][1]
        result[f"{nace_codes}:{nace_labels}"] = i["count"]

    return result


def inner_secondary_view_per_year_nace_cpc(
    col, field, aggregation, extra_aggr_param=[], first_year=2014, final_year=2021
):
    
    codes = col.distinct(field[0])
    descriptions = col.distinct(field[1])

    sv_values = []
    code_dict = {}
    for code, description in zip(codes, descriptions):
        code_dict[code] = description
        sv_values.append(f"{code}: {description}")
    # print("Distinct " + field + " values :", len(sv_values))
    year_range = list(range(first_year, final_year + 1))  # sci.distinct("pub_year")
    # print("Year Range:", len(year_range))

    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for key in sv_values:
        result[key] = {}
        for year in year_range:
            result[key][year] = 0

    for i in agg:
        year, keys = i["_id"]
        if type(keys) == str:
            if year >= first_year and year <= final_year and keys in codes:
                result[f"{keys}: {code_dict[keys]}"][year] += i["count"]
            continue
        for key in set(keys):
            # remove set() if we want each individual affiliation to count
            # print("Year and Key:", year, " - ", key)
            if year >= first_year and year <= final_year and key in codes:
                result[f"{key}: {code_dict[key]}"][year] += i["count"]

    return result


def inner_secondary_view_per_year(
    col, field, aggregation, extra_aggr_param=[], first_year=2014, final_year=2021
):
    
    codes = col.distinct(field[0])
    descriptions = col.distinct(field[1])

    sv_values = []
    code_dict = {}
    for code, description in zip(codes, descriptions):
        code_dict[code] = description
        sv_values.append(f"{code}: {description}")
    # print("Distinct " + field + " values :", len(sv_values))
    year_range = list(range(first_year, final_year + 1))  # sci.distinct("pub_year")
    # print("Year Range:", len(year_range))

    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for key in sv_values:
        result[key] = {}
        for year in year_range:
            result[key][year] = 0

    for i in agg:
        year, keys = i["_id"]
        if type(keys) == str:
            if year >= first_year and year <= final_year and keys in codes:
                result[f"{keys}: {code_dict[keys]}"][year] += i["count"]
            continue
        for key in set(keys):
            # remove set() if we want each individual affiliation to count
            # print("Year and Key:", year, " - ", key)
            if year >= first_year and year <= final_year and key in codes:
                result[f"{key}: {code_dict[key]}"][year] += i["count"]

    return result


def inner_secondary_view(col, field, aggregation, extra_aggr_param=[]):

    first_part = field.split('.')[0]
    extra_aggr_param[0]['$match'][first_part] = {"$exists": True, "$not": {"$size": 0}}
    
    sv_values = col.distinct(field)

    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for x in sv_values:
        result[x] = 0
    for i in agg:
        for x in set(
            i["_id"]
        ):  # remove set() if we want each individual affiliation to count
            if x in sv_values:
                result[x] += i["count"]

    return result

def extract_values(row, field):
    """Extract values from a list of dictionaries based on a given field."""
    _values = []
    for item in row:
        if field in item:
            _values.append(item[field])
    return _values


def inner_secondary_view_pd(col, field, aggregation, extra_aggr_param=[]):

    first_part = field.split('.')[0]
    
    sv_values = col.distinct(field)
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    df = pd.DataFrame(list(agg))
    df[field] = df['fos_prediction'].apply(lambda row: extract_values(row, first_part))
    lists = df[field].tolist()
    flattened_list = [item for sublist in lists for item in sublist if item is not None]
    # Count the occurrences of each value
    value_counts = Counter(flattened_list)

    result = {}
    for x in value_counts:
        result[x] = value_counts[x]
    return result

def inner_secondary_view_nace(col, field, aggregation, extra_aggr_param=[]):
    
    codes = col.distinct(field[0])
    descriptions = col.distinct(field[1])

    sv_values = []
    code_dict = {}
    for code, description in zip(codes, descriptions):
        code_dict[code] = description
        sv_values.append(f"{code}: {description}")
    agg = col.aggregate(aggregation(field[0], extra_aggr_param), allowDiskUse=True)

    result = {}
    for x in sv_values:
        result[x] = 0
    for i in agg:
        for x in set(
            i["_id"]
        ):  # remove set() if we want each individual affiliation to count
            if x in codes:
                result[f"{x}: {code_dict[x]}"] += i["count"]

    return result

def secondary_view_per_year(
    col, field, aggregation, extra_aggr_param=[], first_year=2000, final_year=2021
):
    sv_values = col.distinct(field)
    # print("Distinct " + field + " values:", len(sv_values))
    year_range = list(range(first_year, final_year + 1))  # sci.distinct("pub_year")
    # print("Year Range:", len(year_range))

    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for key in sv_values:
        result[key] = {}
        for year in year_range:
            result[key][year] = 0

    for i in agg:
        year, key = i["_id"]
        result[key][year] = i["count"]

    return result


def secondary_view_per_company(
    col, field, aggregation, extra_aggr_param=[], first_year=2000, final_year=2021
):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}

    for i in agg:
        companies, sec_view = i["_id"]
        if not companies:
            continue
        if type(sec_view) != str:
            sec_view = str(sec_view)
        # print(companies, sec_view)
        for company in companies:
            if company + "||" + sec_view not in result.keys():
                result[company + "||" + sec_view] = i["count"]
            else:
                result[company + "||" + sec_view] += i["count"]

    return result

# Function to flatten a list of lists and count unique values, excluding None
def count_unique_values_exclude_none(list_of_lists):
    # Flatten the list of lists and exclude None
    flattened = [item for sublist in list_of_lists for item in sublist if item is not None]
    # Count unique values
    return dict(Counter(flattened))


def inner_secondary_view_pd_per_year(
    col, field, aggregation, extra_aggr_param=[], first_year=2014, final_year=2021
):
    sv_values = col.distinct(field)
    # print("Distinct " + field + " values :", len(sv_values))
    year_range = list(range(first_year, final_year + 1))  # sci.distinct("pub_year")
    # print("Year Range:", len(year_range))
    
    first_part = field.split('.')[0]

    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    df = pd.DataFrame(list(agg))

    df[field] = df['fos_prediction'].apply(lambda row: extract_values(row, first_part))

    grouped = df.groupby('pub_year')[field].apply(list).to_dict()

    # Apply the function to each key in the grouped dictionary
    counted_values_per_year = {year: count_unique_values_exclude_none(values) for year, values in grouped.items()}

    return counted_values_per_year

def inner_secondary_view_per_year_science(
    col, field, aggregation, extra_aggr_param=[], first_year=2014, final_year=2021
):
    # Unwinding the 'field' if it's an array, else process it as a value
    aggregation_pipeline = [
        {'$unwind': '$' + field},
        *aggregation(field, extra_aggr_param)
    ]

    agg = col.aggregate(aggregation_pipeline, allowDiskUse=True)

    sv_values = set()  # Using set to avoid duplicate values
    year_range = list(range(first_year, final_year + 1))

    for doc in col.find({}, {field: 1}):
        val = doc.get(field)
        if isinstance(val, list):
            sv_values.update(val)
        else:
            sv_values.add(val)

    result = {}
    for key in sv_values:
        result[key] = {}
        for year in year_range:
            result[key][year] = 0

    for i in agg:
        year, keys = i["_id"]
        if type(keys) == str:
            if year >= first_year and year <= final_year and keys in sv_values:
                result[keys][year] += i["count"]
            continue
        for key in set(keys):
            # remove set() if we want each individual affiliation to count
            # print("Year and Key:", year, " - ", key)
            if year >= first_year and year <= final_year and key in sv_values:
                result[key][year] += i["count"]

    return result


def inner_secondary_view_per_company(
    col, field, aggregation, extra_aggr_param=[], first_year=2000, final_year=2022
):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}

    for i in agg:
        companies, sec_view = i["_id"]
        if not companies or not sec_view:
            continue
        # print(companies, sec_view)
        for company in companies:
            for sec in sec_view:
                if not sec:
                    continue
                if company + "||" + sec not in result.keys():
                    result[company + "||" + sec] = i["count"]
                else:
                    result[company + "||" + sec] += i["count"]

    return result


def inner_secondary_view_nace_cpc(col, field, aggregation, extra_aggr_param=[]):
    # Get the results of the MongoDB aggregation
    agg = col.aggregate(aggregation(None, extra_aggr_param), allowDiskUse=True)
    intermediate_results = []
    for i in agg:
        nace_codes = i["_id"][0]
        nace_labels = i["_id"][1]
        count = i["count"]
        for i in range(len(nace_codes)):
            output_dict = {field: f"{nace_codes[i]}:{nace_labels[i]}", "count": count}
            intermediate_results.append(output_dict)

    # Group the intermediate results by year and NACE/CPC code
    output_dict = {}
    for item in intermediate_results:
        key = f"{item[field]}"
        if key not in output_dict:
            output_dict[key] = {field: item[field], "count": item["count"]}
        else:
            output_dict[key]["count"] += count
    intermediate_results = list(output_dict.values())

    # Reformat the intermediate results into the desired output format
    temp = {}
    for result in intermediate_results:
        if result[field] not in temp:
            temp[result[field]] = {}
        temp[result[field]] = result["count"]
    intermediate_results = temp

    return intermediate_results


def ranking_secondary_view(col, field, aggregation, extra_aggr_param=[]):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for i in agg:
        my_dict = {
            item["company_name"]: item["revenue_by_employee"] for item in i["count"]
        }
        result[i["_id"]] = my_dict

    return result


def inner_secondary_view_nace_cpc_companies(
    col, field, aggregation, extra_aggr_param=[]
):
    # Get the results of the MongoDB aggregation
    agg = col.aggregate(aggregation(None, extra_aggr_param), allowDiskUse=True)
    mongo_results = []
    for i in agg:
        mongo_results.append(i)

    # Reformat the MongoDB results into an intermediate format
    intermediate_results = []
    for i in mongo_results:
        nace_codes = i["_id"][0]
        nace_labels = i["_id"][1]
        count = i["count"]
        if nace_codes is not None:
            output_dict = {field: f"{nace_codes}:{nace_labels}", "count": count}
            intermediate_results.append(output_dict)

    # Group the intermediate results by year and NACE/CPC code
    output_dict = {}
    for item in intermediate_results:
        key = f"{item[field]}"
        if key not in output_dict:
            output_dict[key] = {field: item[field], "count": item["count"]}
        else:
            output_dict[key]["count"] += count
    intermediate_results = list(output_dict.values())

    # Reformat the intermediate results into the desired output format
    temp = {}
    for result in intermediate_results:
        if result[field] not in temp:
            temp[result[field]] = {}
        temp[result[field]] = result["count"]
    intermediate_results = temp

    return intermediate_results


def sdg_aggregation(sci, aggregation, extra_aggr_param=[]):
    # lookup = [
    #     {
    #         "$lookup": {
    #             "from": "SDGs_scientific",
    #             "localField": "doi",
    #             "foreignField": "doi",
    #             "as": "sdg",
    #         }
    #     },
    #     {"$set": {"sdg": {"$arrayElemAt": ["$sdg.SDGs", 0]}}},
    # ]

    agg = sci.aggregate(aggregation("sdg", extra_aggr_param), allowDiskUse=True)

    result = {}
    sv_values = set()
    for i in agg:
        if i["_id"]:
            for x in i["_id"]:
                sv_values.add(x)
                if x in result:
                    result[x] += i["count"]
                else:
                    result[x] = i["count"]

    result = {
        k: result[k]
        for k in sorted(list(result.keys()), key=lambda x: int(x.split(".")[0]))
    }

    return result


def secondary_view_per_publisher(col, field, aggregation, extra_aggr_param=[]):
    agg = col.aggregate(aggregation(field, extra_aggr_param))

    result = {}
    for i in agg:
        result[i["_id"][1]] = i["_id"][0]

    return result


def top_companies_nace(col, field, aggregation, number_of_comp, extra_aggr_param=[]):
    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for i in agg:
        result_dict = {}
        for data in i["count"]:
            keys = []
            for key in i["count"][0]:
                keys.append(key)
            result_dict[data[keys[0]]] = data[keys[1]]
        sorted_d = dict(sorted(result_dict.items(), key=lambda x: x[1]))
        # Keep only the top 10 items
        top = dict(list(sorted_d.items())[-number_of_comp:])
        nace_codes = i["_id"][0]
        nace_labels = i["_id"][1]
        result[f"{nace_codes}:{nace_labels}"] = top
    return result


def top_companies(col, field, aggregation, number_of_comp, extra_aggr_param=[]):
    mongo_agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for agg in mongo_agg:
        result_dict = {}
        for data in agg["count"]:
            keys = []
            for key in agg["count"][0]:
                keys.append(key)
            if len(keys) > 2:
                data_list = []
                for j in range(len(keys) - 1):
                    data_list.append(data[keys[j + 1]])
                result_dict[data[keys[0]]] = data_list
                sorted_d = dict(sorted(result_dict.items(), key=lambda x: x[1]))
                # Keep only the top 10 items
                top = dict(list(sorted_d.items())[-number_of_comp:])
                top = {k: len(set(v[1])) for k, v in top.items()}
                # top = {k: len(v[2]) for k, v in top.items()}
            else:
                result_dict[data[keys[0]]] = data[keys[1]]
                sorted_d = dict(sorted(result_dict.items(), key=lambda x: x[1]))

                # Keep only the top 10 items
                top = dict(list(sorted_d.items())[-number_of_comp:])
        result[agg["_id"][0]] = top
    return result


def ESG_secondary_view(col, field, aggregation, extra_aggr_param=[]):
    sv_values = col.distinct(field)
    # print("Distinct " + field + " values:", len(sv_values))

    agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    dict_counts = {}
    for i in agg:
        for sv in sv_values:
            dict_counts[sv] = i["count"][0].count(sv)
        result[i["_id"]] = dict_counts
    return result


def top_companies_submetric(
    col, field, aggregation, number_of_comp, extra_aggr_param=[]
):
    mongo_agg = col.aggregate(aggregation(field, extra_aggr_param), allowDiskUse=True)

    result = {}
    for agg in mongo_agg:
        result_dict = {}
        for data in agg["count"]:
            keys = []
            for key in agg["count"][0]:
                keys.append(key)
            if len(keys) > 2:
                data_list = []
                for j in range(len(keys) - 1):
                    data_list.append(data[keys[j + 1]])
                result_dict[data[keys[0]]] = data_list
                sorted_d = dict(sorted(result_dict.items(), key=lambda x: x[1]))
                # Keep only the top 10 items
                top = dict(list(sorted_d.items())[-number_of_comp:])
                # top = {k: [len(set(v[1])), v[2]] for k, v in top.items()}
                top = {k: [len(set(v[1])), v[2]] for k, v in top.items()}
            else:
                result_dict[data[keys[0]]] = data[keys[1]]
                sorted_d = dict(sorted(result_dict.items(), key=lambda x: x[1]))
                # Keep only the top 10 items
                top = dict(list(sorted_d.items())[-number_of_comp:])
        result[agg["_id"][0]] = top
    return result
