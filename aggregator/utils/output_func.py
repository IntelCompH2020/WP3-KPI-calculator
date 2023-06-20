import json
from pathlib import Path

script_dir = Path(__file__).resolve().parent.parent

with open(script_dir.joinpath("config/ind_value_datatypes.json")) as j:
    ind_value_datatypes = json.loads(j.read())
with open(script_dir.joinpath("config/sv_key_datatypes.json")) as j:
    sv_key_datatypes = json.loads(j.read())
with open(script_dir.joinpath("config/indicator_schema_template.json")) as j:
    indicator_schema = json.loads(j.read())
with open(script_dir.joinpath("config/indicator_schema_template_per_year.json")) as j:
    indicator_schema_per_year = json.loads(j.read())
with open(
    script_dir.joinpath("config/indicator_schema_template_per_publisher.json")
) as j:
    indicator_schema_per_publisher = json.loads(j.read())
with open(script_dir.joinpath("config/mapping.json")) as j:
    code_to_label = json.loads(j.read())
label_to_code = {v: k for k, v in code_to_label.items()}


def get_indicator_schema(dgid, pvid, indid, svid, data):
    data = {k: v for k, v in data.copy().items() if k is not None and not v == "null"}
    for k, v in dict(sorted(data.items())).items():
        if isinstance(v, dict):
            ind_schema = indicator_schema_per_year.copy()
            break
        else:
            ind_schema = indicator_schema.copy()
            break
    if indid == "i01" and svid == "sv10":
        ind_schema = indicator_schema_per_publisher.copy()

    ind_schema["metadata"]["label"] = (
        code_to_label[dgid]
        + " - "
        + code_to_label[pvid]
        + " - "
        + code_to_label[indid]
        + " - "
        + code_to_label[svid]
    )
    ind_schema["metadata"]["code"] = dgid + "_" + pvid + "_" + indid + "_" + svid
    ind_schema["schema"]["fields"][10]["basetype"] = sv_key_datatypes[svid]
    ind_schema["schema"]["fields"][11]["basetype"] = ind_value_datatypes[indid]
    return ind_schema


def check_empty_dict(input_dict):
    if not input_dict:
        raise ValueError("Dictionary is empty")


def get_data(dgid, pvid, indid, svid, data):
    output = []

    common_dict = {
        "domain": code_to_label[dgid].split(" ")[0],
        "country": code_to_label[dgid].split(" ")[1],
        "dg": code_to_label[dgid],
        "dgid": dgid,
        "pv": code_to_label[pvid],
        "pvId": pvid,
        "ind": code_to_label[indid],
        "indid": indid,
        "sv": code_to_label[svid],
        "svid": svid,
    }

    def process_data(k, v):
        if isinstance(v, dict):
            for sub_k, sub_v in dict(sorted(v.items())).items():
                if isinstance(sub_v, dict):
                    for sub_sub_k, sub_sub_v in dict(sorted(sub_v.items())).items():
                        output.append(
                            {
                                **common_dict,
                                "key": k,
                                "metric_scope": sub_k,
                                "year": sub_sub_k,
                                "value": sub_sub_v,
                            }
                        )
                else:
                    if svid == "sv17" or svid == "sv18":
                        output.append(
                            {
                                **common_dict,
                                "key": k,
                                "metric_scope": sub_k,
                                "value": sub_v,
                            }
                        )
                    elif indid == "i12a" and not (svid == "sv01" or svid == "sv00"):
                        output.append(
                            {
                                **common_dict,
                                "tag": k,
                                "key": sub_k,
                                "value": sub_v,
                            }
                        )
                    elif indid == "i12a" and (svid == "sv01" or svid == "sv00"):
                        output.append(
                            {
                                **common_dict,
                                "tag": k,
                                "year": sub_k,
                                "value": sub_v,
                            }
                        )
                    else:
                        output.append(
                            {**common_dict, "key": k, "year": sub_k, "value": sub_v}
                        )
        else:
            output.append({**common_dict, "key": k, "value": v})

    check_empty_dict(data)
    data = {k: v for k, v in data.copy().items() if k is not None and v != "null"}
    for k, v in dict(sorted(data.items())).items():
        process_data(k, v)

    return output


def produce_results(dgid, pvid, results, logging):
    # Convert results to desired format
    output_lake = Path("/media/datalake/stiviewer")
    output_dir = output_lake.joinpath("output/output_done/diamantis")

    count = 0
    for indid in results.keys():
        try:
            for svid in results[indid].keys():
                data = get_data(dgid, pvid, indid, svid, results[indid][svid])
                data_file = json.dumps(data, indent=2)
                count += len(data_file)
                data_path = output_dir.joinpath(
                    f"{dgid}_{pvid}_{indid}_{svid}_data.json"
                )
                with open(data_path, "w") as f:
                    f.write(data_file)
                logging.info(
                    f"For Indicator {indid} " f"Secondary view {svid} file created."
                )
        except Exception as e:
            logging.error(
                f"Error executing function {indid}" f"for view {svid}: {str(e)}"
            )
