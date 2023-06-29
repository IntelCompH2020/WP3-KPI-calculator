import json
from pathlib import Path

script_dir = Path(__file__).resolve().parent.parent


def check_empty_dict(input_dict):
    if not input_dict:
        raise ValueError("Dictionary is empty")


def get_data(dgid, pvid, indid, svid, data):
    output = []

    with open(script_dir.joinpath("config/mapping.json")) as j:
        code_to_label = json.loads(j.read())

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
                                "year": str(sub_sub_k),
                                "value": sub_sub_v,
                            }
                        )
                else:
                    if svid in {"sv17", "sv18", "sv07", "sv07b", "sv09"}:
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
                                "year": str(sub_k),
                                "value": sub_v,
                            }
                        )
                    else:
                        output.append(
                            {
                                **common_dict,
                                "key": k,
                                "year": str(sub_k),
                                "value": sub_v,
                            }
                        )
        else:
            if svid == "sv01":
                output.append({**common_dict, "key": str(k), "value": v})
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

    for indid in results.keys():
        try:
            for svid in results[indid].keys():
                data = get_data(dgid, pvid, indid, svid, results[indid][svid])
                data_file = json.dumps(data, indent=2)
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
