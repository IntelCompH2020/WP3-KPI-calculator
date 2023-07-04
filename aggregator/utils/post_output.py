import json
from pathlib import Path
import requests

script_dir = Path(__file__).resolve().parent.parent


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def check_empty_dict(input_dict):
    if not input_dict:
        raise ValueError("Dictionary is empty")


def get_data(job_id, user_id, dgid, pvid, indid, svid, data):
    output = []

    with open(script_dir.joinpath("config/mapping.json")) as j:
        code_to_label = json.loads(j.read())

    common_dict = {
        "domain": code_to_label[dgid].split(" ")[0],
        "country": code_to_label[dgid].split(" ")[1],
        "dg": code_to_label[dgid],
        # "dg": dgid,
        "dgid": dgid,
        "pv": code_to_label[pvid],
        # "pv": pvid,
        "pvId": pvid,
        "ind": code_to_label[indid],
        "indid": indid,
        "sv": code_to_label[svid],
        "svid": svid,
        "analysis_id": job_id,
        "user_id": user_id,
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


def produce_results(job_id, user_id, dgid, pvid, results, logging, errors, completed):
    # Read the configuration from the JSON file
    with open(script_dir.joinpath("utils/pass.json")) as file:
        config = json.load(file)

    # Extract the values from the configuration
    client_id = config["client_id"]
    username = config["username"]
    client_secret = config["client_secret"]
    password = config["password"]

    # Get the access token using requests
    url = "https://gateway.opix.ai/auth/realms/default/protocol/openid-connect/token"
    access = {
        "client_id": client_id,
        "grant_type": "password",
        "client_secret": client_secret,
        "scope": "openid",
        "username": username,
        "password": password,
    }

    response = requests.post(url, data=access)
    access_token = response.json()["access_token"]

    # Batch size for elk
    BATCH_SIZE = 50
    for indid in results.keys():
        for svid in results[indid].keys():
            try:
                data = get_data(
                    job_id, user_id, dgid, pvid, indid, svid, results[indid][svid]
                )
                # Drop zero values
                new_data = [item for item in data if item["value"] != 0]
                # print(new_data[0])
                total_batches = (len(new_data) + BATCH_SIZE - 1) // BATCH_SIZE
                for i, batch in enumerate(chunks(new_data, BATCH_SIZE), start=1):
                    # Call the function to send the data to the API
                    # Send the data to the API
                    api_url = (
                        "https://gateway.opix.ai/sti-viewer/api/api/indicator-point/"
                        "664de786-2879-4b65-82c7-df5d0d30be84/bulk-persist"
                    )
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    }
                    response = requests.post(api_url, headers=headers, json=batch)
                    # Check if the request was successful
                    if response.status_code != 200:
                        raise Exception(
                            f"Error sending batch of data from {indid}_{svid}. "
                            f"Status code: {response.status_code}"
                        )
                    if i == total_batches:
                        logging.info(f"Data from {indid}_{svid} was sent successfully.")
                        if indid not in completed:
                            completed[indid] = []
                        completed[indid].append(svid)
            except Exception as e:
                error_message = (
                    f"Error executing function {indid} for view {svid}: {str(e)}"
                )
                if indid not in errors:
                    errors[indid] = []
                errors[indid].append(svid)
                logging.error(error_message)
    return errors, completed
