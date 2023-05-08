import json
from pathlib import Path
import requests

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


def get_data(dgid, pvid, indid, svid, data):
    output = []
    data = {k: v for k, v in data.copy().items() if k is not None and not v == "null"}
    for k, v in dict(sorted(data.items())).items():
        if not isinstance(v, dict):
            output.append(
                {
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
                    "key": k,
                    "value": v,
                }
            )
        else:
            for year, value in dict(sorted(v.items())).items():
                output.append(
                    {
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
                        "key": k,
                        "year": year,
                        "value": value,
                    }
                )
    return output


def produce_results(dgid, pvid, results, logging):
    # Read the configuration from the JSON file

    with open(script_dir.joinpath("config/ind_value_datatypes.json")) as file:
        config = json.load(file)

    # Extract the values from the configuration
    client_id = config["client_id"]
    username = config["username"]
    client_secret = config["client_secret"]
    password = config["password"]

    # Get the access token using requests
    url = "https://gateway.opix.ai/auth/realms/default/protocol/openid-connect/token"
    data = {
        "client_id": client_id,
        "grant_type": "password",
        "client_secret": client_secret,
        "scope": "openid",
        "username": username,
        "password": password,
    }

    response = requests.post(url, data=data)
    access_token = response.json()["access_token"]

    for indid in results.keys():
        for svid in results[indid].keys():
            data = get_data(dgid, pvid, indid, svid, results[indid][svid])

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
            response = requests.post(api_url, headers=headers, json=data)

            # Check if the request was successful
            if response.status_code == 200:
                logging.info(f"Data from {indid}_{svid} was sent successfully.")
            else:
                logging.error(
                    f"Error sending data from {indid}_{svid}. "
                    f"Status code: {response.status_code}"
                )
