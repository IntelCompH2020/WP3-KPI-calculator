import json
import requests
from pathlib import Path


def send_data_to_api(job_id, template):
    script_dir = Path(__file__).resolve().parent.parent
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

    # Send the data to the API
    api_url = "https://gateway.opix.ai/sti-viewer/api/api/user-settings/persist"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Construct the payload JSON
    payload = {
        "key": job_id,
        "entityType": "Application",
        "type": "Dashboard",
        "value": template,  # Use the file content as the value
    }

    response = requests.post(api_url, headers=headers, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        print(f"Data from {job_id} was sent successfully.")
    else:
        print(
            f"Error sending data from {job_id}. " f"Status code: {response.status_code}"
        )
