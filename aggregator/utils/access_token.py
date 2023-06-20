import json
from pathlib import Path
import requests

script_dir = Path(__file__).resolve().parent.parent


def get_access():
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

    print(access_token)


if __name__ == "__main__":
    get_access()

    import uuid

    # Generate a UUID
    uuid_value = uuid.uuid4()

    # Convert UUID to string
    uuid_string = str(uuid_value)

    print(uuid_string)
