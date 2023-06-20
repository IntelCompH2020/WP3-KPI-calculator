import os
import json
import requests


def send_data_to_api(client_id, username, client_secret, password, directory):
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

    # Loop through the JSON files in the directory and send them to the API
    for filename in os.listdir(directory):
        if filename.endswith("_data.json"):
            file_path = os.path.join(directory, filename)

            # Read the JSON content of the file
            with open(file_path) as file:
                file_content = json.load(file)

            # Send the data to the API
            api_url = (
                "https://gateway.opix.ai/sti-viewer/api/api/indicator-point/"
                "664de786-2879-4b65-82c7-df5d0d30be84/bulk-persist"
            )
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            response = requests.post(api_url, headers=headers, json=file_content)

            # Check if the request was successful
            if response.status_code == 200:
                print(f"Data from {filename} was sent successfully.")
            else:
                print(
                    f"Error sending data from {filename}. "
                    f"Status code: {response.status_code}"
                )


# Read the configuration from the JSON file
with open("pass.json") as file:
    config = json.load(file)

# Extract the values from the configuration
client_id = config["client_id"]
username = config["username"]
client_secret = config["client_secret"]
password = config["password"]
directory = config["directory"]

# Call the function to send the data to the API
send_data_to_api(client_id, username, client_secret, password, directory)
