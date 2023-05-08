import requests
import json
from pathlib import Path

script_dir = Path(__file__).resolve().parent.parent

# Read the authorization token from the file
with open(script_dir.joinpath("utils/auth_token.txt")) as file:
    auth_token = file.read().strip()

url = "https://gateway.opix.ai/sti-viewer/api/api/indicator/persist/es"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {auth_token}",
}

# Read the data from the JSON file
with open("data.json") as file:
    data = json.load(file)

response = requests.post(url, headers=headers, data=json.dumps(data))

print(response.text)
