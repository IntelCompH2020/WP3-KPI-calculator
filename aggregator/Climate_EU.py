import pymongo
import json
import importlib
import argparse
from utils import output_func


def main(config_file_path):
    # Load configuration file
    with open(config_file_path) as config_file:
        config_data = json.load(config_file)

    # Connect to MongoDB
    myclient = pymongo.MongoClient(
        "mongodb://adminuser:password123@gateway.opix.ai:27017/"
    )
    STI_viewer_data = myclient["sti_viewer"]
    pat = STI_viewer_data["patstat_demo_gkou"]

    results = {}

    # Import and call functions based on the configuration file
    for func_config in config_data["functions"]:
        module_name = func_config["module"]
        function_name = func_config["function"]

        # Import the function dynamically
        module = importlib.import_module(module_name)
        function_to_call = getattr(module, function_name)

        # Call the function
        results = function_to_call(pat, results)
        print(results)

    output_func.produce_results("dg01", "pv01", results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute functions based on a configuration file."
    )
    parser.add_argument(
        "config_file", help="Path to the configuration file (JSON format)."
    )
    args = parser.parse_args()
    main(args.config_file)
