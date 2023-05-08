import pymongo
import json
import importlib
import argparse
import logging
from pathlib import Path
import os
from utils import output_func


def main(config_file_path):
    # Configure logging
    log_dir = "/media/datalake/stiviewer/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "output.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file)],
    )

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
        template = func_config["template"]

        # Import the function dynamically
        module = importlib.import_module(module_name)
        function_to_call = getattr(module, function_name)

        # Call the function
        results = function_to_call(pat, results, template)
        logging.info(f"Function {function_name} executed successfully.")

    output_func.produce_results("dg01", "pv01", results, logging)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute functions based on a configuration file."
    )
    parser.add_argument(
        "config_file", help="Path to the configuration file (JSON format)."
    )
    args = parser.parse_args()
    main(args.config_file)
