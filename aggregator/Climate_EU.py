import pymongo
import json
import importlib
import argparse
import logging
import os
from utils import post_output
from utils import output_func
from utils import uf


def main(config_file_path):
    # Load configuration file
    with open(config_file_path) as config_file:
        config_data = json.load(config_file)

    # Configure logging
    log_dir = "/workdir/patent-workflow/" + config_data["job_id"] + "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "output.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file)],
    )

    # Connect to MongoDB
    myclient = pymongo.MongoClient(
        "mongodb://adminuser:password123@gateway.opix.ai:27017/"
    )

    # Import and call functions based on the configuration file
    for func_config in config_data["functions"]:
        results = {}
        try:
            module_name = func_config["module"]
            function_name = func_config["function"]
            template = config_data["templates"][0][func_config["template"]]

            # Import the function dynamically
            module = importlib.import_module(module_name)
            function_to_call = getattr(module, function_name)

            domain = config_data["dgpv"][0]["dg"]
            topic = config_data["dgpv"][0]["pv"]

            # {
            #   "dg": "Climate Change",
            #   "pv": "Energy"
            # }

            # HARD CODED
            dg = uf.dg
            pv = uf.pv
            # HARD CODED

            spark_output = config_data["spark_output"]

            # Call the function
            if config_data["job_id"] == "intelcompt":
                STI_viewer_data = myclient["STI_viewer_data"]
                results = function_to_call(
                    STI_viewer_data[func_config["collection"]],
                    results,
                    template,
                    spark_output,
                )
                output_func.produce_results(
                    # config_data["dgpv"][0]["dg"],
                    # config_data["dgpv"][0]["pv"],
                    dg,
                    pv,
                    results,
                    logging,
                )
            else:
                STI_viewer_data = myclient["testdb"]
                collection = config_data["job_id"]
                results = function_to_call(
                    STI_viewer_data[collection], results, template
                )
                logging.info(
                    f"For domain {domain} and topic {topic} Function {function_name} "
                    f"for module {module_name} executed successfully."
                )
                post_output.produce_results(
                    config_data["job_id"],
                    config_data["user_id"],
                    dg,
                    pv,
                    # config_data["dgpv"][0]["dg"],
                    # config_data["dgpv"][0]["pv"],
                    results,
                    logging,
                )

        except Exception as e:
            logging.error(
                f"Error executing function {function_name} "
                f"for module {module_name}: {str(e)}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute functions based on a configuration file."
    )
    parser.add_argument(
        "config_file", help="Path to the configuration file (JSON format)."
    )
    args = parser.parse_args()
    main(args.config_file)
