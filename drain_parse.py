import json
import logging
import os
import subprocess
import sys
import time
from os.path import dirname
from typing import List, Dict, Any, Tuple
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from masker import LogMasker
from collections import defaultdict
import re
import argparse
import wget

ini_content = """[SNAPSHOT]
snapshot_interval_minutes = 10
compress_state = True

[DRAIN]
# engine is Optional parameter. Engine will be "Drain" if the engine argument is not specified.
# engine has two options: 'Drain' and 'JaccardDrain'.
# engine = Drain
sim_th = 0.7
depth = 6
max_children = 512
max_clusters = 1024

[PROFILING]
enabled = True
report_sec = 30
"""

# File path for the INI file
drain_config_file = "drain3.ini"

# Check if the file exists
if not os.path.exists(drain_config_file):
    # If the file does not exist, create it
    with open(drain_config_file, "w") as fout:
        fout.write(ini_content)

config = TemplateMinerConfig()
config.load(drain_config_file)
config.profiling_enabled = True


def get_log_lines(log_file_path):
    if not os.path.exists(log_file_path):
        raise FileNotFoundError(f"Log file not found: {log_file_path}")

    try:
        with open(log_file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file]
    except Exception as e:
        logging.error(f"Error reading log file: {e}")
        raise


def parse_log_file(log_lines):
    if not log_lines:
        raise ValueError("Empty log lines provided")

    template_miner = TemplateMiner(config=config)
    masker = LogMasker()
    for line in log_lines:
        line = line.rstrip()
        masked_line, _ = masker.mask(line)
        result = template_miner.add_log_message(masked_line)

    return template_miner


def get_tokens(s):
    parts = re.split(r"(<[^>]*>)", s)
    # Remove any empty strings from the result
    return [part for part in parts if part]


def extract_parameters(template, masked_line, parameters):
    template_tokens = template.split()
    log_tokens = masked_line.split()

    if len(template_tokens) != len(log_tokens):
        return []  # Return empty list if tokens don't match

    # Extract parameters
    new_parameters = []
    for template_token, log_token in zip(template_tokens, log_tokens):
        if template_token == "<*>":
            # For wildcard tokens, store the actual value
            new_parameters.append({"token": "<*>", "value": log_token})
        elif template_token.startswith("<") and template_token.endswith(">"):
            # For other tokens, store both the token type and value
            param_value = parameters.get(template_token)
            if param_value is not None:
                if isinstance(param_value, (list, tuple)):
                    value = param_value[0] if param_value else ""
                else:
                    value = str(param_value)
                new_parameters.append({"token": template_token, "value": value})
    return new_parameters


def display_clusters(template_miner):
    sorted_clusters = sorted(
        template_miner.drain.clusters, key=lambda it: it.size, reverse=True
    )
    print(f"----------clusters:--------------------")
    res = []
    for cluster in sorted_clusters:
        print(cluster)
        res.append(cluster.get_template())
    return res


def get_parameters_by_cluster(template_miner, log_lines):
    masker = LogMasker()
    parameters_by_cluster = defaultdict(list)

    for line in log_lines:
        try:
            line = line.rstrip()
            masked_line, parameters = masker.mask(line)
            matched_cluster = template_miner.match(masked_line)

            if matched_cluster:
                template = matched_cluster.get_template()
                cluster_id = matched_cluster.cluster_id

                params = extract_parameters(template, masked_line, parameters)
                if params:  # Only add if we got parameters
                    parameters_by_cluster[cluster_id].append(
                        {"line": line, "parameters": params}
                    )
        except Exception as e:
            logging.warning(f"Error processing line: {line}. Error: {str(e)}")
            continue

    return dict(parameters_by_cluster)  # Convert defaultdict to regular dict


def get_log_templates(log_file_path: str) -> Tuple[List[str], TemplateMiner, List[str]]:
    """
    Process a log file and extract templates.

    Args:
        log_file_path: Path to the log file

    Returns:
        Tuple containing:
        - List of template strings
        - TemplateMiner instance
        - Original log lines
    """
    log_lines = get_log_lines(log_file_path)
    template_miner = parse_log_file(log_lines)
    return display_clusters(template_miner), template_miner, log_lines


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Log Parser Tool")
    parser.add_argument("--log_file", help="Path to the log file")
    parser.add_argument("--log_file_url", help="URL to download the log file")
    parser.add_argument(
        "--action", choices=["templates", "parameters"], help="Action to perform"
    )
    parser.add_argument(
        "--cluster_id", type=int, help="Cluster ID for parameters action"
    )

    args = parser.parse_args()

    # Get parameters from either environment variables or command line arguments
    log_file = (
        os.getenv("LOG_FILE") or args.log_file or "downloaded_log.txt"
    )  # Default filename
    log_file_url = os.getenv("LOG_FILE_URL") or args.log_file_url
    action = os.getenv("ACTION") or args.action
    cluster_id = os.getenv("CLUSTER_ID") or args.cluster_id

    if log_file_url:
        print(f"Downloading log file from {log_file_url}")
        wget.download(log_file_url, log_file)
        print("\nDownload complete")

    if not os.path.exists(log_file):
        print("Error: Log file not found and no URL provided to download")
        sys.exit(1)

    if not action or action not in ["templates", "parameters"]:
        print('Error: ACTION must be either "templates" or "parameters"')
        sys.exit(1)

    try:
        # First get the templates and necessary objects
        clusters, template_miner, log_lines = get_log_templates(log_file)

        if action == "templates":
            # Templates are already displayed by display_clusters()
            pass
        elif action == "parameters":
            if not cluster_id:
                print('Error: CLUSTER_ID is required when action is "parameters"')
                sys.exit(1)

            try:
                cluster_id = int(cluster_id)
            except ValueError:
                print("Error: CLUSTER_ID must be a valid integer")
                sys.exit(1)

            parameters_by_cluster = get_parameters_by_cluster(template_miner, log_lines)
            if cluster_id not in parameters_by_cluster:
                print(
                    json.dumps(
                        {"error": f"No parameters found for cluster ID {cluster_id}"}
                    )
                )
            else:
                print(
                    json.dumps(
                        {
                            "cluster_id": cluster_id,
                            "parameters": parameters_by_cluster[cluster_id],
                        }
                    )
                )

    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
