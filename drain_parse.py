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
import hashlib
from urllib.parse import urlparse
import pathlib

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
        if (
            template_token == "<*>"
        ):  # template token is <*>, but the log token can be `fleet.cattle.io<PATH>` or `<PATH><DIGITS>` which requires more processing
            param_name = template_token
            split_log_tokens = get_tokens(log_token)
            res_full_string = ""
            for each_token in split_log_tokens:
                if each_token in parameters:
                    actual_log_token = parameters[each_token].pop(0)
                    res_full_string += actual_log_token
                else:
                    res_full_string += each_token
            new_parameters.append({"token": param_name, "value": res_full_string})

        else:
            tokens = get_tokens(
                template_token
            )  # template token can be something like `fleet.cattle.io<PATH>` or `<PATH><DIGITS>`, so we split them and examine each part
            for token in tokens:
                if token.startswith("<") and token.endswith(">"):
                    actual_log_token = parameters[token].pop(0)
                    new_parameters.append({"token": token, "value": actual_log_token})
        # TODO: can template_token be a string like `fleet.cattle.io<*>`? hopefully not.
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
    """Process a log file and extract templates."""
    log_lines = get_log_lines(log_file_path)
    template_miner = parse_log_file(log_lines)

    clusters = [cluster.get_template() for cluster in template_miner.drain.clusters]

    return clusters, template_miner, log_lines


def get_cache_filename(url: str) -> str:
    """
    Generate a consistent filename from URL that's filesystem-friendly.

    Args:
        url: The URL of the log file

    Returns:
        A filename in the format: {url_hostname}_{hash}_{filename}.log
    """
    parsed_url = urlparse(url)
    original_filename = os.path.basename(parsed_url.path)
    if not original_filename:
        original_filename = "log"

    # Create a short hash of the full URL
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

    # Get hostname, removing any non-alphanumeric characters
    hostname = "".join(c for c in parsed_url.hostname if c.isalnum())

    # Construct the filename
    return f"{hostname}_{url_hash}_{original_filename}"


def get_or_download_file(url: str, cache_dir: str = "cache") -> str:
    """
    Downloads a file if it doesn't exist in cache, otherwise returns cached file path.

    Args:
        url: The URL to download from
        cache_dir: Directory to store downloaded files

    Returns:
        Path to the local file
    """
    # Create cache directory if it doesn't exist
    pathlib.Path(cache_dir).mkdir(parents=True, exist_ok=True)

    # Generate filename from URL
    filename = get_cache_filename(url)
    cached_file_path = os.path.join(cache_dir, filename)

    # If file doesn't exist in cache, download it
    if not os.path.exists(cached_file_path):
        print(f"Downloading log file from {url}")
        wget.download(url, cached_file_path)
        print("\nDownload complete")
    else:
        print(f"Using cached file: {cached_file_path}")

    return cached_file_path


def save_snapshot(template_miner, log_lines, cache_dir: str = "cache"):
    """Save the current state of clusters and processed logs"""
    snapshot = {
        "clusters": [
            {
                "id": cluster.cluster_id,
                "size": cluster.size,
                "template": cluster.get_template(),
            }
            for cluster in template_miner.drain.clusters
        ],
        "log_lines": log_lines,
    }

    snapshot_path = os.path.join(cache_dir, "last_template_snapshot.json")
    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f)

    return snapshot


def load_snapshot(cache_dir: str = "cache"):
    """Load the last saved template snapshot"""
    snapshot_path = os.path.join(cache_dir, "last_template_snapshot.json")
    if not os.path.exists(snapshot_path):
        raise FileNotFoundError(
            "No analysis snapshot found. Please analyze the log patterns first:\n"
            f"python3 drain_parse.py --log_file_url '{os.getenv('LOG_FILE_URL')}' --action analyze"
        )

    with open(snapshot_path, "r") as f:
        return json.load(f)


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Log Parser Tool")
    parser.add_argument("--log_file", help="Path to the log file")
    parser.add_argument("--log_file_url", help="URL to download the log file")
    parser.add_argument(
        "--action",
        choices=["analyze", "extract"],  # or ["discover", "extract"]
        help="Action to perform: 'analyze' to discover log patterns, 'extract' to get parameters from a pattern",
    )
    parser.add_argument(
        "--cluster_id", type=int, help="Cluster ID for parameters action"
    )

    args = parser.parse_args()

    # Get parameters from either environment variables or command line arguments
    log_file = os.getenv("LOG_FILE") or args.log_file
    log_file_url = os.getenv("LOG_FILE_URL") or args.log_file_url
    action = os.getenv("ACTION") or args.action
    cluster_id = os.getenv("CLUSTER_ID") or args.cluster_id

    # Handle file location
    if log_file_url:
        log_file = get_or_download_file(log_file_url)
    elif not log_file:
        print("Error: Either LOG_FILE or LOG_FILE_URL must be provided")
        sys.exit(1)

    if not os.path.exists(log_file):
        print("Error: Log file not found")
        sys.exit(1)

    if not action or action not in ["analyze", "extract"]:
        print(
            'Error: ACTION must be either "analyze" (to discover patterns) or "extract" (to get parameters from a pattern)'
        )
        sys.exit(1)

    try:
        if action == "analyze":
            clusters, template_miner, log_lines = get_log_templates(log_file)
            snapshot = save_snapshot(template_miner, log_lines)

            print(
                json.dumps(
                    {
                        "message": "Analysis complete. You can now use 'extract' action with --cluster_id to get parameters.",
                        "clusters": [
                            {
                                "id": c["id"],
                                "size": c["size"],
                                "template": c["template"],
                            }
                            for c in snapshot["clusters"]
                        ],
                    },
                    indent=2,
                )
            )

        elif action == "extract":
            if not cluster_id:
                print(
                    json.dumps(
                        {
                            "error": "Cluster ID is required. Please run 'analyze' action first to see available cluster IDs"
                        }
                    )
                )
                sys.exit(1)

            try:
                # Load the last snapshot instead of reprocessing
                snapshot = load_snapshot()

                # Verify the cluster_id exists in the snapshot
                cluster_exists = any(
                    c["id"] == cluster_id for c in snapshot["clusters"]
                )
                if not cluster_exists:
                    print(
                        json.dumps(
                            {
                                "error": f"Cluster ID {cluster_id} not found in last template snapshot"
                            }
                        )
                    )
                    sys.exit(1)

                # Reprocess only for parameter extraction using saved log lines
                template_miner = parse_log_file(snapshot["log_lines"])
                parameters = get_parameters_by_cluster(
                    template_miner, snapshot["log_lines"]
                )

                if cluster_id not in parameters:
                    print(
                        json.dumps(
                            {
                                "error": f"No parameters found for cluster ID {cluster_id}"
                            }
                        )
                    )
                else:
                    print(
                        json.dumps(
                            {
                                "cluster_id": cluster_id,
                                "template": next(
                                    c["template"]
                                    for c in snapshot["clusters"]
                                    if c["id"] == cluster_id
                                ),
                                "parameters": parameters[cluster_id],
                            }
                        )
                    )

            except FileNotFoundError:
                print(
                    json.dumps(
                        {
                            "error": "No analysis snapshot found. Please analyze the log patterns first:\n"
                            f"python3 drain_parse.py --log_file_url '{log_file_url}' --action analyze"
                        }
                    )
                )
                sys.exit(1)

    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
