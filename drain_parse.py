
import json
import logging
import os
import subprocess
import sys
import time
from os.path import dirname

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from masker import LogMasker
from collections import defaultdict
import re


config = TemplateMinerConfig()
config.load(f"drain3.ini")
config.profiling_enabled = True



def get_log_lines(log_file_path):
    log_lines = []
    with open(log_file_path, 'r') as file:
        # Read each line in the file
        for line in file:
            # Process the line (for example, print it)
            log_lines.append(line.strip())
    return log_lines


def parse_log_file(log_lines):
    
    template_miner = TemplateMiner(config=config)
    masker = LogMasker()
    for line in log_lines:
        line = line.rstrip()
        masked_line, _ = masker.mask(line)
        result = template_miner.add_log_message(masked_line)

    return template_miner

def get_tokens(s):
    parts = re.split(r'(<[^>]*>)', s)
    # Remove any empty strings from the result
    return [part for part in parts if part]


def extract_parameters(template, masked_line, parameters):
    template_tokens = template.split()
    log_tokens = masked_line.split()

    # Check if the template and log have the same number of tokens
    if len(template_tokens) != len(log_tokens):
        raise ValueError("Template and log do not match in structure.")

    # Extract parameters
    new_parameters = []
    for template_token, log_token in zip(template_tokens, log_tokens):
            
        if template_token == "<*>": # template token is <*>, but the log token can be `fleet.cattle.io<PATH>` or `<PATH><DIGITS>` which requires more processing
            # Extract parameter name and value
            param_name = template_token 
            split_log_tokens = get_tokens(log_token)
            res_full_string = ""
            for each_token in split_log_tokens:
                if each_token in parameters:
                    actual_log_token = parameters[each_token].pop(0)
                    res_full_string += actual_log_token
                else:
                    res_full_string += each_token
            new_parameters.append({param_name: res_full_string})

        else:
            
            tokens = get_tokens(template_token) # template token can be something like `fleet.cattle.io<PATH>`, so we split them and examine each part
            for token in tokens:
                if token.startswith('<') and token.endswith('>'):
                    actual_log_token = parameters[token].pop(0)
                    new_parameters.append({token: actual_log_token})
    return new_parameters


def display_clusters(template_miner):
    sorted_clusters = sorted(template_miner.drain.clusters, key=lambda it: it.size, reverse=True)
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
        
        line = line.rstrip()
        masked_line, parameters = masker.mask(line)
        matched_cluster = template_miner.match(masked_line)
        template = matched_cluster.get_template()
        cluster_id = matched_cluster.cluster_id
        
        paras = extract_parameters(template, masked_line, parameters)
        parameters_by_cluster[cluster_id].append(paras)
    return parameters_by_cluster


def get_log_templates(log_file_path):
    log_lines = get_log_lines(log_file_path)
    template_miner = parse_log_file(log_lines)
    return display_clusters(template_miner), template_miner, log_lines
