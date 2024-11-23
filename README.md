# otto8-log-tool

A tool for analyzing log files using the DRAIN algorithm to discover log patterns and extract parameters.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/otto8-log-tool.git
cd otto8-log-tool
```

2. Create and activate a virtual environment:

```bash
python -m venv otto8-log-tool-py3.11
source otto8-log-tool-py3.11/bin/activate  # On Unix/macOS
# or
.\otto8-log-tool-py3.11\Scripts\activate  # On Windows
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

The tool has two main actions:

1. `analyze`: Discover log patterns in your log file
2. `extract`: Extract parameters from specific log patterns

### Analyzing Log Patterns

To analyze a log file and discover patterns:

```bash
python3 drain_parse.py --log_file_url "URL_TO_YOUR_LOG_FILE" --action analyze
```

Example:

```bash
python3 drain_parse.py --log_file_url "https://raw.githubusercontent.com/logpai/loghub/refs/heads/master/OpenStack/OpenStack_2k.log" --action analyze
```

This will:

- Download and cache the log file (if using URL)
- Analyze the log patterns
- Output JSON containing:
  - All discovered patterns
  - Their cluster IDs
  - Number of occurrences (size)

### Extracting Parameters

After analyzing, you can extract parameters from specific patterns using their cluster ID:

```bash
python3 drain_parse.py --log_file_url "URL_TO_YOUR_LOG_FILE" --action extract --cluster_id <ID>
```

Example:

```bash
python3 drain_parse.py --log_file_url "https://raw.githubusercontent.com/logpai/loghub/refs/heads/master/OpenStack/OpenStack_2k.log" --action extract --cluster_id 1
```

This will:

- Use the cached log file
- Extract all parameters from log entries matching the specified cluster
- Output JSON containing:
  - The original template
  - All extracted parameters and their values

### Using Local Files

You can also use local log files instead of URLs:

```bash
python3 drain_parse.py --log_file "path/to/your/logfile.log" --action analyze
python3 drain_parse.py --log_file "path/to/your/logfile.log" --action extract --cluster_id 1
```

### File Caching

- Downloaded log files are cached in the `cache` directory
- Each file is named using a combination of hostname and URL hash
- Subsequent runs will use the cached file instead of downloading again

### Environment Variables

You can also use environment variables instead of command-line arguments:

- `LOG_FILE`: Path to local log file
- `LOG_FILE_URL`: URL of log file to download
- `ACTION`: Either "analyze" or "extract"
- `CLUSTER_ID`: ID of cluster for parameter extraction

## Example Workflow

1. First, analyze your log file to discover patterns:

```bash
python3 drain_parse.py --log_file_url "your_log_url" --action analyze > patterns.json
```

2. Review the patterns to find the cluster ID you're interested in:

```bash
cat patterns.json | jq '.clusters[] | select(.template | contains("specific text"))'
```

3. Extract parameters from the pattern you're interested in:

```bash
python3 drain_parse.py --log_file_url "your_log_url" --action extract --cluster_id 1
```

## Notes

- Always run `analyze` before `extract`
- Cluster IDs are specific to each analysis run
- Log files are cached to avoid unnecessary downloads
- The tool outputs JSON for easy parsing and integration with other tools

See `log_parsing_tools.ipynb` for additional examples and usage patterns.
