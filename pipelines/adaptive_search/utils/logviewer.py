import json
import sys
from datetime import datetime
import argparse
import os
from colorama import init, Fore, Style

init()  # Initialize colorama

COLORS = {
    "DEBUG": Fore.BLUE,
    "INFO": Fore.GREEN,
    "WARNING": Fore.YELLOW,
    "ERROR": Fore.RED,
    "CRITICAL": Fore.RED + Style.BRIGHT
}

def format_log_entry(entry):
    try:
        data = json.loads(entry)
        level = data.get("level", "INFO")
        color = COLORS.get(level, "")
        reset = Style.RESET_ALL
        
        # Format timestamp
        timestamp = data.get("timestamp", "")
        
        # Basic info
        message = data.get("message", "")
        
        # Get important context
        context = []
        extra = {}
        for k, v in data.items():
            if k not in ["timestamp", "level", "logger", "message"]:
                extra[k] = v
        
        # Extract key context fields
        for field in ["operation", "search_id", "session_id", "status"]:
            if field in extra:
                context.append(f"{field}={extra[field]}")
        
        # Format important metrics if present
        if "metrics" in extra:
            metrics = extra["metrics"]
            context.append(f"requests={metrics.get('total_requests', 0)}")
            context.append(f"unique={metrics.get('unique_results', 0)}")
        
        context_str = " | ".join(context)
        
        # Full formatted log line
        return f"{timestamp} {color}{level.ljust(8)}{reset} {message} [{context_str}]"
    
    except json.JSONDecodeError:
        return entry  # Return the original line if not valid JSON

def main():
    parser = argparse.ArgumentParser(description="Pretty print JSON log files")
    parser.add_argument("logfile", help="Path to the JSON log file")
    parser.add_argument("-l", "--level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Minimum log level to display")
    parser.add_argument("-f", "--filter", help="Only show logs containing this text")
    parser.add_argument("-o", "--operation", help="Filter by operation type")
    args = parser.parse_args()
    
    level_priorities = {
        "DEBUG": 0,
        "INFO": 1,
        "WARNING": 2,
        "ERROR": 3,
        "CRITICAL": 4
    }
    min_priority = level_priorities.get(args.level, 0)
    
    with open(args.logfile, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            try:
                data = json.loads(line)
                
                # Apply level filter
                if args.level and level_priorities.get(data.get("level", ""), 0) < min_priority:
                    continue
                    
                # Apply text filter
                if args.filter and args.filter.lower() not in line.lower():
                    continue
                    
                # Apply operation filter
                if args.operation:
                    operation = data.get("operation", "")
                    if operation != args.operation:
                        continue
                
                # Print formatted entry
                print(format_log_entry(line))
                
            except json.JSONDecodeError:
                print(line)

if __name__ == "__main__":
    main()