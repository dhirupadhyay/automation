# Author: dhiraj.upadhyay
# This script analyzes SQL files in a specified directory, identifying Common Table Expressions (CTEs) and checking for potential antipatterns.
# It extracts CTE names and their occurrences, warns if a CTE is used more than twice in FROM or JOIN conditions, and provides a total count.
# The analysis is performed on each SQL file in the given directory.

import re
import os
from collections import Counter
import csv
import datetime
import getpass  

def get_developer_name():
    return getpass.getuser()

def extract_cte_names(sql_script):
    # Regular expression for identifying antipattern that is start of CTE here
    start_cte_pattern = re.compile(r'\bWITH\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(', re.IGNORECASE)
    # Regular expression for the continuation of CTE
    continue_cte_pattern = re.compile(r'\),\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(', re.IGNORECASE)
    # Extract CTE names using both patterns
    start_cte_names = start_cte_pattern.findall(sql_script)
    continue_cte_names = continue_cte_pattern.findall(sql_script)
    # Combine the CTE names from both patterns
    cte_names = start_cte_names + continue_cte_names
    return cte_names

def extract_used_ctes(sql_script):
    # Regular expression to find CTE names in FROM clause and JOIN conditions
    cte_occurrence_pattern = re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', re.IGNORECASE)
    # Extract all occurrences of CTE names
    cte_occurrences = cte_occurrence_pattern.findall(sql_script)
    # Remove occurrences in SELECT columns
    select_columns_pattern = re.compile(r'\bSELECT\b(.+?)\bFROM\b', re.DOTALL | re.IGNORECASE)
    select_columns_matches = select_columns_pattern.findall(sql_script)
    for match in select_columns_matches:
        select_columns = match.split(',')
        for column in select_columns:
            cte_occurrences = [cte for cte in cte_occurrences if cte not in column]
    return cte_occurrences

def count_ctes_in_sql_file(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
            # Extract CTE names using the defined function
            cte_names = extract_cte_names(sql_script)
            # Extract used CTEs in FROM and JOIN conditions
            used_ctes = extract_used_ctes(sql_script)
            # Count each distinct CTE name
            cte_counts = Counter(cte_names)
            warnings = []
            # Check if a CTE is being used more than twice
            for cte, count in cte_counts.items():
                if used_ctes.count(cte) > 2:
                    warnings.append(f"Warning: CTE '{cte}' is being used twice or more than twice in FROM or JOIN conditions. Consider creating a temp table instead of CTE for better performance.")
            return cte_counts, warnings
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return Counter(), []

output_directory = r'C:\Users\dhiraj.upadhyay\Desktop\antipattern\automation\cte_analysis_output'

def write_results_to_csv(results, output_directory):
    columns = ["Message", "File Name", "Developer Name", "Start Time", "End Time", "Antipattern Type"]

    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y%m%d%H%M%S")  # Format: YYYYMMDDHHMMSS
    output_file_name = f'antipattern_cte_analysis_{formatted_time}.csv'
    output_file_path = os.path.join(output_directory, output_file_name)

    with open(output_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header
        csv_writer.writerow(columns)

        for result in results:
            sql_script_file_name = result[0]
            cte_counts = result[1]
            warnings = result[2]
            total_count = sum(cte_counts.values())
            antipattern_type = "CTE Analysis"

            if warnings:
                for i, warning in enumerate(warnings, start=1):
                    csv_writer.writerow([f"Warning {i}: {warning.lstrip('Warning: ')}", sql_script_file_name, get_developer_name(),
                                         current_time, datetime.datetime.now(), antipattern_type])
            else:
                csv_writer.writerow([f"No of CTEs = {total_count} (All CTEs are optimized, no modification needed)", sql_script_file_name, get_developer_name(),
                                     current_time, datetime.datetime.now(), antipattern_type])
            
def analyze_sql_files(directory_path):
    if not os.path.isdir(directory_path):
        print(f"The provided path is not a directory: {directory_path}")
        return
    sql_files = [f for f in os.listdir(directory_path) if f.endswith(".sql")]
    if not sql_files:
        print(f"No SQL files found in the directory: {directory_path}")
        return
    results = []
    for sql_file in sql_files:
        file_path = os.path.join(directory_path, sql_file)
        cte_counts, warnings = count_ctes_in_sql_file(file_path)
        total_count = sum(cte_counts.values())
        results.append((sql_file, cte_counts, warnings, total_count))
    return results

directory_path = r'C:\Users\dhiraj.upadhyay\Desktop\antipattern\automation\sql_testing_scripts'
result_list = analyze_sql_files(directory_path)

for result in result_list:
    for cte_name, count in result[1].items():
        print(f"File: {result[0]}, CTE: {cte_name}, Count: {count}")
    for warning in result[2]:
        print(warning)
    print(f"Total CTE Count: {result[3]}")
    print("-" * 50)

write_results_to_csv(result_list, output_directory)
