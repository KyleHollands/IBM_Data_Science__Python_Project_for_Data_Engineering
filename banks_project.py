# ============================================================================
# Project: Acquiring and Processing Information on World's Largest Banks
# ============================================================================
#
# Project Scenario:
# You have been hired as a data engineer by research organization. Your boss
# has asked you to create a code that can be used to compile the list of the
# top 10 largest banks in the world ranked by market capitalization in billion
# USD. Further, the data needs to be transformed and stored in GBP, EUR and
# INR as well, in accordance with the exchange rate information that has been
# made available to you as a CSV file. The processed information table is to
# be saved locally in a CSV format and as a database table.
#
# Your job is to create an automated system to generate this information so
# that the same can be executed in every financial quarter to prepare the report.
#
# ============================================================================
# Project Parameters
# ============================================================================
# Parameter                              | Value
# ---------------------------------------|-------------------------------------
# Code name                              | banks_project.py
# Data URL                               | https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks
# Exchange rate CSV path                 | https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
# Table Attributes (upon Extraction only)| Name, MC_USD_Billion
# Table Attributes (final)               | Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
# Output CSV Path                        | ./Largest_banks_data.csv
# Database name                          | Banks.db
# Table name                             | Largest_banks
# Log file                               | code_log.txt
#
# ============================================================================
# Project Tasks
# ============================================================================
#
# Task 1:
# Write a function log_progress() to log the progress of the code at different
# stages in a file code_log.txt. Use the list of log points provided to create
# log entries as every stage of the code.
#
# Task 2:
# Extract the tabular information from the given URL under the heading
# 'By market capitalization' and save it to a dataframe.
#   a. Inspect the webpage and identify the position and pattern of the
#      tabular information in the HTML code
#   b. Write the code for a function extract() to perform the required data
#      extraction.
#   c. Execute a function call to extract() to verify the output.
#
# Task 3:
# Transform the dataframe by adding columns for Market Capitalization in GBP,
# EUR and INR, rounded to 2 decimal places, based on the exchange rate
# information shared as a CSV file.
#   a. Write the code for a function transform() to perform the said task.
#   b. Execute a function call to transform() and verify the output.
#
# Task 4:
# Load the transformed dataframe to an output CSV file. Write a function
# load_to_csv(), execute a function call and verify the output.
#
# Task 5:
# Load the transformed dataframe to an SQL database server as a table. Write
# a function load_to_db(), execute a function call and verify the output.
#
# Task 6:
# Run queries on the database table. Write a function run_queries(), execute
# a given set of queries and verify the output.
#
# Task 7:
# Verify that the log entries have been completed at all stages by checking
# the contents of the file code_log.txt.
#
# ============================================================================

# ============================================================================
# Import Required Libraries
# ============================================================================

import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np


# ============================================================================
# Configuration Constants
# ============================================================================

URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_RATE_CSV = "Project Files/exchange_rate.csv"
COLUMNS = ["Name", "MC_USD_Billion"]
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_Banks"
OUTPUT_CSV_PATH = "Largest_banks_data.csv"
LOG_FILE = "code_log.txt"


# ============================================================================
# ETL Function Definitions
# ============================================================================


def log_progress(message):
    """This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    timestamp_format = "%Y-%h-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp},{message}\n")


def extract(url, columns):
    """This function aims to extract the required information from the
    website and save it to a data frame. The function returns the data
    frame for further processing."""
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, "html.parser")

    # Find the heading with id "By_market_capitalization"
    heading = soup.find("span", {"id": "By_market_capitalization"})

    # Navigate to the parent heading, then find the next table
    table = heading.parent.find_next("table")

    # Extract rows from the table body
    rows = table.find("tbody").find_all("tr")

    data = []
    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            name = col[1].get_text(strip=True)
            mc_usd = col[2].get_text(strip=True)
            data.append({"Name": name, "MC_USD_Billion": mc_usd})
    df = pd.DataFrame(data, columns=columns)
    # print(df)
    return df


def transform(df, csv_path):
    """This function accesses the CSV file for exchange rate information,
    and adds three columns to the data frame, each containing the
    transformed version of Market Cap column to respective currencies"""

    # Read exchange rate CSV and create dictionary
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate_df.set_index("Currency")["Rate"].to_dict()

    # Convert MC_USD_Billion to float once (remove commas and convert)
    df["MC_USD_Billion"] = df["MC_USD_Billion"].str.replace(",", "").astype(float)

    # Add columns for each currency conversion
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * exchange_rate_dict["GBP"]).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * exchange_rate_dict["EUR"]).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * exchange_rate_dict["INR"]).round(2)

    return df


def load_to_csv(df, output_path):
    """This function saves the final data frame as a CSV file in the
    provided path. Function returns nothing."""
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    """This function saves the final data frame to a database table with
    the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    """This function runs the query on the database table and prints the
    output on the terminal. Function returns nothing."""
    print(query_statement)
    query_output = pd.read_sql_query(query_statement, sql_connection)
    print(query_output)


# ============================================================================
# Main Execution
# ============================================================================


def main():
    """Main ETL workflow that orchestrates the extraction, transformation,
    and loading process for bank data."""
    try:
        # ETL Process
        log_progress("Preliminaries complete. Initiating ETL process")

        # Extraction phase
        extracted_data = extract(URL, COLUMNS)
        log_progress("Data extraction complete. Initiating Transformation process")

        # Transformation phase
        transformed_data = transform(extracted_data, EXCHANGE_RATE_CSV)
        log_progress("Data transformation complete. Initiating Loading process")

        # Loading phase
        load_to_csv(transformed_data, OUTPUT_CSV_PATH)
        log_progress("Data saved to CSV file")

        with sqlite3.connect(DB_NAME) as conn:
            log_progress("SQL Connection initiated")
            load_to_db(transformed_data, conn, TABLE_NAME)
            log_progress("Data loaded to Database as a table, Executing queries")

            # Query phase
            # Define multiple queries to run
            queries = [
                f"SELECT * FROM {TABLE_NAME}",
                f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}",
                f"SELECT Name from {TABLE_NAME} LIMIT 5",
            ]
            # Execute each query
            for query in queries:
                run_query(query, conn)
                print("\n" + "=" * 50 + "\n")

            log_progress("Process Complete")

        log_progress("Server Connection closed")

    except Exception as e:
        log_progress(f"ETL Job Failed: {e}")


if __name__ == "__main__":
    main()
