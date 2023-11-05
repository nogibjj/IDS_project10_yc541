# Email Data Analysis with PySpark

This repository contains a PySpark script for analyzing an email dataset. The script performs basic data processing, executes a Spark SQL query to find the most common words, and transforms data into a suitable format for further analysis.

## Dataset

The dataset is assumed to contain various statistics related to emails, such as the most common words in the email body and counts of unique values.

## Prerequisites

Before running the script, ensure you have the following installed:
- Python 3.x
- PySpark
- A Spark cluster (local mode is sufficient for small datasets)

## Script

`email_analysis.py` is the main script of the project. It reads the data from a CSV file, processes it, and saves the results.

### Features

- Reading data from a CSV file
- Selecting relevant columns for analysis
- Executing a Spark SQL query to determine the most common words
- Transforming column data types
- Writing the processed data back to a CSV file

### Running the Script

To run the script, use the following command in the terminal:

```bash
spark-submit email_analysis.py