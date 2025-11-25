Part 1 — Data Quality

Purpose

This folder contains scripts and notebooks used for data quality checks and cleaning. It demonstrates how input datasets were validated, common issues discovered, and corrective actions applied.

Files

code/ — contains scripts and notebooks (e.g., dq_checks.py, dq_notebook.ipynb)

documentation.md — this file

Environment

Python 3.9+ recommended

Install requirements:

pip install -r part1-data-quality/code/requirements.txt

How to run

Place sample data in part1-data-quality/code/data/ (if included).

Run basic checks:

python part1-data-quality/code/dq_checks.py --input part1-data-quality/code/data/input.csv --output part1-data-quality/code/data/report.csv

For notebooks, open dq_notebook.ipynb and run cells in order.

Output

report.csv — summary of quality checks

example cleaned dataset: cleaned_input.csv

Notes

Key checks included: missing values, type mismatches, outliers, duplicates, referential integrity.

See code/dq_checks.py for detailed validation rules.
