# Phase2-Virtual--Interview-Assignment-Javed-Akhtar

Virtual Assignment — Javed Akhtar

This repository contains my completed virtual interview assignment divided into four major parts. Parts 2, 3, and 4 involve running SQL scripts on Microsoft SQL Server using SQL Server Management Studio (SSMS).

Repository Structure
/part1-data-quality/
  - code/                      # optional Python or SQL files
  - documentation.md


/part2-transformation/
  - code/                      # SQL scripts for transformations
  - architecture-diagram.png   # or PDF
  - documentation.md


/part3-analysis/
  - executive-summary.pdf
  - code/                      # SQL scripts for analysis
  - supporting-analysis/


/part4-monitoring/
  - code/                      # SQL scripts for monitoring checks
  - documentation.md
How to Run (MS SQL Server + SSMS)

All SQL-based tasks in Part 2, Part 3, and Part 4 require:

Microsoft SQL Server (any edition)

SQL Server Management Studio (SSMS)

The provided dataset file events.csv

Step 1 — Import events.csv into SQL Server

Open SQL Server Management Studio (SSMS).

Connect to your SQL instance.

Right‑click on the target database → Tasks → Import Flat File.

Select events.csv.

Follow the wizard to create the table automatically.

Verify import using:

SELECT TOP 100 * FROM events;
Step 2 — Run SQL Scripts for Each Part

Each folder contains .sql files. Run them individually:

Open SSMS.

File → Open → select the .sql file.

Click Execute.

Outputs (tables or views) will be created as defined in the script.
---

# How to use this document

Copy the README content into the repository `README.md`. For each folder, copy the `documentation.md` content into the corresponding file. Replace placeholder URLs and your GitHub username where indicated. Update the requirements and config examples with any real values or environment instructions.

Good luck — and tell me if you want me to generate the `requirements.txt` templates, example `config.yml`, or a zipped repo structure you can download.
