# Airflow Data Pipeline Project

![Screenshot from 2024-10-03 02-57-15](https://github.com/user-attachments/assets/f94ed0d9-9cf1-4406-bbd4-35ac800d1eb3)

This project is a demonstration of an end-to-end data pipeline using **Apache Airflow**. The pipeline generates synthetic customer, product, store, and transaction data, loads it into a PostgreSQL database, calculates various metrics, sends a report via email, and then automatically deletes the generated files at the end of the process.

## Project Components

- **Apache Airflow** for orchestrating the pipeline.
- **PostgreSQL** as the database.

- **Faker** for generating synthetic data.
- **Python** for creating tasks to generate data, calculate metrics, send emails, and clean up files.
- **Pandas** for data processing.

## Prerequisites

- Docker
- Docker Compose
- Python 3.x
- Apache Airflow installed
- PostgreSQL database setup (with appropriate credentials)

## Data Pipeline Overview

1. **Create Tables:** A task to create the necessary tables in PostgreSQL for customers, products, stores, and transactions.
2. **Generate Data:** Tasks to generate synthetic data for customers, products, stores, and transactions using the Faker Python package.
3. **Calculate Metrics:** A task that reads the generated transaction data and calculates various metrics like the total transaction amount, top customer, and average transaction value.
4. **Send Email:** A task that sends an email report with the calculated metrics.
5. **Delete Files:** A final task that cleans up all the generated CSV and report files from the data folder.


![Screenshot from 2024-10-03 02-37-34](https://github.com/user-attachments/assets/baaf4a99-baff-47e6-98fb-1179fc906cf8)

