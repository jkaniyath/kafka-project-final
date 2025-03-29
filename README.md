# Kafka-Databricks Stream Project

This project implements a data streaming pipeline using Kafka and Databricks, designed to process data efficiently through a medallion architecture (Bronze, Silver, and Gold layers). The pipeline starts with a Python Kafka producer that reads data from a JSON file and sends messages to Confluent Kafka. Databricks streaming loads this data from Kafka topics, storing raw data in the Bronze layer, cleaning and transforming it in the Silver layer, and aggregating it in the Gold layer.

To ensure data consistency and historical tracking, the project applies the Slowly Changing Dimension Type 2 (SCD2) approach to a specific table where historical data needs to be maintained. It also leverages Change Data Capture (CDC) in Delta Lake to read data efficiently. For performance optimization, Delta tables, liquid clustering, and Databricks best practices are implemented to enhance query performance and storage efficiency.

The project also includes Databricks job execution using asset bundles, allowing for efficient job deployment and management, and demonstrates how to develop and deploy using a CI/CD pipeline, ensuring seamless integration, testing, and automated deployment of Databricks workflows.

## Pre-requisites

- Confluent cloud account
- Azure account
- Github account
- VS code
- Python 3.11
- Git installed
- Databricks CLI installed

## 1. Python Kafka Producer
