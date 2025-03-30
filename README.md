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
### Overview
This Python application reads messages from a JSON file line by line and sends them to a Kafka topic hosted on Confluent Cloud. The user specifies the JSON file, target Kafka topic, key column, and the number of messages to send via command-line arguments. All Confluent Kafka secrets are managed securely using the Python **dotenv** library.

### Features
- Reads JSON data and sends each line as a Kafka message.

- Supports specifying a key column from the JSON file to use as the message key.

- Allows sending a limited number of messages or the entire file.

- Uses Confluent Cloud as the Kafka broker.

### Pre-requisites

#### 1. Set Up Confluent Cloud Kafka Topics
You need a Confluent Cloud account and create a cluster along with the following Kafka topics. For detailed instructions on how to create topics in Confluent Cloud, refer to the official documentation: [Create Kafka Topics.](https://docs.confluent.io/platform/current/control-center/topics/create.html)
- books
- customers
- orders

#### 2. Install Dependencies
Ensure you have Python installed along with **pip** and **setuptools**, then install the required dependencies:
```
pip install setuptools
```

#### 3. Clone the Repository and Install
You can find the project's GitHub repository at the following link:
<repository_url>

Clone the project and install the required dependencies using setup.py (all necessary libraries are included in setup.py):

```
git clone https://github.com/jkaniyath/kafka_producer.git
cd kafka_producer
python setup.py install
```
### Usage
Run the script with the required arguments:

```
python main.py <json_file> <topic> <key_column_name> <counts>
```
Arguments:

- json_file (str): Path to the JSON file containing messages.

- topic (str): Kafka topic name where messages should be sent.

- key_column_name (str): Column from the JSON file to use as the message key.

- counts (int): Number of messages to send (0 to send all lines).

Example Usage

To send the first 100 messages from orders.json to the orders topic using order_id as the key:

```
python main.py orders.json orders order_id 100
```

Run main.py --help for more details:

```
python main.py --help
```

