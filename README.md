# Kafka-Databricks Stream Project
This project implements a data streaming pipeline using Kafka and Databricks, designed to process data efficiently through a medallion architecture (Bronze, Silver, and Gold layers). The pipeline starts with a Python Kafka producer that reads data from a JSON file and sends messages to Confluent Kafka. Databricks streaming loads this data from Kafka topics, storing raw data in the Bronze layer, cleaning and transforming it in the Silver layer, and aggregating it in the Gold layer.

To ensure data consistency and historical tracking, the project applies the Slowly Changing Dimension Type 2 (SCD2) approach to a specific table where historical data needs to be maintained. It also leverages Change Data Capture (CDC) in Delta Lake to read data efficiently. For performance optimization, Delta tables, liquid clustering, and Databricks best practices are implemented to enhance query performance and storage efficiency.

The project also includes Databricks job execution using asset bundles, allowing for efficient job deployment and management, and demonstrates how to develop and deploy using a CI/CD pipeline, ensuring seamless integration, testing, and automated deployment of Databricks workflows.

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
You need Git installed, a Confluent Cloud account, and create a cluster along with the following Kafka topics. For detailed instructions on how to create topics in Confluent Cloud, refer to the official documentation: [Create Kafka Topics.](https://docs.confluent.io/platform/current/control-center/topics/create.html)
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
[repository_url](https://github.com/jkaniyath/kafka_producer.git)

Clone the project and install the required dependencies using setup.py (all necessary libraries are included in setup.py):

```
git clone https://github.com/jkaniyath/kafka_producer.git
cd kafka_producer
pip install -e .
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

## 2. Databricks Stream

### Overview
This Spark Streaming application consumes messages from Confluent Cloud Kafka topics (books, consumers, and orders), stores them in the bronze layer, processes them in the silver layer, and aggregates them in the gold layer.

The schemas for these layers are structured within Unity Catalog and stored in an external location on Azure Storage Gen2. All Confluent Cloud secrets are managed using Databricks secrets. The complete source code and details are available in the [GitHub repository](https://github.com/jkaniyath/kafka-project-final)

### Prerequisites

- Ensure the following prerequisites are met before setting up the application:

- A Databricks workspace enabled for Unity Catalog

- Databricks CLI installed

- GitHub account

- VS Code

- Python 3.11 (preferable)

### Setup Steps

#### 1. Create an Azure Storage Account

Set up an Azure Storage Account to store external data locations and managed tables.

#### 2. Create Storage Containers
Create five containers in Azure Storage for different purposes:

- Metastore Details: Stores metadata for Unity Catalog.

- Schema Storage: Create directories named bronze, silver, and gold for storing schemas and managed table data.

- Checkpoints: Stores checkpoint information for the streaming application.

- Landing Zone: Stores static table data (e.g., country details).

- Logging: Stores application logs.

#### 3. Set Up Databricks and Unity Catalog

- Create a Premium Databricks account.

- Create an Access Connector for Azure Databricks to access Azure Storage. [Refer to the official documentation](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/azure-managed-identities)

- Enable Unity Catalog in Databricks. [Follow the setup guide](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/enable-workspaces)

- Define following external locations in Databricks:

    bronze, silver, gold, checkpoints, landing, logger.

- Create catalog in Databricks:

    Example: dev for development, prod for production.

#### 4. Clone the GitHub Repository
Clone the project repository:
```
git clone https://github.com/jkaniyath/kafka-project-final.git
```
#### 5. Install Dependencies
Navigate to the project directory and install dependencies:
```
pip install -e .
```

#### 6. Development Environment Setup
This project is developed using VS Code with the Databricks extension. Refer to the [VS Code Databricks extension guide](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/)

#### 7. Run Unit Tests
This project uses pytest for unit testing. Run unit tests using Databricks connect.

#### 8. Running Databricks Jobs

After successfully running unit tests:

- Execute Databricks jobs in the development environment using Databricks Asset Bundles. [See the documentation](https://docs.databricks.com/aws/en/dev-tools/bundles)

- Run Databricks Asset Bundles in CI/CD using github action [Run a CI/CD workflow with a Databricks Asset Bundle and GitHub Actions](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/ci-cd-bundles):<br />
    This project follows the process below:

    - Execute unit tests.

    - Validate the asset bundle.

    - Deploy to the staging environment.
