# Azure Data Engineering Project: Olympic Data Processing

## Overview

This project involves the processing of Olympic data using Azure services to create a scalable and secure data engineering pipeline. The primary components include Azure Data Factory, Azure Databricks with Apache Spark, and Azure Data Lake Storage Gen2.

## Workflow

1. **Data Ingestion (Azure Data Factory):**
   - Utilized Azure Data Factory to extract Olympic data from the source.

2. **Data Organization (Azure Data Lake Storage Gen2):**
   - Created two folders within Azure Data Lake Storage Gen2: one for raw data and another for transformed data.

3. **Data Transformation (Azure Databricks with Apache Spark):**
   - Implemented data transformation tasks using Azure Databricks with Apache Spark for efficient processing.

4. **Authentication (App Registration):**
   - Developed an authentication application using Azure Active Directory app registration to ensure secure access to resources.

5. **Data Storage (Azure Data Lake Storage Gen2):**
   - Stored the processed and transformed data securely in Azure Data Lake Storage Gen2.

## Key Features

- **Scalability:**
  - Leveraged Azure Data Lake Storage Gen2 for scalable and high-performance data storage.

- **Security:**
  - Implemented app registration for authentication, enhancing the security of the data lake solution.

- **Organization:**
  - Separated raw and transformed data within Azure Data Lake Storage Gen2 for better data management.

## Instructions for Replication

1. **Data Ingestion:**
   - Set up Azure Data Factory to pull Olympic data from the source.

2. **Data Transformation:**
   - Use Azure Databricks with Apache Spark for data transformation tasks.

3. **Authentication Setup:**
   - Create an Azure Active Directory app registration for secure access.

4. **Data Storage Configuration:**
   - Set up Azure Data Lake Storage Gen2 and configure folders for raw and transformed data.

5. **Run the Pipeline:**
   - Execute the data engineering pipeline to replicate the Olympic data processing workflow.

## Dependencies

- Azure Data Factory
- Azure Databricks with Apache Spark
- Azure Data Lake Storage Gen2
- Azure Active Directory (App Registration)

### References 
Followed Darshil parmer video on Youtube: https://www.youtube.com/watch?v=IaA9YNlg5hM&list=WL&index=25&t=133s&ab_channel=DarshilParmar
