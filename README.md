# XML File transformation and storing data into Azure CosmosDB - Sample code

The project transforms sample xml file(book.xml) using Apache spark managed by Azure Databricks. Post transformation it stores the data to Azure CosmosDB.

## Features

This project framework provides the following features:

* Store xml files into Azure Data Lake Storage Gen2
* Transform XML files into Spark RDD
* Store transformed data into Azure CosmosDB

## Getting Started

To begin with you need download book.xml file from this repository and upload it under Azure Data Lake Storage Gen2 storage account.

### Prerequisites

- Azure subscription
- Azure Data Lake Storage Gen2 stroage account
- Azure Databricks spark cluster
- Azure CosmosDB
- Familiarity with python, Azure Databricks, Azure Key Valut store and Apache Spark 

### Installation

- Create Apache spark cluster using Azure Databricks by following this link - https://docs.microsoft.com/en-us/azure/azure-databricks/quickstart-create-databricks-workspace-portal

### Step by Step Guide

%md-sandbox

### Creating a Shared Access Signature (SAS) URL

Azure provides you with a secure way to create and share access keys for your Azure Blob Store without compromising your account keys. for this demo create a unique storage account and upload book.xml file shared with you in the blob container.

More details are provided <a href="http://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1" target="_blank"> in this document</a>.

This allows access to your Azure Blob Store data directly from Databricks distributed file system (DBFS).

As shown in the screen shot, in the Azure Portal, go to the storage account containing the blob to be mounted. Then:

1. Select Shared access signature from the menu.
2. Click the Generate SAS button.
3. Copy the entire Blob service SAS URL to the clipboard.
4. Use the URL in the mount operation, as shown below.

<img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-sas-keys.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; margin-top: 20px; padding: 10px"/>
