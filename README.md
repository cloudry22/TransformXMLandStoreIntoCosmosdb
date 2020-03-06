# XML File transformation and storing data into Azure CosmosDB - Sample code

The project transforms sample xml file(books.xml) using Apache spark managed by Azure Databricks. Post transformation it stores the data to Azure CosmosDB.

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

- Before you begin running the code in your Azure databricks notebook please make sure you have updated your Azure resources details in the code as necessary. 

### Step 1: Creating a Shared Access Signature (SAS) URL

Azure provides you with a secure way to create and share access keys for your Azure Blob Store without compromising your account keys. for this demo create a unique storage account and upload book.xml file shared with you in the blob container.

More details are provided <a href="http://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1" target="_blank"> in this document</a>.

This allows access to your Azure Blob Store data directly from Databricks distributed file system (DBFS).

As shown in the screen shot, in the Azure Portal, go to the storage account containing the blob to be mounted. Then:

1. Select Shared access signature from the menu.
2. Click the Generate SAS button.
3. Copy the entire Blob service SAS URL to the clipboard.
4. Use the URL in the mount operation, as shown below.

<img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-sas-keys.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; margin-top: 20px; padding: 10px"/>

### Step 2: Paste the following code in your Azure databricks notebook and update the code as instructed. 

    SasURL = "<Paste blob service SAS URL here>"
    indQuestionMark = SasURL.index('?')
    SasKey = SasURL[indQuestionMark:len(SasURL)]
    StorageAccount = "<Enter your storage account name>"
    ContainerName = "<Enter your container name>"
    MountPoint = "/mnt/<enter your mount point name>"

    dbutils.fs.mount(
      source = "wasbs://%s@%s.blob.core.windows.net/" % (ContainerName, StorageAccount),
      mount_point = MountPoint,
      extra_configs = {"fs.azure.sas.%s.%s.blob.core.windows.net" % (ContainerName, StorageAccount) : "%s" % SasKey}
    )

### Step 3: Paste the following line into your Azure Databricks notebook to mount Azure Datalake Storage Gen2 container

    %fs mounts

### Step 4: Paste the following line into your Azure Databricks notebook to see the list of file under your mountpoint

    %fs ls /mnt/<enter your mount point name here>
  
### Step 5: Paste the following code into your Azure Databricks notebook to parse xml tree, extract the records and transform to new RDD

    file_rdd = spark.read.text("/mnt/<Enter your mountpoint name>/books.xml", wholetext=True).rdd

    def parse_xml(rdd):
        """
        Read the xml string from rdd, parse and extract the elements,
        then return a list of list.
        """
        results = []
        root = ET.fromstring(rdd[0])

        for b in root.findall('book'):
            rec = []
            rec.append(b.attrib['id'])
            for e in ELEMENTS_TO_EXTRAT:
                if b.find(e) is None:
                    rec.append(None)
                    continue
                value = b.find(e).text
                if e == 'publish_date':
                    value = datetime.strptime(value, '%Y-%m-%d')
                rec.append(value)
            results.append(rec)

        return results
  
      records_rdd = file_rdd.flatMap(parse_xml)

### Step 6: Paste the following code into your Azure Databricks notebook to define the schema

    from datetime import datetime
    import xml.etree.ElementTree as ET
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (StructType, StructField, StringType, DateType)

### columns for the DataFrame

    COL_NAMES = ['book_id', 'author', 'title', 'genre', 'price', 'publish_date', 'description']
    ELEMENTS_TO_EXTRAT = [c for c in COL_NAMES if c != 'book_id']

    def set_schema():
        """
        Define the schema for the DataFrame
        """
        schema_list = []
        for c in COL_NAMES:
            if c == 'publish_date':
                schema_list.append(StructField(c, DateType(), True))
            else:
                schema_list.append(StructField(c, StringType(), True))
    
        return StructType(schema_list)
    if __name__ == "__main__":
        spark = SparkSession\
            .builder\
            .getOrCreate()

    my_schema = set_schema()

### Step 7: Paste the following code into your Azure Databricks notebook to convert RDDs to Dataframe with the pre-defined schema

    book_df = records_rdd.toDF(my_schema)
    book_df.show()

### Step 8: Paste the following code into your Azure Databricks notebook to write Dataframe to your mountpoint

    book = book_df.write.parquet("/mnt/<enter your mountpoint name here>/output/book.parquet")
  
### Step 9: Follow the below instruction to create Azure Key Vault

### Key Vault-backed secret scopes

Azure Databricks has two types of secret scopes: Key Vault-backed and Databricks-backed. These secret scopes allow you to store secrets, such as database connection strings, securely. If someone tries to output a secret to a notebook, it is replaced by `[REDACTED]`. This helps prevent someone from viewing the secret or accidentally leaking it when displaying or sharing the notebook.

### Setup

To begin, you will need to create the required resources in the Azure portal. Follow the steps below to create a Key Vault service, and Azure Cosmos DB.

### Azure Key Vault

1. Open <https://portal.azure.com> in a new browser tab.
2. Select the **Create a resource** option on the upper left-hand corner of the Azure portal

    ![Output after Key Vault creation completes](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-keyvault.png)
3. In the Search box, enter **Key Vault**.
4. From the results list, choose **Key Vault**.
5. On the Key Vault section, choose **Create**.
6. On the **Create key vault** section provide the following information:
    - **Name**: A unique name is required. For this quickstart we use **Databricks-Demo**. 
    - **Subscription**: Choose a subscription.
    - Under **Resource Group** select the resource group under which your Azure Databricks workspace is created.
    - In the **Location** pull-down menu, choose the same location as your Azure Databricks workspace.
    - Check the **Pin to dashboard** checkbox.
    - Leave the other options to their defaults.
    
    ![Key Vault creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-keyvault-form.png)
7. After providing the information above, select **Create**.
8. After Key Vault is created, open it and navigate to **Properties**.
9. Copy the **DNS Name** and **Resource ID** properties and paste them to Notebook or some other text application that you can **reference later**.

    ![Key Vault properties](https://databricksdemostore.blob.core.windows.net/images/04-MDW/keyvault-properties.png)
    
### Step 10: Follow the below instruction to create Azure Cosmos DB

1. In the Azure portal, select the **Create a resource** option on the upper left-hand corner.

    ![Output after Azure Cosmos DB creation completes](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-cosmos-db.png)
2. In the Search box, enter **Cosmos DB**.
3. From the results list, choose **Cosmos DB**.
4. On the SQL Database section, choose **Create**.
5. On the **Create Cosmos DB** section provide the following information:
    - **Subscription**: Choose a subscription.
    - Under **Resource Group** select the resource group under which your Azure Databricks workspace is created.
    - **Account Name**: A unique name is required. For this quickstart we use **databricks-demo**.
    - In the **API** pull-down menu, choose **Core (SQL)**.
    - In the **Location** pull-down menu, choose the same location as your Azure Databricks workspace.
    - For **Geo-Redundancy**, select **Enable**.
    - For **Multi-region Writes**, select **Enable**.
    - Leave the other options to their defaults.
    
    ![Cosmos DB creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-cosmos-db-form.png)
6. After providing the information above, select **Review + create**.
7. In the review section, select **Create**.
8. After Cosmos DB is created, navigate to it and select **Data Explorer** on the left-hand menu.
9. Select **New Database**, then provide the following options in the form:
    - **Database id**: demos
    - **Provision throughput**: checked
    - **Throughput**: 10000
    
    ![Cosmos DB New Database creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-new-database.png)
10. Select **OK**, then select **New Collection** to add a collection to the database, then provide the following options in the form:
    - **Database id**: select **Use existing** then select **demos**
    - **Collection id**: books
    - **Partition key**: id
    - **Provision dedicated throughput for this collection**: unchecked
    - **Utilize all of the available database throughput for this collection**: checked
    
    ![Cosmos DB New Collection creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-new-collection.png)
11. Select **OK**.
12. Select **Keys** on the left-hand menu of your new Cosmos DB instance and copy the **URI** and **Primary Key** (or Secondary Key) values and paste them to Notebook or some other text application that you can **reference later**. What you need for connecting to Cosmos DB are:
    - **Endpoint**: Your Cosmos DB url (i.e. https://youraccount.documents.azure.com:443/)
    - **Masterkey**: The primary or secondary key string for you Cosmos DB account
    - **Database**: The name of the database (`demos`)
    - **Collection**: The name of the collection that you wish to query (`documents`)
    
    ![Cosmos DB Keys](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-keys.png)

### Step 11: Configure Azure Databricks to access Azure Key Valut and CosmosDB

Now that you have an instance of Key Vault up and running, it is time to let Azure Databricks know how to connect to it.

1. The first step is to open a new web browser tab and navigate to https://<your_azure_databricks_url>#secrets/createScope (for example, https://westus.azuredatabricks.net#secrets/createScope).
2. Enter the name of the secret scope, such as `key-vault-secrets`.
3. Select **Creator** within the Manage Principal drop-down to specify only the creator (which is you) of the secret scope has the MANAGE permission.

  > MANAGE permission allows users to read and write to this secret scope, and, in the case of accounts on the Azure Databricks Premium Plan, to change permissions for the scope.

  > Your account must have the Azure Databricks Premium Plan for you to be able to select Creator. This is the recommended approach: grant MANAGE permission to the Creator when you create the secret scope, and then assign more granular access permissions after you have tested the scope.

4. Enter the **DNS Name** (for example, https://databricks-demo.vault.azure.net/) and **Resource ID** you copied earlier during the Key Vault creation step, for example: `/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourcegroups/azure-databricks/providers/Microsoft.KeyVault/vaults/Databricks-Demo`. If this is a preconfigured environment, you do not need to complete this step.

  ![Create Secret Scope form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-secret-scope.png 'Create Secret Scope')
5. Select **Create**.

After a moment, you will see a dialog verifying that the secret scope has been created.

### Step 12: Create secrets in Key Vault

To create secrets in Key Vault that can be accessed from your new secret scope in Databricks, you need to either use the Azure portal or the Key Vault CLI. For simplicity's sake, we will use the Azure portal:

1. In the Azure portal, navigate to your Key Vault instance.
2. Select **Secrets** in the left-hand menu.
3. Select **+ Generate/Import** in the Secrets toolbar.
4. Provide the following in the Create a secret form, leaving all other values at their defaults:
  - **Name**: cosmos-uri
  - **Value**: the URI link for your Cosmos server
  
  ![Create a secret form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-secret.png 'Create a secret')
5. Select **Create**.

**Repeat steps 3 - 5** to create the following secrets:

| Secret name | Secret value |
| --- | --- |
| cosmos-key | the key value for your Azure Cosmos DB instance |

When you are finished, you should have the following secrets listed within your Key Vault instance:

![List of Key Vault secrets](https://databricksdemostore.blob.core.windows.net/images/04-MDW/keyvault-secrets.png 'Key Vault secrets')

### Step 13: Azure Cosmos DB - Connect using Key Vault

In an earlier lesson ([Key Vault-backed Secret Scopes]($./08-Key-Vault-backed-secret-scopes)), you created a Key Vault-backed secret scope for Azure Databricks, and securely stored your Azure Cosmos DB key within.

In this lesson, you will use the Azure Cosmos DB secrets that are securely stored within the Key Vault-backed secret scope to connect to your Azure Cosmos DB instance. Next, you will use the DataFrame API to execute SQL queries and control the parallelism of reads through the JDBC interface.

If you are running in an Azure Databricks environment that is already pre-configured with the libraries you need, you can skip to the next cell. To use this notebook in your own Databricks environment, you will need to create libraries, using the [Create Library](https://docs.azuredatabricks.net/user-guide/libraries.html) interface in Azure Databricks. Follow the steps below to attach the `azure-eventhubs-spark` library to your cluster:

1. In the left-hand navigation menu of your Databricks workspace, select **Workspace**, select the down chevron next to **Shared**, and then select **Create** and **Library**.

  ![Create Databricks Library](https://databricksdemostore.blob.core.windows.net/images/08/03/databricks-create-library.png 'Create Databricks Library')

2. On the New Library screen, do the following:

  - **Source**: Select Maven Coordinate.
  - **Coordinate**: Enter "azure-cosmosdb-spark", and then select **com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:2.0.2** (or later version).
  - Select **Create Library**.
  
  ![Databricks new Maven library](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-create-library.png 'Databricks new Maven library')

3. On the library page that is displayed, check the **Attach** checkbox next to the name of your cluster to run the library on that cluster.

  ![Databricks attach library](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-attach-library.png 'Databricks attach library')

Once complete, return to this notebook to continue with the lesson.

### Step 14: Access Key Vault secrets and configure the Cosmos DB connector

In a previous lesson, you created two secrets in Key Vault for your Azure Cosmos DB instance: **cosmos-uri** and **cosmos-key**. These two values will be used to configure the Cosmos DB connector. Let's start out by retrieving those values and storing them in new variables. Enter the following two lines of code in your Azure databricks cell and run it.

    uri = dbutils.secrets.get(scope = "key-vault-secrets", key = "cosmos-uri")
    key = dbutils.secrets.get(scope = "key-vault-secrets", key = "cosmos-key")

### Step 15: Paste the following code to your Azure Databricks notebook cell to Write transformed data to Cosmos DB

    readDF = spark.read.parquet("/mnt/<Enter your mountpoint name here>/output/book.parquet")
    writeConfig = {
    "Endpoint" : uri,
    "Masterkey" : key,
    "Database" : "<Enter your Cosmos DB name here>",
    "Collection" : "<Enter your CosmosDB container name here>",
    "Upsert" : "true"
    }
    (readDF
    .write
    .format("com.microsoft.azure.cosmosdb.spark")
    .mode("overwrite")
    .options(**writeConfig)
    .save()
    )
  
### Step 16: Go to Azure portal and access your CosmosDB and go to data explorer. You will be able to see the data under your collection name.
