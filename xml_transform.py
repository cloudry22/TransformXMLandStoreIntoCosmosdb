# Databricks notebook source
SasURL = "https://apodatalake1.blob.core.windows.net/?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-31T13:42:14Z&st=2023-10-25T05:42:14Z&spr=https&sig=xAaLvtFipDBDHqOUa6kCzD6QQBd%2FztBDqwLoB%2B%2BjsrE%3D"
indQuestionMark = SasURL.index('?')
SasKey = SasURL[indQuestionMark:len(SasURL)]
StorageAccount = "apodatalake1"
ContainerName = "xml"
MountPoint = "/mnt/xml"

dbutils.fs.mount(
  source = "wasbs://%s@%s.blob.core.windows.net/" % (ContainerName, StorageAccount),
  mount_point = MountPoint,
  extra_configs = {"fs.azure.sas.%s.%s.blob.core.windows.net" % (ContainerName, StorageAccount) : "%s" % SasKey}
)

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /mnt/xml
# MAGIC

# COMMAND ----------

file_rdd = spark.read.text("/mnt/xml/books.xml", wholetext=True).rdd

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

# COMMAND ----------

from datetime import datetime
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, DateType)

# COMMAND ----------

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

# COMMAND ----------

book_df = records_rdd.toDF(my_schema)
book_df.show()

# COMMAND ----------

book_df = records_rdd.toDF(my_schema)
book_df.show()

# COMMAND ----------



# COMMAND ----------


