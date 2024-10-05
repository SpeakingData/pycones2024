# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %pip install jinja2

# COMMAND ----------

import sys
import os

root_path = os.path.abspath('.')
sys.path.append(f"{root_path}/shared/")

# COMMAND ----------

import functools as ft
from functools import reduce
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from dataclasses import dataclass
from databricks import lakehouse_monitoring as lm
from delta_utils import stream_merge_into_delta 
from table_constraints_utils import create_constraints_table

# COMMAND ----------

import dbldatagen as dg

@dataclass
class DataGeneratorEnvironment:
    schema: str
    num_clients: int = 100
    num_products: int = 10
    num_sales: int = 1000
    spark: SparkSession = None  # This assumes you'll inject a Spark session externally
    table_constraints_name = 'tableConstraints'
    quarantine_schema = 'quarantine'

    def initialize_environment(self):

        # Create Table constraints
        create_constraints_table(self.spark, self.schema, f"{self.schema}.{self.table_constraints_name}")

        # Client Dimension Table
        bronze_schema = dg.DataGenerator(self.spark, rows=self.num_sales, seedMethod="hash_fieldname")
        bronze_schema = (bronze_schema
            .withColumn("client_id", "long", uniqueValues=self.num_clients)
            .withColumn("first_name", "string", values=["John", "Jane", "Alex", "Chris", "Katie", "James", "Sarah"], random=True)
            .withColumn("last_name", "string", values=["Smith", "Doe", "Johnson", "Lee", "Brown", "Williams"], random=True)
            .withColumn("gender", "string", values=["M", "F"], random=True)
            .withColumn("age", "int", minValue=18, maxValue=80, random=True)
            .withColumn("email", "string", expr="concat(first_name, '.', last_name, '@example.com')")
            .withColumn("signup_date", "date", begin="2020-01-01", end="2024-09-01", random=True)
            .withColumn("address", "string", values=["New York", "California", "Texas", "Florida", "Illinois"], random=True)
            .withColumn("product_id", "long", uniqueValues=self.num_products)
            .withColumn("product_name", "string", values=["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"], random=True)
            .withColumn("category", "string", values=["Electronics", "Home", "Toys", "Clothing", "Books"], random=True)
            .withColumn("price", "float", minValue=10.0, maxValue=2000.0, random=True)
            .withColumn("date", "date", begin="2021-01-01", end="2024-12-31", random=True)
            .withColumn("sales_id", "long", uniqueValues=self.num_sales)
            # .withColumn("client_id", "int", minValue=1, maxValue=self.num_clients, random=True)
            # .withColumn("product_id", "int", minValue=1, maxValue=self.num_products, random=True)
            .withColumn("quantity", "int", minValue=1, maxValue=10, random=True)
            .withColumn("sale_amount", "float", expr="quantity * rand() * 200")
        )

        # Generate Data
        bronze_df = bronze_schema.build()

        # Save to tables
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}_{self.quarantine_schema}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS monitoring")
        bronze_df.write.mode("overwrite").saveAsTable(f"{self.schema}.bronze_data_quality")
        self.spark.sql(f"ALTER TABLE {self.schema}.bronze_data_quality SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    def append_new_sales_bronze(self, num_records=1000):
        # Get max sales_id from current table
        sales_max_id = self.spark.table(f"{self.schema}.bronze_data_quality").agg(F.max("sales_id")).collect()[0][0]

        # Generate new sales records
        bronze_schema = dg.DataGenerator(self.spark, rows=num_records, seedMethod="hash_fieldname")
        bronze_schema = (bronze_schema
            .withColumn("client_id", "long", uniqueValues=self.num_clients, percentNulls=0.3)
            .withColumn("first_name", "string", values=["John", "Jane", "Alex", "Chris", "Katie", "James", "Sarah"], random=True)
            .withColumn("last_name", "string", values=["Smith", "Doe", "Johnson", "Lee", "Brown", "Williams"], random=True)
            .withColumn("gender", "string", values=["M", "F"], random=True)
            .withColumn("age", "int", minValue=18, maxValue=80, random=True)
            .withColumn("email", "string", expr="concat(first_name, '.', last_name, '@example.com')")
            .withColumn("signup_date", "date", begin="2020-01-01", end="2024-12-31", random=True, percentNulls=0.4)
            .withColumn("address", "string", values=["New York", "California", "Texas", "Florida", "Illinois", "Washington"], random=True)
            .withColumn("product_id", "long", uniqueValues=self.num_products, percentNulls=0.2)
            .withColumn("product_name", "string", values=["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"], random=True)
            .withColumn("category", "string", values=["Electronics", "Home", "Toys", "Clothing", "Books"], random=True)
            .withColumn("price", "float", minValue=10.0, maxValue=2000.0, random=True)
            .withColumn("date", "date", begin="2024-01-01", end="2024-10-06", random=True)
            .withColumn("sales_id", "long", uniqueValues=num_records, minValue=sales_max_id + 1)
            .withColumn("quantity", "int", minValue=1, maxValue=10, random=True)
            .withColumn("sale_amount", "float", expr="quantity * rand() * 200")
        )

        # Generate new data and append to table
        bronze_df = bronze_schema.build()
        bronze_df.write.mode("append").saveAsTable(f"{self.schema}.bronze_data_quality")

    def clean_environment(self, full_reset: bool = False):
        for table in [y for (x, y, z) in self.spark.sql(f"SHOW TABLES IN {self.schema}").collect()]:
            if full_reset:
                self.spark.sql(f"DROP TABLE IF EXISTS {self.schema}.{table}")
            else:
                self.spark.sql(f"TRUNCATE TABLE {self.schema}.{table}")
        for table in [y for (x, y, z) in self.spark.sql(f"SHOW TABLES IN {self.schema}_{self.quarantine_schema}").collect()]:
            if full_reset:
                self.spark.sql(f"DROP TABLE IF EXISTS {self.schema}_{self.quarantine_schema}.{table}")
            else:
                self.spark.sql(f"TRUNCATE TABLE {self.schema}_{self.quarantine_schema}.{table}")
        dbutils.fs.rm("/tmp/", True)

    def process_bronze_to_silver(self, bronze_table ,silver_table, cols, ids ,checkpoint_path):
        df = self.spark.readStream.table(f"{self.schema}.{bronze_table}").select(cols)
        query = (
            df
            .dropDuplicates(ids)
            .writeStream
            .format('delta')
            .option("checkpointLocation", f"{checkpoint_path}/{silver_table}/")
            .foreachBatch(lambda batch_df, batch_id: stream_merge_into_delta(spark=spark, batch_df=batch_df, batch_id=batch_id, table_schema=self.schema, table_name=silver_table, ids=ids, max_col=None, lakehouse_iq_enabled=False))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
