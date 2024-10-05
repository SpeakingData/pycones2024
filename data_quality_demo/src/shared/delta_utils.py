from functools import reduce
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import Window, Column, DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import explode, col
from table_constraints_utils import apply_table_constaints, get_table_constraints_conditions
import logging
import sys

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add console handler for Databricks output
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Ensure no duplicate handlers
if not logger.hasHandlers():
    logger.addHandler(console_handler)


def stream_merge_into_delta(
    spark: SparkSession, 
    batch_df: DataFrame, 
    batch_id: int, 
    table_schema: str, 
    table_name: str, 
    ids: list, 
    max_col: str = None, 
    lakehouse_iq_enabled: bool = False, 
    quarantine_schema: str = "quarantine", 
    table_constraints_name: str = "tableConstraints"
) -> None:
    """
    Merge incoming streaming batch into a Delta table, handling deduplication, quarantine of invalid records, and optional lakehouse monitoring.

    Args:
        spark (SparkSession): Spark session.
        batch_df (DataFrame): The input batch dataframe.
        batch_id (int): Unique ID of the current batch.
        table_schema (str): Schema of the Delta table.
        table_name (str): Name of the Delta table.
        ids (list): List of column names that uniquely identify records.
        max_col (str, optional): Column name to use for deduplication, if applicable. Defaults to None.
        lakehouse_iq_enabled (bool, optional): Enable Lakehouse IQ monitoring. Defaults to False.
        quarantine_schema (str, optional): Schema where quarantine records will be stored. Defaults to "quarantine".
        table_constraints_name (str, optional): Name of the table constraints for validation. Defaults to "tableConstraints".

    Returns:
        None: This function doesn't return anything. It merges the batch into the Delta table.
    """
    logger.info("Starting stream_merge_into_delta for table: %s.%s", table_schema, table_name)
    
    # Step 1: Dedupe input batch, use max_col if provided
    if max_col:
        logger.info("Deduplication using max column: %s", max_col)
        w = Window.partitionBy(ids).orderBy(col(max_col).desc())
        df_dedupe = (
            batch_df
            .withColumn("rank", F.rank().over(w))
            .filter(col("rank") == 1)
            .drop("rank")
        )
    else:
        logger.info("Deduplication using primary keys: %s", ids)
        df_dedupe = batch_df.dropDuplicates(ids)

    # Step 2: Check if Delta table exists
    if spark.catalog.tableExists(f"{table_schema}.{table_name}"):
        logger.info("Delta table exists: %s.%s", table_schema, table_name)
        delta = DeltaTable.forName(spark, f"{table_schema}.{table_name}")
    else:
        # Step 3: Create Delta table if it doesn't exist
        logger.info("Creating Delta table: %s.%s", table_schema, table_name)
        delta = (DeltaTable.create(spark)
                 .tableName(f"{table_schema}.{table_name}")
                 .addColumns(df_dedupe.schema)
                 .execute())
        
        # Apply table constraints after table creation
        apply_table_constaints(spark, f"{table_schema}.{table_constraints_name}", table_name)

        # Step 4: Enable Lakehouse IQ monitoring if specified
        if lakehouse_iq_enabled:
            try:
                logger.info("Enabling Lakehouse IQ monitoring for table: %s.%s", table_schema, table_name)
                lm.create_monitor(
                    table_name=f"{table_schema}.{table_name}",
                    profile_type=lm.Snapshot(),
                    output_schema_name=f"monitoring",
                    schedule=lm.MonitorCronSchedule(
                        quartz_cron_expression="0 0 0 ? * SUN",  # Schedule refresh every Sunday at midnight
                        timezone_id="PST",
                    ),
                )
            except Exception as e:
                logger.error("Error creating monitor in Lakehouse IQ: %s", e)
                raise Exception(f"Error creating monitor in Lakehouse IQ for table {table_schema}.{table_name}: {e}")

    # Step 5: Quarantine invalid records based on table constraints
    constraints_conditions = get_table_constraints_conditions(spark, table_schema, table_name)
    quarantine_records = df_dedupe.filter(constraints_conditions)
    valid_records = df_dedupe.filter(~constraints_conditions)

    # Write quarantine records to the specified quarantine schema
    quarantine_records.write.mode("append").saveAsTable(f"{table_schema}_{quarantine_schema}.{table_name}")
    
    logger.info("Total Records: %d", df_dedupe.count())
    logger.info("Records moved to quarantine: %d", quarantine_records.count())
    logger.info("Valid Records: %d", valid_records.count())

    # Step 6: Create merge condition based on primary keys (ids)
    condition = " AND ".join(f"l.{c} = r.{c}" for c in ids)
    logger.info("Merge condition: %s", condition)

    # Step 7: Perform the merge operation into Delta table
    merge = (
        delta.alias("l")
        .merge(valid_records.alias("r"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )
    
    merge.execute()
    logger.info("Merge operation completed for table: %s.%s", table_schema, table_name)
