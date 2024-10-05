from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
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

def create_constraints_table(spark:SparkSession, schema: str, table_name: str):
    clientConstraints = f"""{{
        "tableName": "silver_clients",
        "constraints": [
            "ALTER TABLE {schema}.silver_clients ALTER COLUMN client_id SET NOT NULL;",
            "ALTER TABLE {schema}.silver_clients ADD CONSTRAINT validAddress  CHECK (address in ('New York', 'California', 'Texas', 'Florida', 'Illinois'));"
        ]
    }}"""


    productConstraints = f"""{{
        "tableName": "silver_products",
        "constraints": [
            "ALTER TABLE {schema}.silver_products ALTER COLUMN product_id SET NOT NULL;"
        ]
    }}"""

    salesConstraints = f"""{{
        "tableName": "silver_sales",
        "constraints": [
            "ALTER TABLE {schema}.silver_sales ALTER COLUMN sales_id SET NOT NULL;",
            "ALTER TABLE {schema}.silver_sales ALTER COLUMN client_id SET NOT NULL;",
            "ALTER TABLE {schema}.silver_sales ALTER COLUMN product_id SET NOT NULL;",
            "ALTER TABLE {schema}.silver_sales ADD CONSTRAINT dateWithinRange  CHECK (date between '2020-01-01' and '2025-01-01');",
            "ALTER TABLE {schema}.silver_sales ADD CONSTRAINT validQuantity  CHECK (quantity > 0);"
        ]
    }}"""

    constraints = [clientConstraints, productConstraints, salesConstraints]

    df = spark.read.json(spark.sparkContext.parallelize(constraints)).select("tableName", explode("constraints"))
    df.write.mode("overwrite").saveAsTable(table_name)

    return constraints 


def apply_table_constaints(spark: SparkSession, source_constraints_tablename: str, table_name: str) -> None:
    """
    Apply table-specific SQL constraints to the given Delta table.

    Args:
        spark (SparkSession): The Spark session.
        source_constraints_tablename (str): The table containing the constraints definitions.
        table_name (str): The table to which constraints are being applied.

    Returns:
        None: This function applies constraints directly to the table using SQL statements.
    """
    logger.info("Applying table constraints to table: %s", table_name)

    # Read the constraints from the source constraints table
    constraints_df = spark.read.table(source_constraints_tablename)
    
    # Filter constraints relevant to the target table
    table_constraints = constraints_df.filter(f"tableName == '{table_name}'")

    # Apply each constraint via SQL execution
    for row in table_constraints.collect():
        try:
            logger.info("Applying constraint: %s", row[1])
            spark.sql(row[1])
        except Exception as e:
            logger.error("Error applying constraint %s to table %s: %s", row[1], table_name, str(e))
            raise

def get_table_constraints_conditions(spark: SparkSession, schema: str, table_name: str) -> Column:
    """
    Generate a composite filter condition for validating table constraints, including non-nullable columns and custom constraints.

    Args:
        spark (SparkSession): The Spark session.
        schema (str): The schema of the table.
        table_name (str): The name of the table for which constraints are generated.

    Returns:
        Column: A PySpark Column object representing the composite filter condition for the constraints.
    """
    logger.info("Generating table constraint conditions for table: %s.%s", schema, table_name)

    # Identify non-nullable fields and generate a filter for them
    nullable_filters = [
        f"{x.name} IS NOT NULL" 
        for x in spark.table(f"{schema}.{table_name}").schema.fields if not x.nullable
    ]
    logger.info("Non-nullable field filters: %s", nullable_filters)

    # Retrieve custom table constraints (from Delta properties)
    constraints_df = (
        spark.sql(f"SHOW TBLPROPERTIES {schema}.{table_name}")
        .filter(F.col("key").startswith("delta.cons"))
        .select("value")
    )

    # Collect the constraints from the table properties
    constraints_filter = [c[0] for c in constraints_df.collect()]
    logger.info("Custom constraints from TBLPROPERTIES: %s", constraints_filter)

    # Combine nullable filters and custom constraints into a single condition
    constraints = nullable_filters + constraints_filter

    # Reduce the list of constraints to a composite filter condition using logical OR
    # If any constraint fails (is false), we want to quarantine the record
    combined_condition = reduce(lambda x, y: x | ~F.expr(y), constraints, F.lit(False))
    logger.info("Combined constraint condition generated.")

    return combined_condition