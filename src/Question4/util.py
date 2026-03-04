from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, explode_outer, posexplode,
    current_date, year, month, day, regexp_replace
)

def read_nested_json(spark, path):
    """1. Read JSON file using dynamic functions."""
    return spark.read.option("multiLine", "true").json(path)

def flatten_json(df):
    """2. Flatten the data frame which is a custom schema."""
    # Exploding employees to flatten the nested array
    return df.select(
        col("id"),
        col("properties.name").alias("storeName"),
        col("properties.storeSize").alias("storeSize"),
        explode(col("employees")).alias("employee")
    ).select(
        "id", "storeName", "storeSize",
        col("employee.empId").alias("empId"),
        col("employee.empName").alias("empName")
    )

def compare_counts(raw_df, flattened_df):
    """3. Find out the record count difference."""
    raw_count = raw_df.count()
    flat_count = flattened_df.count()
    print(f"[INFO] Raw Count: {raw_count}, Flattened Count: {flat_count}")
    return raw_count, flat_count

def apply_naming_convention(df):
    """6. Convert column names from camel case to snake case."""
    for column in df.columns:
        # Simple regex to convert camelCase to snake_case
        snake_col = "".join(["_" + c.lower() if c.isupper() else c for c in column]).lstrip("_")
        df = df.withColumnRenamed(column, snake_col)
    return df

def add_date_columns(df):
    """7 & 8. Add load_date, year, month, and day columns."""
    return df.withColumn("load_date", current_date()) \
             .withColumn("year", year(col("load_date"))) \
             .withColumn("month", month(col("load_date"))) \
             .withColumn("day", day(col("load_date")))

# def write_to_employee_table(spark, df):
#     """9. Write as a partitioned JSON table in 'employee' database."""
#     spark.sql("CREATE DATABASE IF NOT EXISTS employee")
#     df.write.mode("overwrite") \
#       .format("json") \
#       .partitionBy("year", "month", "day") \
#       .option("replaceWhere", "year >= 2024") \
#       .saveAsTable("employee.employee_details")

def write_to_employee_table(spark, df):
    """9. Write as a local partitioned JSON directory (Skip Hive)."""
    # We use .save() instead of .saveAsTable() to avoid Hive permission errors
    df.write.mode("overwrite") \
      .format("json") \
      .partitionBy("year", "month", "day") \
      .save("output/employee_details_json")