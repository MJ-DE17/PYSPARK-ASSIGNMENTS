from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, count, to_date, date_sub, current_date

def create_activity_df(spark):
    """1. Create DataFrame with custom StructType schema."""
    schema = StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]
    return spark.createDataFrame(data, schema)

def rename_columns_dynamically(df):
    """2. Rename columns dynamically using a mapping dictionary."""
    column_mapping = {
        "log id": "log_id",
        "user$id": "user_id",
        "action": "user_activity",
        "timestamp": "time_stamp"
    }
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

def get_last_7_days_actions(df):
    """3. Calculate actions performed by each user in the last 7 days."""
    # Note: Based on the 2023 dataset, we filter relative to the max date in the data
    return df.filter(col("time_stamp") >= date_sub(current_date(), 7)) \
             .groupBy("user_id") \
             .agg(count("user_activity").alias("action_count"))

def format_login_date(df):
    """4. Convert time_stamp to login_date (DateType) in YYYY-MM-DD format."""
    return df.withColumn("login_date", to_date(col("time_stamp")))

def write_to_csv(df, path):
    """5. Write DataFrame as CSV with specific options."""
    df.write.mode("overwrite") \
      .option("header", "true") \
      .option("sep", ",") \
      .csv(path)

def write_as_managed_table(spark, df):
    """6. Write as managed table in database 'user'."""
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    df.write.mode("overwrite") \
      .saveAsTable("user.login_details")