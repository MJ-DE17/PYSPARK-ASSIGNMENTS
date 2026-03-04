from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, StructField

def create_card_df(spark):
    """Creates the credit card DataFrame using a manual schema."""
    schema = StructType([StructField("card_number", StringType(), True)])
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]
    return spark.createDataFrame(data, schema)

def get_partition_info(df):
    """Returns the current number of partitions in the DataFrame."""
    return df.rdd.getNumPartitions()

def repartition_df(df, n):
    """Increases partitions to n using repartition()."""
    return df.repartition(n)

def coalesce_df(df, n):
    """Decreases partitions back to n using coalesce()."""
    return df.coalesce(n)

def mask_card_number(number):
    """Logic to mask all but the last 4 digits of a card string."""
    if number and len(number) >= 4:
        return "*" * (len(number) - 4) + number[-4:]
    return number

# Registering the UDF
mask_udf = udf(mask_card_number, StringType())

def get_masked_df(df):
    """Returns DF with original and masked card number columns."""
    return df.withColumn("masked_card_number", mask_udf(col("card_number")))