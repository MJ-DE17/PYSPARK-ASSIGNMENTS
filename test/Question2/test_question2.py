import pytest
from pyspark.sql import SparkSession
from src.Question2.util import *


@pytest.fixture(scope="session")
def spark():
    print("\n[INFO] Starting Spark for Question 2...")
    session = SparkSession.builder.master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_partition_logic(spark):
    print("\n[RUNNING] Testing Partition Management...")
    df = create_card_df(spark)
    initial = get_partition_info(df)

    # Test Increase
    df_5 = repartition_df(df, 5)
    assert get_partition_info(df_5) == 5, "Failed to increase partitions to 5"

    # Test Decrease
    df_back = coalesce_df(df_5, initial)
    assert get_partition_info(df_back) == initial, f"Failed to return to {initial} partitions"
    print(f"[SUCCESS] Partition transitions (Initial -> 5 -> {initial}) passed.")


def test_masking_udf(spark):
    print("\n[RUNNING] Testing Credit Card Masking UDF...")
    df = create_card_df(spark)
    masked_df = get_masked_df(df)

    # Take first row to verify
    sample = masked_df.filter(col("card_number") == "1234567891234567").collect()[0]

    expected = "************4567"
    assert sample.masked_card_number == expected, f"Expected {expected}, but got {sample.masked_card_number}"
    print(f"[SUCCESS] Masking Test Passed. Sample: {sample.card_number} -> {sample.masked_card_number}")