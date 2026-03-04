import pytest
from pyspark.sql import SparkSession
from src.Question1.util import *


@pytest.fixture(scope="session")
def spark():
    print("\n[INFO] Starting Spark Session for Testing...")
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Test Question1") \
        .getOrCreate()
    yield spark
    print("\n[INFO] Cleaning up Spark Session...")
    spark.stop()


def test_only_iphone13(spark):
    print("\n[RUNNING] Test: Only iPhone 13 Customers...")
    purchase_df = create_purchase_df(spark)
    result = get_only_iphone13_customers(purchase_df).collect()
    customers = [row.customer for row in result]

    # Custom assertion message
    assert 4 in customers, f"FAILED: Expected Customer 4, but got {customers}"
    print(f"[SUCCESS] Only iPhone 13 Test Passed. Found Customers: {customers}")


def test_upgraded_customers(spark):
    print("\n[RUNNING] Test: Upgraded Customers (13 to 14)...")
    purchase_df = create_purchase_df(spark)
    result = get_iphone_upgraders(purchase_df).collect()
    customers = [row.customer for row in result]

    assert 1 in customers and 3 in customers, f"FAILED: Expected [1, 3], but got {customers}"
    print(f"[SUCCESS] Upgrade Test Passed. Found Customers: {customers}")


def test_all_products(spark):
    print("\n[RUNNING] Test: Customers who bought ALL products...")
    purchase_df = create_purchase_df(spark)
    product_df = create_product_df(spark)
    result = get_customers_all_products(purchase_df, product_df).collect()
    customers = [row.customer for row in result]

    assert 1 in customers, f"FAILED: Expected Customer 1 to have bought everything, but got {customers}"
    print(f"[SUCCESS] All Products Test Passed. Found Customers: {customers}")