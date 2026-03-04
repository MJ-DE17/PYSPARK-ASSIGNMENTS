import pytest
from pyspark.sql import SparkSession
from src.Question4.util import *


@pytest.fixture(scope="session")
def spark():
    print("\n[INFO] Starting Spark for Question 4 Testing...")
    session = SparkSession.builder.master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_flattening_logic(spark):
    print("\n[RUNNING] Testing JSON Flattening...")
    json_path = "src/Question4/nested_json_file.json"
    raw_df = read_nested_json(spark, json_path)
    flat_df = flatten_json(raw_df)

    # Based on image_cb3ad8, there are 3 employees in the array
    assert flat_df.count() == 3
    assert "emp_name" not in flat_df.columns  # still camelCase at this point
    print(f"[SUCCESS] Flattening produced {flat_df.count()} records as expected.")


def test_snake_case_conversion(spark):
    print("\n[RUNNING] Testing CamelCase to SnakeCase...")
    df = spark.createDataFrame([(1, "Test")], ["storeName", "empId"])
    snake_df = apply_naming_convention(df)

    assert "store_name" in snake_df.columns
    assert "emp_id" in snake_df.columns
    print(f"[SUCCESS] Columns renamed: {snake_df.columns}")