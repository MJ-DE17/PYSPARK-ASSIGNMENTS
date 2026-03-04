import pytest
from pyspark.sql import SparkSession
from src.Question3.util import *


@pytest.fixture(scope="session")
def spark():
    print("\n[INFO] Initializing Spark for Question 3...")
    session = SparkSession.builder.master("local[1]").enableHiveSupport().getOrCreate()
    yield session
    session.stop()


def test_dynamic_renaming(spark):
    print("\n[RUNNING] Testing Dynamic Column Renaming...")
    df = create_activity_df(spark)
    renamed_df = rename_columns_dynamically(df)

    expected_cols = ["log_id", "user_id", "user_activity", "time_stamp"]
    assert renamed_df.columns == expected_cols
    print(f"[SUCCESS] Columns renamed correctly: {renamed_df.columns}")


def test_date_conversion(spark):
    print("\n[RUNNING] Testing Timestamp to Date Conversion...")
    df = create_activity_df(spark)
    renamed_df = rename_columns_dynamically(df)
    formatted_df = format_login_date(renamed_df)

    # Check data type of login_date
    dtype_dict = dict(formatted_df.dtypes)
    assert dtype_dict["login_date"] == "date"

    # Check sample value format
    sample = formatted_df.select("login_date").first()[0]
    assert str(sample) == "2023-09-05"
    print(f"[SUCCESS] Date conversion passed. Sample: {sample}")