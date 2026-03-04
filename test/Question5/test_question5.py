import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.Question5.util import *


@pytest.fixture(scope="session")
def spark():
    """Initializes a local Spark Session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Question5_CRT_Test") \
        .getOrCreate()


def test_full_pipeline_logic(spark):
    # 1. Test Dataframe Creation & Custom Schema
    print("\n[TEST 1] Verifying Dataframe Creation...")
    emp_df, dept_df, country_df = create_dfs(spark)
    assert emp_df.count() == 7
    assert "employee_id" in emp_df.columns
    assert "dept_id" in dept_df.columns

    # 2. Test Average Salary per Department
    print("[TEST 2] Verifying Average Salary...")
    avg_df = get_avg_salary(emp_df)
    # D101 Avg: (9000+8900+7900)/3 = 8600
    d101_val = avg_df.filter(col("department") == "D101").collect()[0]["avg_salary"]
    assert d101_val == 8600.0

    # 3. Test Name starts with 'm'
    print("[TEST 3] Verifying Name Filter ('m')...")
    m_df = filter_name_starts_with_m(emp_df, dept_df)
    # Michel and Maria start with 'm'
    assert m_df.count() == 2

    # 4. Test Bonus Column (Salary * 2)
    print("[TEST 4] Verifying Bonus Calculation...")
    bonus_df = add_bonus_column(emp_df)
    sample_bonus = bonus_df.filter(col("employee_id") == 11).collect()[0]["bonus"]
    assert sample_bonus == 18000

    # 5. Test Column Reordering
    print("[TEST 5] Verifying Column Order...")
    reordered_df = reorder_columns(emp_df)
    expected_order = ["employee_id", "employee_name", "salary", "State", "Age", "department"]
    assert reordered_df.columns == expected_order

    # 6. Test Dynamic Joins (Inner/Left/Right)
    print("[TEST 6] Verifying Dynamic Joins...")
    joins = perform_joins(emp_df, dept_df)
    assert "inner" in joins and "left" in joins and "right" in joins
    # Right join should include D104 (HR) even if no employees are in it
    assert joins["right"].filter(col("dept_id") == "D104").count() > 0

    # 7 & 8. Test Country Name Swap and Lowercase/Date
    print("[TEST 7 & 8] Verifying Final Transformation...")
    country_swapped = replace_state_with_country(emp_df, country_df)
    final_df = finalize_df(country_swapped)

    # Check if 'State' now contains 'newyork' instead of 'ny'
    sample_state = final_df.filter(col("employee_id") == 11).collect()[0]["state"]
    assert sample_state == "newyork"
    # Check lowercase
    assert "employee_id" in final_df.columns
    assert "LOAD_DATE" not in final_df.columns  # Should be lowercase
    assert "load_date" in final_df.columns

    print("\n[SUCCESS] All 8 logic points verified successfully!")