import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_date, lower
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_dfs(spark):
    """1. Create all 3 dataframes with custom dynamic schemas."""
    # Employee Data
    emp_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]
    emp_schema = StructType([
        StructField("employee_id", IntegerType()),
        StructField("employee_name", StringType()),
        StructField("department", StringType()),
        StructField("State", StringType()),
        StructField("salary", IntegerType()),
        StructField("Age", IntegerType())
    ])
    employee_df = spark.createDataFrame(emp_data, emp_schema)

    # Department Data
    dept_data = [("D101", "sales"), ("D102", "finance"), ("D103", "marketing"),
                 ("D104", "hr"), ("D105", "support")]
    dept_schema = ["dept_id", "dept_name"]
    department_df = spark.createDataFrame(dept_data, dept_schema)

    # Country Data
    country_data = [("ny", "newyork"), ("ca", "California"), ("uk", "Russia")]
    country_schema = ["country_code", "country_name"]
    country_df = spark.createDataFrame(country_data, country_schema)

    return employee_df, department_df, country_df

def get_avg_salary(emp_df):
    """2. Find avg salary of each department."""
    return emp_df.groupBy("department").agg(avg("salary").alias("avg_salary"))

def filter_name_starts_with_m(emp_df, dept_df):
    """3. Find employee name and dept name starting with 'm'."""
    return emp_df.join(dept_df, emp_df.department == dept_df.dept_id) \
                 .filter(col("employee_name").startswith("m")) \
                 .select("employee_name", "dept_name")

def add_bonus_column(emp_df):
    """4. Create bonus column (salary * 2)."""
    return emp_df.withColumn("bonus", col("salary") * 2)

def reorder_columns(emp_df):
    """5. Reorder columns: (id, name, salary, State, Age, department)."""
    return emp_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")

def perform_joins(emp_df, dept_df):
    """6. Give inner, left, and right join results dynamically."""
    results = {}
    for join_type in ["inner", "left", "right"]:
        results[join_type] = emp_df.join(dept_df, emp_df.department == dept_df.dept_id, join_type)
    return results

def replace_state_with_country(emp_df, country_df):
    """7. Replace State code with country_name."""
    return emp_df.join(country_df, emp_df.State == country_df.country_code, "left") \
                 .drop("State", "country_code") \
                 .withColumnRenamed("country_name", "State")

def finalize_df(df):
    """8. Lowercase columns and add load_date."""
    for name in df.columns:
        df = df.withColumnRenamed(name, name.lower())
    return df.withColumn("load_date", current_date())

def write_external_tables(df):
    """9. Write external tables in CSV and Parquet."""
    base_path = "external_output/employee_db"
    # Note: Writing to local folders to avoid Hive/Winutils permission errors
    df.write.mode("overwrite").format("csv").option("header", "true").save(f"{base_path}/csv_table")
    df.write.mode("overwrite").format("parquet").save(f"{base_path}/parquet_table")