# PySpark Assignments

This repository contains a set of PySpark exercises designed to practice core Data Engineering concepts such as DataFrame creation, transformations, partition management, schema handling, joins, flattening nested data, and writing data to storage formats and tables.

---

## Question 1 – Customer Purchase Analysis

This task focuses on creating DataFrames with custom schemas and performing analytical queries on purchase data.

Key operations:

* Create DataFrames for purchase data and product data using a custom schema.
* Identify customers who purchased only a specific product (iphone13).
* Detect customers who upgraded from iphone13 to iphone14.
* Determine customers who purchased all available product models.

Concepts used:
DataFrame creation, aggregation, filtering, set comparison.

---

## Question 2 – Partition Management and Data Masking

This task demonstrates partition manipulation and the use of User Defined Functions (UDF).

Key operations:

* Create a DataFrame containing credit card numbers using different read methods.
* Check the current number of partitions.
* Increase and decrease partitions using repartition and coalesce.
* Implement a UDF to mask credit card numbers while showing only the last four digits.

Concepts used:
Partition control, UDF creation, data masking.

---

## Question 3 – User Activity Processing

This task focuses on schema definition, column transformations, and writing data to storage.

Key operations:

* Create a DataFrame with a custom schema using StructType and StructField.
* Dynamically rename columns.
* Calculate user activity counts for the last seven days.
* Convert timestamp values to a date column.
* Write results to CSV files using different write options.
* Save the data as a managed table.

Concepts used:
Schema definition, date transformations, file writing, managed tables.

---

## Question 4 – JSON Processing and Data Flattening

This task works with nested JSON data and demonstrates flattening techniques.

Key operations:

* Read JSON data using a dynamic function.
* Flatten nested structures.
* Compare record counts before and after flattening.
* Demonstrate the use of explode, explode_outer, and posexplode functions.
* Filter specific records and convert column names from camelCase to snake_case.
* Add load date and derive year, month, and day columns.
* Write the data to a partitioned table.

Concepts used:
Nested data handling, explode functions, partitioned tables.

---

## Question 5 – Employee Data Analysis

This task demonstrates joins, aggregations, schema handling, and writing external tables.

Key operations:

* Create employee, department, and country DataFrames with dynamic schemas.
* Calculate the average salary per department.
* Filter employees based on name conditions.
* Create new derived columns such as bonus.
* Reorder columns dynamically.
* Perform inner, left, and right joins.
* Replace state codes with country names.
* Convert column names to lowercase and add load_date.
* Write external tables in CSV and Parquet formats.

Concepts used:
Joins, aggregations, column transformations, external tables.

---

## Project Structure

```
src/
 ├── Question1
 ├── Question2
 ├── Question3
 ├── Question4
 └── Question5

test/
 ├── Question1
 ├── Question2
 ├── Question3
 ├── Question4
 └── Question5
```

Each question contains implementation logic inside the `src` folder and corresponding test cases in the `test` folder.

---

## Running the Project

Run the application:

```
python src/Question1/driver.py
```

Run all tests:

```
pytest -v
```

---

