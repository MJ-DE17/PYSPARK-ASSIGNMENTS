import os
from util import *


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("NestedJsonAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()

    # This finds the directory where driver.py lives
    current_dir = os.path.dirname(__file__)

    # This joins that directory with the filename
    json_path = os.path.join(current_dir, "nested_json_file.json")

    # Now read using the absolute path
    # 1. Read & 3. Compare Counts
    raw_df = read_nested_json(spark, json_path)

    flat_df = flatten_json(raw_df)
    compare_counts(raw_df, flat_df)

    # 4. Explosion variations (Demonstration)
    print("\n[INFO] Demonstration of PoseExplode:")
    raw_df.select("id", posexplode(col("employees"))).show()

    # 5. Filter & 6. Rename
    processed_df = flat_df.filter(col("id") == 1001)
    snake_df = apply_naming_convention(processed_df)

    # 7 & 8. Dates
    final_df = add_date_columns(snake_df)
    final_df.show()

    # 9. Write Table
    write_to_employee_table(spark, final_df)
    print("\n[SUCCESS] Question 4 Processing Complete.")


if __name__ == "__main__":
    main()