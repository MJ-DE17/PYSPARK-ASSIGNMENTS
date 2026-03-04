from util import *

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Question5_Final") \
        .getOrCreate()

    # 1. Create DataFrames
    emp_df, dept_df, country_df = create_dfs(spark)

    # 2. Average Salary
    print("\n[TASK 2] Avg Salary per Department:")
    get_avg_salary(emp_df).show()

    # 3. Filter Start with 'm'
    print("[TASK 3] Names starting with 'm':")
    filter_name_starts_with_m(emp_df, dept_df).show()

    # 4 & 5. Bonus and Reorder
    print("[TASK 5] Reordered Employee Data:")
    reorder_columns(emp_df).show()

    # 6. Dynamic Joins
    joins = perform_joins(emp_df, dept_df)
    print("[TASK 6] Dynamic Joins (Inner Example):")
    joins["inner"].show()

    # 7 & 8. Final Transformation
    print("[TASK 7 & 8] Final Lowercase DF with Load Date:")
    transformed_df = replace_state_with_country(emp_df, country_df)
    final_df = finalize_df(transformed_df)
    final_df.show()

    # 9. Write Tables
    write_external_tables(final_df)
    print("\n[SUCCESS] Pipeline complete. Check 'external_output/' for CSV/Parquet.")

if __name__ == "__main__":
    main()