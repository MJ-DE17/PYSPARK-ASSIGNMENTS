import os
import sys

# 1. Set environment FIRST
os.environ['HADOOP_HOME'] = r"C:\spark\spark-3.5.8-bin-hadoop3"
os.environ['PATH'] += os.pathsep + r"C:\spark\spark-3.5.8-bin-hadoop3\bin"

# 2. Then import your logic
from util import *


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("UserActivityAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()

    # Process Data
    raw_df = create_activity_df(spark)
    renamed_df = rename_columns_dynamically(raw_df)
    final_df = format_login_date(renamed_df)

    # Show Results
    print("\n[STEP 4] Final DataFrame with login_date:")
    final_df.show()

    # Writing Operations
    write_to_csv(final_df, "output/user_activity_csv")
    write_as_managed_table(spark, final_df)
    print("\n[SUCCESS] Data written to CSV and Managed Table user.login_details")


if __name__ == "__main__":
    main()