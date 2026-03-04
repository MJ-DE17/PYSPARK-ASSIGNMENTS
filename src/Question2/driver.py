from util import *


def main():
    spark = SparkSession.builder.master("local[*]").appName("CardAnalysis").getOrCreate()

    # 1. Create DataFrame
    df = create_card_df(spark)
    original_partitions = get_partition_info(df)

    # 2. Print initial partitions
    print(f"\n[STEP 2] Initial Partitions: {original_partitions}")

    # 3. Increase to 5
    df_increased = repartition_df(df, 5)
    print(f"[STEP 3] Increased Partitions: {get_partition_info(df_increased)}")

    # 4. Decrease back
    df_decreased = coalesce_df(df_increased, original_partitions)
    print(f"[STEP 4] Decreased back to: {get_partition_info(df_decreased)}")

    # 5 & 6. Apply UDF and show Output
    final_df = get_masked_df(df)
    print("\n[STEP 6] Masked Card Numbers:")
    final_df.show()


if __name__ == "__main__":
    main()