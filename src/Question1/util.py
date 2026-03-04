from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_spark_session():
    return SparkSession.builder.master("local[*]").appName("PurchaseAnalysis").get_all()

def create_purchase_df(spark):
    schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])
    data = [
        (1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"),
        (2, "dell i5 core"), (3, "iphone13"), (3, "dell i5 core"),
        (1, "dell i3 core"), (1, "hp i5 core"), (1, "iphone14"),
        (3, "iphone14"), (4, "iphone13")
    ]
    return spark.createDataFrame(data, schema)

def create_product_df(spark):
    schema = StructType([StructField("product_model", StringType(), True)])
    data = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",), ("iphone14",)]
    return spark.createDataFrame(data, schema)

# Q2: Customers who bought ONLY iphone13
def get_only_iphone13_customers(df):
    return df.groupBy("customer").agg(
        F.collect_set("product_model").alias("products"),
        F.count("product_model").alias("count")
    ).filter((F.size("products") == 1) & (F.element_at("products", 1) == "iphone13")) \
     .select("customer")

# Q3: Customers who upgraded from iphone13 to iphone14
def get_iphone_upgraders(df):
    return df.filter(F.col("product_model").isin(["iphone13", "iphone14"])) \
        .groupBy("customer") \
        .agg(F.collect_set("product_model").alias("models")) \
        .filter(F.array_contains("models", "iphone13") & F.array_contains("models", "iphone14")) \
        .select("customer")

# Q4: Customers who bought all models in Product Data
def get_customers_all_products(purchase_df, product_df):
    total_products = product_df.count()
    return purchase_df.join(product_df, on="product_model", how="inner") \
        .groupBy("customer") \
        .agg(F.countDistinct("product_model").alias("distinct_count")) \
        .filter(F.col("distinct_count") == total_products) \
        .select("customer")