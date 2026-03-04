from util import *


def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    purchase_df = create_purchase_df(spark)
    product_df = create_product_df(spark)

    print("Customers who bought only iphone13:")
    get_only_iphone13_customers(purchase_df).show()

    print("Customers who upgraded from iphone13 to iphone14:")
    get_iphone_upgraders(purchase_df).show()

    print("Customers who bought all products:")
    get_customers_all_products(purchase_df, product_df).show()


if __name__ == "__main__":
    main()