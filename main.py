from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("ProductCategoryExample").getOrCreate()

# test data
products = [
    ("p1", "Product1"),
    ("p2", "Product2"),
    ("p3", "Product3"),
    ("p4", "Product4")
]

categories = [
    ("c1", "Category1"),
    ("c2", "Category2"),
    ("c3", "Category3")
]

product_categories = [
    ("p1", "c1"),
    ("p1", "c2"),
    ("p2", "c1"),
    ("p3", "c3")
]

# Creating DataFrame
products_df = spark.createDataFrame(products, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories, ["category_id", "category_name"])
product_categories_df = spark.createDataFrame(product_categories, ["product_id", "category_id"])

# union products and cats
product_category_pairs_df = product_categories_df.join(
    products_df, on="product_id", how="inner"
).join(
    categories_df, on="category_id", how="inner"
).select(
    col("product_name"), col("category_name")
)

# print pairs "Name-Category"
product_category_pairs_df.show()

# Find products without categories, using left_anti join
products_with_no_categories_df = products_df.join(
    product_categories_df, on="product_id", how="left_anti"
).select(col("product_name"))

# Print products without categories
products_with_no_categories_df.show()

# Stop SparkSession
spark.stop()
