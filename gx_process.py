from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import year

from process import (
    create_df1_filtered,
    create_joined_df,
    create_multi_order_customers,
    create_total_amount_by_country,
)

spark = SparkSession.builder.master("local[*]") \
        .appName("spark_processing") \
        .config("spark.driver.memory", "3G") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

df1 = spark.read.csv("C:/exercice2/gx/data/tables/df1.csv", sep=',', header=True, inferSchema=True)
df_orders = spark.read.csv("C:/exercice2/gx/data/tables/df_orders.csv", sep=',', header=True, inferSchema=True)
df_customers = spark.read.csv("C:/exercice2/gx/data/tables/df_customers.csv", sep=',', header=True, inferSchema=True)

df1 = spark.read.csv("C:/exercice2/gx/data/tables/df1.csv", sep=',', header=True, inferSchema=True)
gdf1 = SparkDFDataset(df1)

# Total_amount column verification
df1_filtered = create_df1_filtered(df1)
df1_filtered_gx = SparkDFDataset(df1_filtered)
df1_filtered_gx.expect_column_values_to_not_be_null(column='total_amount')

# Dates only in 2021
df_orders_gx = SparkDFDataset(df_orders)
df_orders_gx.expect_column_values_to_match_regex(column="order_date", regex=r"^2021-\d{2}-\d{2}$")

validation_result1 = df1_filtered_gx.validate()
validation_result2 = df_orders_gx.validate()
failures = [x for x in validation_result1.results if not x.success]
if len(failures) > 0:
    print("There are failures detected")
    print(failures)
else:
    print("No failures")
failures = [x for x in validation_result2.results if not x.success]
if len(failures) > 0:
    print("There are failures detected")
    print(failures)
else:
    print("No failures")