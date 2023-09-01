from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, year
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.master("local[*]") \
        .appName("spark_processing") \
        .config("spark.driver.memory", "3G") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def create_df1_filtered(df1: DataFrame) -> DataFrame:
        # Effectuer des opérations de nettoyage et de transformation des données sur df1
        df1_filtered = df1.withColumn("date", col("date").cast(DateType())) \
                .withColumnRenamed("amount", "total_amount") \
                .filter(col("total_amount") > 50) \
                .withColumn("year", year("date")) \
                .withColumn("city", F.upper(F.col("city")))
        return df1_filtered

def create_joined_df(df_orders: DataFrame, df_customers: DataFrame) -> DataFrame:
        # Effectuer une jointure entre df_orders et df_customers
        joined_df = df_orders.join(df_customers, on="customer_id", how='left')
        return joined_df

def create_total_amount_by_country(df1: DataFrame) -> DataFrame:
        # Effectuer une agrégation pour obtenir le montant total des commandes par pays
        total_amount_by_country = df1.groupBy("city").agg(sum("amount").alias("total_order_amount"))
        return total_amount_by_country

def create_multi_order_customers(joined_df: DataFrame) -> DataFrame:
        # Utiliser la clause HAVING après une agrégation
        orders_per_customer = joined_df.groupBy("customer_name", "customer_id").agg(count("order_id").alias("order_count"))
        multi_order_customers = orders_per_customer.filter(col("order_count") > 1)
        return multi_order_customers

# # Sauvegarder le résultat final dans un fichier CSV
# total_amount_by_country.write.csv("path/to/save/total_amount_by_country.csv", header=True)
# multi_order_customers.write.csv("path/to/save/multi_order_customers.csv", header=True)

#
print("process Run successfully")
# Stop spark session
spark.stop()
