from datetime import datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from process import (
    create_df1_filtered,
    create_joined_df,
    create_multi_order_customers,
    create_total_amount_by_country,
)


# Define a fixture named "spark"
@pytest.fixture(scope="session")
def spark():
    """create spark session for test file"""
    spark = SparkSession.builder.master("local[*]") \
        .config("spark.driver.memory", "3G") \
        .config("spark.sql.shuffle.partitions", "8") \
        .appName("Spark session for local tests") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.mark.usefixtures("spark")
class TestProcessing:
    def test_create_df1_filtered(self, spark: SparkSession) -> None:
        # Create test dataframe df1
        test_data = [
            Row(id=5, date="2021-05-20", amount=75, city="London"),
            Row(id=6, date="2021-06-10", amount=120, city="New York"),
            Row(id=7, date="2021-07-25", amount=40, city="Paris"),
            Row(id=8, date="2021-08-15", amount=90, city="Tokyo"),
            Row(id=11, date="2021-08-11", amount=100, city="Tokyo")
        ]
        test_columns = ["id", "date", "amount", "city"]
        test_df = spark.createDataFrame(test_data, test_columns)

        # Create expected_df
        expected_data = [
            Row(id=5, date=datetime.strptime("2021-05-20", "%Y-%m-%d").date(), total_amount=75, city="LONDON", year=2021),
            Row(id=6, date=datetime.strptime("2021-06-10", "%Y-%m-%d").date(), total_amount=120, city="NEW YORK", year=2021),
            Row(id=8, date=datetime.strptime("2021-08-15", "%Y-%m-%d").date(), total_amount=90, city="TOKYO", year=2021),
            Row(id=11, date=datetime.strptime("2021-08-11", "%Y-%m-%d").date(), total_amount=100, city="TOKYO", year=2021)
        ]
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("date", DateType(), True),
            StructField("total_amount", LongType(), True),
            StructField("city", StringType(), True),
            StructField("year", IntegerType(), True)
        ])
        expected_df = spark.createDataFrame(expected_data, schema)
        assert_df_equality(create_df1_filtered(test_df),expected_df, ignore_row_order=True)

    def test_create_joined_df(self, spark: SparkSession) -> None:
        # Create test dataframe df_orders
        test_data_orders = [
            Row(order_id=201, customer_id=2001, order_date="2022-05-20"),
            Row(order_id=202, customer_id=2002, order_date="2022-06-10"),
            Row(order_id=203, customer_id=2001, order_date="2022-07-25"),
            Row(order_id=204, customer_id=2003, order_date="2022-08-15")
        ]
        test_orders_columns = ["order_id", "customer_id", "order_date"]
        test_df_orders = spark.createDataFrame(test_data_orders, test_orders_columns)

        # Create test dataframe df_orders
        test_data_customers = [
            Row(customer_id=2001, customer_name="Eve", customer_country="Canada"),
            Row(customer_id=2002, customer_name="David", customer_country="Australia"),
            Row(customer_id=2003, customer_name="Grace", customer_country="Germany")
        ]
        test_customers_columns = ["customer_id", "customer_name", "customer_country"]
        test_df_customers = spark.createDataFrame(test_data_customers, test_customers_columns)
        
        # Create expected_df
        expected_data = [
            Row(customer_id=2001, order_id=201, order_date="2022-05-20", customer_name="Eve", customer_country="Canada"),
            Row(customer_id=2002, order_id=202, order_date="2022-06-10", customer_name="David", customer_country="Australia"),
            Row(customer_id=2001, order_id=203, order_date="2022-07-25", customer_name="Eve", customer_country="Canada"),
            Row(customer_id=2003, order_id=204, order_date="2022-08-15", customer_name="Grace", customer_country="Germany")
        ]

        # Define the schema using StructType
        schema = StructType([
            StructField("customer_id", LongType(), True),
            StructField("order_id", LongType(), True),
            StructField("order_date", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_country", StringType(), True)
        ])
        expected_df = spark.createDataFrame(expected_data, schema)
        assert_df_equality(create_joined_df(test_df_orders, test_df_customers), expected_df, ignore_row_order=True)

    def test_create_total_amount_by_country(self, spark: SparkSession) -> None:
        # Create test dataframe df1
        test_data = [
            Row(id=5, date="2021-05-20", amount=75, city="London"),
            Row(id=6, date="2021-06-10", amount=120, city="New York"),
            Row(id=7, date="2021-07-25", amount=40, city="Paris"),
            Row(id=8, date="2021-08-15", amount=90, city="Tokyo"),
            Row(id=11, date="2021-08-11", amount=100, city="Tokyo")
        ]
        test_columns = ["id", "date", "amount", "city"]
        test_df = spark.createDataFrame(test_data, test_columns)

        # Create expected_df
        expected_data = [
            Row(city="London", total_order_amount=75),
            Row(city="New York", total_order_amount=120),
            Row(city="Paris", total_order_amount=40),
            Row(city="Tokyo", total_order_amount=190)
        ]

        schema = StructType([
            StructField("city", StringType(), True),
            StructField("total_order_amount", LongType(), True)
        ])

        expected_df = spark.createDataFrame(expected_data, schema)
        assert_df_equality(create_total_amount_by_country(test_df), expected_df, ignore_row_order=True)

    def test_create_multi_order_customers(self, spark: SparkSession):
        # Create test_df
        test_data = [
            Row(customer_id=2001, order_id=201, order_date="2022-05-20", customer_name="Eve", customer_country="Canada"),
            Row(customer_id=2002, order_id=202, order_date="2022-06-10", customer_name="David", customer_country="Australia"),
            Row(customer_id=2001, order_id=203, order_date="2022-07-25", customer_name="Eve", customer_country="Canada"),
            Row(customer_id=2003, order_id=204, order_date="2022-08-15", customer_name="Grace", customer_country="Germany")
        ]

        # Define the schema using StructType
        test_columns = ["customer_id", "order_id", "order_date", "customer_name", "customer_country"]

        test_df = spark.createDataFrame(test_data, test_columns)

        # Create test dataframe df_orders
        expected_data = [
            Row(customer_name="Eve", customer_id=2001, order_count=2),
        ]

        expected_schema = StructType([
            StructField("customer_name", StringType(), True),
            StructField("customer_id", LongType(), True),
            StructField("order_count", LongType(), False)
        ])
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        assert_df_equality(create_multi_order_customers(test_df), expected_df, ignore_row_order=True)
