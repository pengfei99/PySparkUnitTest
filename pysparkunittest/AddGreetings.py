import pyspark.sql.functions as sql_fun
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def add_greeting(df):
    return df.withColumn("greeting", sql_fun.lit("hello!"))



