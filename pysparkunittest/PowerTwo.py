from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType


def power_two(n: float):
    return pow(n, 2)


power_two_UDF = udf(lambda arg: power_two(arg), DoubleType())


def create_column_with_power(df, column_name: str):
    return df.withColumn("power_2", power_two_UDF(col(column_name)))
