import pyspark.sql.functions as sql_fun


def add_greeting(df):
    return df.withColumn("greeting", sql_fun.lit("hello!"))
