import pyspark.sql.functions as sql_fun
import quinn
from pyspark.sql.functions import ltrim, rtrim, trim


# This function removes special character such as !@#$%^&*() from a string column by using quinn
def remove_special_letter_quinn(dataframe, column_name: str):
    return dataframe.withColumn("cleaned_{}".format(column_name),
                                quinn.remove_non_word_characters(sql_fun.col(column_name)))


def remove_special_letter_native(dataframe, column_name: str):
    return dataframe.withColumn("cleaned_{}".format(column_name),
                                sql_fun.regexp_replace(sql_fun.col(column_name), "[^\\w\\s]+", ""))


def remove_space(df, col_name, position):
    if position not in ["l", "r", "a"]:
        raise ValueError("The position value must be l, r or a")
    # get origin column orders
    columns = df.columns
    # remove left side space
    if position == "l":
        return df.withColumn("tmp", ltrim(sql_fun.col(col_name))).drop(col_name).withColumnRenamed("tmp",
                                                                                                   col_name).select(
            *columns)
    # remove right side space
    elif position == "r":
        return df.withColumn("tmp", rtrim(sql_fun.col(col_name))).drop(col_name).withColumnRenamed("tmp",
                                                                                                   col_name).select(
            *columns)
    # remove all side space
    elif position == "a":
        return df.withColumn("tmp", trim(sql_fun.col(col_name))).drop(col_name).withColumnRenamed("tmp",
                                                                                                  col_name).select(
            *columns)
