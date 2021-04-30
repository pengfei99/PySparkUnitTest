from pyspark.sql.types import StructType, StructField, StringType

from pysparkunittest.AddGreetings import add_greeting
from chispa.dataframe_comparer import assert_df_equality


def test_add_greeting(spark):
    source_data = [
        ("toto",),
        ("titi",),
        ("tata",),
    ]

    source_df = spark.createDataFrame(source_data, ["name"])
    actual_df = add_greeting(source_df)
    expected_data = [
        ("toto", "hello!"),
        ("titi", "hello!"),
        ("tata", "hello!"),

    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "greeting"])
    source_df.show()
    actual_df.show()
    expected_df.show()
    assert_df_equality(actual_df, expected_df,ignore_nullable=True)
