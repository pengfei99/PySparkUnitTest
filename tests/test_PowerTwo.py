from chispa.column_comparer import assert_approx_column_equality
from chispa.dataframe_comparer import assert_approx_df_equality, assert_df_equality

from pysparkunittest.PowerTwo import power_two, create_column_with_power


def test_power_two():
    assert 4.0 == power_two(2.0)


def test_create_column_with_approximate_column_equality(spark):
    source_data = [
        (1.0, 1.01),
        (2.0, 4.06),
        (3.0, 9.001),
        (4.0, 16.001)
    ]
    source_df = spark.createDataFrame(source_data, ["base", "expected_result"])
    df = create_column_with_power(source_df, "base")
    df.show()
    assert_approx_column_equality(df, "expected_result", "power_2", 0.1)


def test_create_column_with_approximate_df_equality(spark):
    source_data = [
        (1.0,),
        (2.0,),
        (3.0,),
        (4.0,)
    ]
    source_df = spark.createDataFrame(source_data, ["base"])
    actual_df = create_column_with_power(source_df, "base")

    expected_data = [
        (1.0, 1.01),
        (2.0, 4.06),
        (3.0, 9.001),
        (4.0, 16.001)
    ]
    expected_df = spark.createDataFrame(expected_data, ["base", "power_2"])
    actual_df.show()
    assert_approx_df_equality(actual_df, expected_df, 0.1)


def test_column_allow_nan_equality(spark):
    source_data = [
        (1.0, 1.01),
        (4.0, 4.06),
        (9.0, 9.001),
        (16.0, 16.001),
        (None, None)
    ]
    df = spark.createDataFrame(source_data, ["col1", "col2"])
    df.show()
    assert_approx_column_equality(df, "col1", "col2", 0.1)


# Python has NaN (not a number) values and two NaN values are not considered equal by default.
# But in a data frame, we would like to have nan equality. To enable nan equality when comparing two data frame
# set the option allow_nan_equality=True. For example,assert_df_equality(df1, df2, allow_nan_equality=True)
def test_df_allow_nan_equality(spark):
    actual_data = [
        (1.0, 1.01),
        (2.0, 4.06),
        (3.0, 9.001),
        (4.0, 16.001),
        (None, None)
    ]
    actual_df = spark.createDataFrame(actual_data, ["col1", "col2"])

    expected_data = [
        (1.0, 1.01),
        (2.0, 4.06),
        (3.0, 9.001),
        (4.0, 16.001),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2"])
    actual_df.show()
    expected_df.show()
    assert_df_equality(actual_df, expected_df, allow_nan_equality=False)


# We can compare two data frame by ignoring row order and/or column order, by setting options ignore_row_order=True
# and/or ignore_column_order=True. Try to remove the option in below example and see if test passes or not.
def test_df_ignore_row_order(spark):
    actual_data = [
        (1.0, 1.01),
        (4.0, 16.001),
        (2.0, 4.06),
        (3.0, 9.001),
        (None, None)
    ]
    actual_df = spark.createDataFrame(actual_data, ["col1", "col2"])
    expected_data = [
        (1.0, 1.01),
        (3.0, 9.001),
        (2.0, 4.06),
        (4.0, 16.001),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2"])
    actual_df.show()
    expected_df.show()
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
