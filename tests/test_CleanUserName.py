import pytest
from chispa.column_comparer import assert_column_equality
from chispa.dataframe_comparer import assert_df_equality

from pysparkunittest.CleanUserName import remove_special_letter_quinn, remove_special_letter_native, remove_space


# Illustrate how to test equality of two dataframe
def test_remove_special_letter_quinn(spark):
    source_data = [
        ("toto@123", "toto123@gmail.com"),
        ("titi$___", "titi@gmail.com"),
        ("!!##tata", "tata@gmail.com"),
    ]

    source_df = spark.createDataFrame(source_data, ["name", "mail"])
    actual_df = remove_special_letter_quinn(source_df, "name")
    expected_data = [
        ("toto@123", "toto123@gmail.com", "toto123"),
        ("titi$___", "titi@gmail.com", "titi___"),
        ("!!##tata", "tata@gmail.com", "tata"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "mail", "cleaned_name"])
    source_df.show()
    actual_df.show()
    expected_df.show()
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


# Illustrate how to test equality of two columns
# note that the assert_column_equality does not require ignore_nullable to avoid schema mismatch issue.
# Because when comparing two columns, it does not check schema
def test_remove_special_letter_native(spark):
    source_data = [
        ("toto@123", "toto123@gmail.com", "toto123"),
        ("titi$___", "titi@gmail.com", "titi___"),
        ("!!##tata", "tata@gmail.com", "tata"),
    ]

    source_df = spark.createDataFrame(source_data, ["name", "mail", "expected_name"])
    actual_df = remove_special_letter_native(source_df, "name")
    actual_df.show()

    assert_column_equality(actual_df, "cleaned_name", "expected_name")


# In a real world scenario you need to test all the remove on every position("l","r","a") here I just illustrate the "a"
def test_remove_space(spark):
    source_data = [
        ("  toto  ", "toto123@gmail.com"),
        ("titi  ", "titi@gmail.com"),
        ("  tata", "tata@gmail.com"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "mail"])
    actual_df = remove_space(source_df, "name", "a")
    expected_data = [
        ("toto", "toto123@gmail.com"),
        ("titi", "titi@gmail.com"),
        ("tata", "tata@gmail.com"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "mail"])
    source_df.show()
    actual_df.show()
    expected_df.show()
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


# As the remove space function takes argument position. If the user gives a wrong argument value. The function
# must raise an exception. We also need to test if the function raise the exception correctly
def test_remove_space_bad_argument(spark):
    source_data = [
        ("  toto  ", "toto123@gmail.com"),
        ("titi  ", "titi@gmail.com"),
        ("  tata", "tata@gmail.com"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "mail"])
    with pytest.raises(ValueError):
        remove_space(source_df, "name", "x")
