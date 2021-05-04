# PySparkUnitTest

This project shows how to do unit testing on pyspark project. For testing spark(scala), please visit this project
https://github.com/pengfei99/SparkScalaUnitTest. The unit testing can help you to avoid run sample data sets to 
develop code on a cluster. Because clusters are slow to start and may cost you money. 


## 0. Background
We found only one framework which can help us to do unit testing on pyspark project.
1. chispa: https://github.com/MrPowers/chispa


## 1. Installation
chispa is developed by using pytest, so it can't work alone. It heavily relies on **pytest** 
(https://docs.pytest.org/) framework. 

And setting pytest up manually is very time-consuming. So the easiest way to integrate it in your project is to 
use a framework to generate a python project with the pytest support.

Here, we use **poetry** to generate a python project.
We suppose you already have poetry on your pc. To generate a new python project, just type the following command. 

```shell script
# general form
poetry new <project-name>

# example
poetry new PythonTest

# add pyspark support
poetry add pyspark

# add chispa as dev dependencies, which will not be added to the production build
poetry add chispa --dev
```
For more information on poetry cli, you can visit this page: https://python-poetry.org/docs/cli/

This will create a python project with the following contents:
```shell
PythonTest
  pythontest
    __init__.py
  pyproject.toml
  README.rst
  tests
    __init__.py
    test_pythontest.py
```

And all the project meta data(e.g. dependencies, build instructions, project info, etc.) are located at pyproject.toml

## 2. Test your functions
In spark, we normally has two type of functions:
1. transform a data frame by adding some new columns.
2. create a new data frame based on one or more source data frames.

So basically, we have two types of test:
1. Column equality test: Check if the generated column value equals the expected column value
2. Data frame equality test: Check if the generated data frame equal the expected data frame.

### 2.1 Column equality test

We mainly used two methods from chispa.column_comparer:
1. assert_column_equality : general equality test on any column type
2. assert_approx_column_equality : if column type is float or double, we may have precision issues. With this method, 
         we can set a precision for the values which we want to compare. 

You can find the test function **test_remove_special_letter_native** in class **tests/test_CleanUserName** which 
illustrates the assert_column_equality.

You can find the test function **test_create_column_with_approximate_column_equality** in class **tests/test_PowerTwo** 
which illustrates the assert_approx_column_equality.

### 2.2 Data frame equality test
To test equality of two data frames, we mainly use two method from chispa.dataframe_comparer import ,:
1. assert_df_equality : It is faster for test DataFrames that locates on your local machine. 
2. assert_approx_df_equality : It is more optimal for DataFrames that are split across nodes in a cluster.

#### 2.2.1 Data frame schema mismatch
These two method check first the equality of the schema. As the schema of spark has three properties:
1. Column Name
2. Column type
3. Nullable
If one of the properties mismatch, you will receive a data frame mismatch error. And based on how your dataframes
are created, **the Nullable properties can be true or false**. In most of time, it does not impact the equality
of data frames. 

To ignore the nullable flag 
```python
assert_df_equality(actual_df, expected_df, ignore_nullable=True)
```

#### 2.2.2 Unordered DataFrame equality comparisons

For most of the time, the row order does not affect the equality of two data frames. For example, DF1 should equal DF2.
But if we just use **assert_df_equality**, it will return a mismatch error
```shell script
# DF1:
+------+
|number|
+------+
|     1|
|     5|
+------+
# DF2:
+------+
|number|
+------+
|     5|
|     1|
+------+
``` 

Same for the column order, for example if we have two dataframe, one is ["name","age"], the other one is ["age","name"].
The **assert_df_equality** will return a mismatch error.

So we want to ignore the row order and/or column order, we can set the **ignore_row_order** and/or **ignore_row_order**
boolean flag to True and assert_df_equality will sort the DataFrames before performing the comparison.

```python
assert_df_equality(actual_df, expected_df, ignore_row_order= True,ignore_column_order= True)

```     
For complete code example, please check **tests/test_PowerTwo.test_df_ignore_row_order(spark)**

#### 2.2.3 Approximate DataFrame Equality
As we mentioned before, if the data frame has float or double column, when we compare them, we need to specify a
precision. For example, if we compare the two below data frame.  

```shell script
# DF1
+------+-------+
|   1.0|    1.0|
|   2.0|    4.0|
|   3.0|    9.0|
|   4.0|   16.0|
+------+-------+
# DF2
|source|power_2|
+------+-------+
|   1.0|   1.01|
|   2.0|   4.06|
|   3.0|  9.001|
|   4.0| 16.001|
+------+-------+

```


```scala
// If set precision to 0.1, this will return true
assert_approx_df_equality(actual_df, expected_df, 0.1)

//If set precision to 0.1, this will return false
assert_approx_df_equality(actual_df, expected_df, 0.o1)
```

For complete code example, please check **tests/TestPowerTwo.test_create_column_with_approximate_df_equality** 

## 3. Creat SparkSession for your test environment

The chispa framework doesn't provide a SparkSession for your test suite, so you'll need to make one yourself. For now,
we use a pytest fixture

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder
        .master("local")
        .appName("PySparkUnitTest")
        .getOrCreate()
```

Note in your local test environment, it's better to set the number of shuffle partitions to a small number like one 
or four. This configuration can make your tests run up to 70% faster. 

**Don't use this SparkSession configuration when you're working with big DataFrames in your test suite or running 
production code.**

