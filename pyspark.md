# PySpark

Much of this guide is built from the following sources.

- [sparkbyexamples](https://sparkbyexamples.com/spark/spark-performance-tuning/)
- [databricks Glossary](https://databricks.com/glossary)
- [Improve Spark performance](https://medium.com/swlh/10-ways-to-improve-spark-performance-b54e89b8d83a)
## Starting a pyspark session

### Import the correct methods

There are older spark instantiation methods under the `Context` names - `SparkContext`, `SQLContext`, and `HiveContext`. We don't need those with the `SparkSession` method that was introduced in Spark 2. We will use the `SparkConf` method to configure a few settings of our spark environment.  Finally, the spark SQL functions are a must to run optimized spark code.  I have elected to import them with the abbreviation `F`.

```python
# Databricks handles the first two imports.
from pyspark.sql import SparkSession 
from pyspark import SparkConf 
# Will need to execute this on Databricks
from pyspark.sql import functions as F # access to the sql functions https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
```

You can read [configuration_docker.md](configuration.md) to see additional settings that can help during a spark session.

## Methods

### `.pivot()`

The shape of our data is important for machine learning and visualization.  In many situations you will move back and forth between 'wide formats' and 'long formats' as you build data for your project. Pyspark has the `.pivot()` and 

- https://databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html
- https://sparkbyexamples.com/pyspark/pyspark-pivot-and-unpivot-dataframe/

## Collecting a list of values using `ArrayType`

Spark DataFrames allow us to store arrays in a cell. These variables are called `ArrayType` columns.

- https://mungingdata.com/apache-spark/arraytype-columns/
- https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.slice.html
- https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.aggregate.html
- https://spark.apache.org/docs/latest/api/python//reference/api/pyspark.sql.functions.sort_array.html
- https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/
- https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.arrays_zip.html
- https://stackoverflow.com/questions/51082758/how-to-explode-multiple-columns-of-a-dataframe-in-pyspark/51085148
- https://stackoverflow.com/questions/43349932/how-to-flatten-long-dataset-to-wide-format-pivot-with-no-join

## Aggregated Calculations

See [aggregate_calculations.md]
