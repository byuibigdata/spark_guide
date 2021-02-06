# Using Spark for exploratory data analysis

## What is Spark?

> From its humble beginnings in the AMPLab at U.C. Berkeley in 2009, Apache Spark has become one of the key big data distributed processing frameworks in the world. Spark can be deployed in a variety of ways, provides native bindings for the Java, Scala, Python, and R programming languages, and supports SQL, streaming data, machine learning, and graph processing. You’ll find it used by banks, telecommunications companies, games companies, governments, and all of the major tech giants such as Apple, Facebook, IBM, and Microsoft. [ref](https://www.infoworld.com/article/3236869/what-is-apache-spark-the-big-data-platform-that-crushed-hadoop.html)

Databricks provides [a great overview](https://databricks.com/spark/about) as well.
### Rules of thumb for spark

We are learning to use Spark as a Data Scientist or Analyst which needs tools for exploring and analyzing data that pushes the boundaries of our local memory and time constraints associated with analyzing 'big' data.  As we explore this space, we will need to keep in mind a few [rules of thumb](https://en.wikipedia.org/wiki/Rule_of_thumb) that should guide our use of Spark.
#### The Spark APIs let you use your language of preference

You can use Java, Scala, Python, or R to access Spark.  Like [Goldilocks and the Three Bears](https://en.wikipedia.org/wiki/Goldilocks_principle), we want the language that is not 'too hot' or 'too cold' for data science use.  Java is a bit too verbose for day-to-day data science work, Scala is fast but still a little verbose, Python is a little slower but engrained in the data science community and R is less easy to implement in a production environment.  

- __pyspark (just right)__: The [pyspark package](https://spark.apache.org/docs/latest/api/python/index.html) looks to be the 'just the right amount' of the Spark APIs.  
- __sparkR (a little cold)__: [Apache has developed an R package](https://spark.apache.org/docs/latest/sparkr.html) that
- __sparklyr (RStudio's warm up)__: If you are experienced with the [Tidyverse](https://www.tidyverse.org/) then [RStudio's sparklyr](https://spark.rstudio.com/) could pull you away from pyspark.

You can read a comparison of sparkR and sparklyr [here](https://developpaper.com/deep-comparative-data-science-toolbox-sparkr-vs-sparklyr/).

#### Use DataFrames (ignore RDDs)

For day-to-day data science use `DataFrame`s are the option you should choose.  

1. Spark has built a framework to optimize Resilient Distributed Dataset (RDD) use when we use the `DataFrame` methods.
2. Spark internally stores `DataFrame`s in a binary format so there is no need to serialize and deserialize data as it moves over the cluster.

Databricks provides a [Deep Dive into Spark SQL’s Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) and [A Tale of Three Apache Spark APIs: RDDs vs DataFrames and Datasets](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html) to help you understand more depth on the relationship between `DataFrame`s.

The bullets an image below are taken from the Databricks articles.

> - If you want unification and simplification of APIs across Spark Libraries, use DataFrame or Dataset.
> - If you are a R user, use DataFrames.
> - If you are a Python user, use DataFrames and resort back to RDDs if you need more control.

![](spark_sql_dataframe_rdd.png)
#### Write and Read serialized data formats

The [Apache Parquet](https://databricks.com/glossary/what-is-parquet) format is optimal for most data science applications. It is a serialized columnar format that provides speed and size benefits for big data applications. The following table compares the savings as well as the speedup obtained by converting data into Parquet from CSV.

| Dataset                              | Size on Amazon S3           | Query Run Time | Data Scanned          | Cost          |
| ------------------------------------ | --------------------------- | -------------- | --------------------- | ------------- |
| Data stored as CSV files             | 1 TB                        | 236 seconds    | 1.15 TB               | $5.75         |
| Data stored in Apache Parquet Format | 130 GB                      | 6.78 seconds   | 2.51 GB               | $0.01         |
| Savings                              | 87% less when using Parquet | 34x faster     | 99% less data scanned | 99.7% savings |

You could use [Avro with Spark](https://spark.apache.org/docs/latest/sql-data-sources-avro.html) as well.  It is stored in rows, much like a `.csv` file, but is serialized.
#### Pick Spark SQL over user defined functions (UDFs)

Python or R UDFs probably are never faster than Spark SQL. Since SQL functions are relatively simple and are not designed for complex tasks it is pretty much impossible to compensate for the cost of repeated serialization, deserialization and data movement between Python or R interpreter and JVM.

Unlike UDFs, Spark SQL functions operate directly on JVM and typically are well integrated with both Catalyst and Tungsten. It means these can be optimized in the execution plan and most of the time can benefit from codgen and other Tungsten optimizations. Moreover these can operate on data in its "native" representation. UDFs, on the other hand, are implemented in Python and require moving data back and forth. [ref](https://stackoverflow.com/questions/38296609/spark-functions-vs-udf-performance)
##### Spark SQL Functions

Spark has provided [searchable documentation of the available Spark SQL APIs](https://spark.apache.org/docs/latest/api/sql/).
##### User Defined Functions

We want to avoid UDFs as much as possible. However, there are times when we have a complex calculation that goes beyond the Spark SQL functions.  The UDF syntax is unique to the language you are using with Spark.

- [UDFs in R](https://spark.apache.org/docs/latest/sparkr.html#applying-user-defined-function): [Notebook example]
- [UDFs in Python](https://docs.databricks.com/spark/latest/spark-sql/udf-python.html): [Notebook example](pyspark_udf.ipynb)
- [UDFs in Scala](https://docs.databricks.com/spark/latest/spark-sql/udf-scala.html) are not covered in this guide.

#### Caching data in memory 

In spark, the DataFrames interim computations are not stored in memory. The DataFrames are only evaluated when the action is called. If we have expensive transformations, then using `cache()` (or `persist()` for finer control) can speed up your EDA.

#### Spark Sessions need your attention

https://www.percona.com/blog/2016/08/17/apache-spark-makes-slow-mysql-queries-10x-faster/