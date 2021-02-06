## Spark connections

## Spark in R and Python

### pyspark

### sparkR and sparklyr




Much of this guide is built from the following sources.

- [sparkbyexamples](https://sparkbyexamples.com/spark/spark-performance-tuning/)
- [databricks Glossary](https://databricks.com/glossary)
- [Improve Spark performance](https://medium.com/swlh/10-ways-to-improve-spark-performance-b54e89b8d83a)
Much of this guide is built from the following sources.

- [sparkbyexamples](https://sparkbyexamples.com/spark/spark-performance-tuning/)
- [databricks Glossary](https://databricks.com/glossary)
- [Improve Spark performance](https://medium.com/swlh/10-ways-to-improve-spark-performance-b54e89b8d83a)

## Starting a pyspark session

### Import the correct methods

There are older spark instantiation methods under the `Context` names - `SparkContext`, `SQLContext`, and `HiveContext`. We don't need those with the `SparkSession` method that was introduced in Spark 2. We will use the `SparkConf` method to configure a few settings of our spark environment.  Finally, the spark SQL functions are a must to run optimized spark code.  I have elected to import them with the abbreviation `F`.

```python
from pyspark.sql import SparkSession #, SQLContext https://spark.apache.org/docs/1.6.1/sql-programming-guide.html
from pyspark import SparkConf #, SparkContext if you don't want to use SparkSession
from pyspark.sql import functions as F # access to the sql functions https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
```

### Create a session

With our methods imported we can configure our session.  These configurations are built to work with the spark configuration from [docker_guide](https://github.com/BYUI451/docker_guide). We will generally use the default configuration values in our work.  You can review all the spark configuration options [here](https://spark.apache.org/docs/latest/configuration.html).

- We will want make our spark user interface to a known port that we have opened. 
- As we will be using Postgress, we will need to provide the respective spark.jar that can be found in [docker_guide](https://github.com/BYUI451/docker_guide)
- The batch size of 10000 is the default.  Lowering this value can fix out-of-memory problems and larger values can boost memory utilization.
- We will want to specify our warehouse location so it doesn't default to the working directory of the Jupyter notebook.
- Finally, we can specify the driver memory available.  

```python
warehouse_location = os.path.abspath('../../../data/spark-warehouse') # make sure your path is set up correctly.
# Create the session
conf = (SparkConf()
    .set("spark.ui.port", "4041")
    .set('spark.jars', '/home/jovyan/scratch/postgresql-42.2.18.jar')
    .set("spark.sql.inMemoryColumnarStorage.compressed", True) # the default has changed so lets just make sure.
    .set("spark.sql.inMemoryColumnarStorage.batchSize",10000) # default
    .set("spark.sql.warehouse.dir", warehouse_location) # set above
    .set("spark.driver.memory", "7g") # lower or increase depending on your system. Local mode helps with executions as well.  
    )

# Create the Session (used to be context)
# you can move the number up or down depending on your memory and processors "local[*]" will use all.
spark = SparkSession.builder \
    .master("local[3]") \
    .appName('test') \
    .config(conf=conf) \
    .getOrCreate()
```

Please read [configuration.md](configuration.md) to see additional settings that can help during your spark session.






[^1]: https://sparkbyexamples.com/spark/spark-performance-tuning/