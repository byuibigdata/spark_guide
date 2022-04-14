# Notes on spark configuration for [all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook)

### Spark Sessions need your attention

We will want to set many configurations as we leverage Spark for our data science projects.  Systems like DataBricks have optimized many of these, but we will still need to understand how to configure our Spark Sessions.  

_You can review the [configuration.md](configuration.md) to see the configurations we can use with the [all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook) built using [docker_guide](https://github.com/BYUI451/docker_guide)._


### Create a session with `all_spark_notebook` docker.

_Don't use this with DataBricks._

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

## database configuration

When you read the the documentation for [SaveAsTable](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=saveastable#pyspark.sql.DataFrameWriter.saveAsTable) you will see that it is the option to permanently save your table into your database instead of creating a temporary database that only last for the spark session using [createTempView](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame.createOrReplaceTempView). Apache Spark even has a section of their programming guide that explains [persistent tables](https://spark.apache.org/docs/2.3.1/sql-programming-guide.html#saving-to-persistent-tables) and provides the relevant pyspark code.

```python
df.write.saveAsTable("people_bucketed")
```

Their user guide provides the following assumption;

> Persistent tables will still exist even after your Spark program has restarted, as long as you __maintain your connection to the same metastore__.

So, within our [all-spark-jupyter Docker container](https://hub.docker.com/r/jupyter/all-spark-notebook) we need to start our `SparkSession` with the proper configurations so that we can maintain our connection to the metastore after we restart. We will need to add a few additional configurations to your `SparkConf()`.  

We need to specify a folder path for our `spark.sql.warehouse.dir` and `derby.system.home` using the `spark.driver.extraJavaOptions` configuration. I have set mine in the below chunk.  You will need to set your own location but I wanted mine to map to my data folder `/home/jovyan/data/spark-warehouse` as built using the information in [docker_guide](https://github.com/BYUI451/docker_guide#getting-started-using-docker-compose).

```python
import os
warehouse_location = os.path.abspath('../data/spark-warehouse')
java_options = "-Dderby.system.home=" + warehouse_location
```

We then want to add the following configurations.

```python
conf = (SparkConf()
    .set("spark.sql.warehouse.dir", warehouse_location) # set above
    .set("hive.metastore.schema.verification", False)
    .set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true")
    .set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    .set("javax.jdo.option.ConnectionUserName", 'userman')
    .set("jdo.option.ConnectionPassword", "pwd")
    .set("spark.driver.extraJavaOptions",java_options)
)
```

I also include some settings to make the memory usage on local computers a little less strenuous along with a few additional settings.  

```python
conf = (SparkConf()
    .set("spark.ui.port", "4041")
    .set('spark.jars', '/home/jovyan/scratch/postgresql-42.2.18.jar')
    .set("spark.driver.memory", "7g")  
    .set("spark.sql.warehouse.dir", warehouse_location) # set above
    .set("hive.metastore.schema.verification", False)
    .set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true")
    .set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    .set("javax.jdo.option.ConnectionUserName", 'userman')
    .set("jdo.option.ConnectionPassword", "pwd")
    .set("spark.driver.extraJavaOptions",java_options)
    .set("spark.sql.inMemoryColumnarStorage.compressed", True) # default
    .set("spark.sql.inMemoryColumnarStorage.batchSize",10000) # default
    )
```

If you have implemented the above configuration correctly, then you will see a `metastore_db` folder and a `derby.log` file in your `spark-warehouse` location once you start creating databases using `spark.sql()`.

```python
spark = SparkSession.builder \
    .master("local[3]") \
    .appName('test') \
    .config(conf=conf) \
    .getOrCreate()
```

## References

### Memory management

- https://www.pgs-soft.com/blog/spark-memory-management-part-1-push-it-to-the-limits/#:~:text=Off%2Dheap%20refers%20to%20objects,processed%20by%20the%20garbage%20collector).
- https://www.tutorialdocs.com/article/spark-memory-management.html
- https://g1thubhub.github.io/spark-memory.html
- https://stackoverflow.com/questions/43330902/spark-off-heap-memory-config-and-tungsten
- https://www.programmersought.com/article/78025859942/

### WSL 2

https://stackoverflow.com/questions/62405765/memory-allocation-to-docker-containers-after-moving-to-wsl-2-in-windows

### Partition management

https://luminousmen.com/post/spark-partitions


### Other

- https://spark.apache.org/docs/2.3.1/sql-programming-guide.html#saving-to-persistent-tables
- https://stackoverflow.com/questions/31980584/how-to-connect-spark-sql-to-remote-hive-metastore-via-thrift-protocol-with-no
- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-hive-metastore.html
- https://stackoverflow.com/questions/38377188/how-to-get-rid-of-derby-log-metastore-db-from-spark-shell/44048667#44048667
- https://stackoverflow.com/questions/45819568/why-there-are-many-spark-warehouse-folders-got-created
- https://db.apache.org/derby/papers/DerbyTut/embedded_intro.html
- https://dzone.com/articles/detailed-guide-setup-apache-spark-development-envi
- https://stackoverflow.com/questions/38377188/how-to-get-rid-of-derby-log-metastore-db-from-spark-shell