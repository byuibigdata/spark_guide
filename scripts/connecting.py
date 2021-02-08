# %%
import pandas as pd
import numpy as np
import os
from pyspark import SparkConf #, SparkContext 
from pyspark.sql import SparkSession #, SQLContext https://spark.apache.org/docs/1.6.1/sql-programming-guide.html
from pyspark.sql import functions as F # access to the sql functions https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
from IPython.display import HTML


# %%
warehouse_location = os.path.abspath('../../../data/spark-warehouse')
java_options = "-Dderby.system.home=" + warehouse_location
print(warehouse_location)
print(java_options)
# make sure you have set the warehouse location to 'hom/jovyan/data/spark-warehouse'


# %%
os.path.normpath("/home/jovyan/data/spark-warehouse") == warehouse_location



# %%
# Create the session
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

# ("hive.metastore.uris", "thrift://192.168.175.160:9083")
# (hive.metastore.schema.verification, False)



# Create the Session (used to be context)
# you can move the number up or down depending on your memory and processors "local[*]" will use all.
spark = SparkSession.builder     .master("local[3]")     .appName('test')     .config(conf=conf)     .enableHiveSupport()     .getOrCreate()


# %%
# spark.stop()
# spark.sql('DROP DATABASE IF EXISTS irs990 CASCADE;')
# spark.sql("create database irs990")
spark.sql("SHOW DATABASES").show()

# %% [markdown]
# ## Pulling data into our Spark environment
# 
# In this example, I am using a postgres database as specified in the below properties based on the 990 irs database we are using in CSE 451. If you don't have access to the database, you use the `.csv` file found in `/scratch`. 
# 
# ### pushing our csv file to a database

# %%
spark.sql('DROP DATABASE IF EXISTS mycsv CASCADE;')
spark.sql("create database mycsv")
spark.sql("USE mycsv")
diamonds = spark.read.format("csv").load("../../../scratch/diamonds.csv")
print(diamonds.limit(5).show(5))
diamonds.write.mode("overwrite").saveAsTable("diamonds")
spark.sql('SHOW TABLES IN mycsv').show()

# %% [markdown]
# ### Pulling from our postgress irs990 database into our Spark database
# 
# For our CSE 451 students that have access to our database the following sections can be used to see the performance benefits of Spark for large tables.

# %%
properties = {
    'driver': 'org.postgresql.Driver',
    'url': 'jdbc:postgresql://c451_db_1:5432/irs990',
    'user': 'postgres',
    'password': 'postgres1234',
}


# %%
# schema table

schema = (spark.read.format('jdbc')
    .option('driver', properties['driver'])
    .option('url', properties['url'])
    .option('dbtable', "information_schema.tables")
    .option('user', properties['user'])
    .load())


# %%
tables = (schema
    .toPandas()
    .filter(['table_catalog', 'table_schema', 'table_name'])
    .query('table_schema == "public"'))


# %%
HTML(tables.to_html())


# %%
# import shutil
# shutil.rmtree('/home/jovyan/data/spark-warehouse/metastore_db')
# shutil.rmtree('/home/jovyan/data/spark-warehouse')
# shutil.rmtree('/home/jovyan/data/spark-warehouse/irs990.db/address_table')
# shutil.rmtree('/home/jovyan/data/spark-warehouse/irs990.db/return_EZOffcrDrctrTrstEmpl')
# shutil.rmtree('/home/jovyan/data/spark-warehouse/irs990.db/tmp_990ez_employees')

# spark.sql('DROP TABLE IF EXISTS return_EZOffcrDrctrTrstEmpl')
# spark.sql('DROP TABLE IF EXISTS address_table')
# spark.sql('DROP TABLE IF EXISTS tmp_990ez_employees')


# %%
return_EZOffcrDrctrTrstEmpl = (spark.read.format('jdbc')
    .option('driver', properties['driver'])
    .option('url', properties['url'])
    .option('dbtable', "return_EZOffcrDrctrTrstEmpl")
    .option('user', properties['user'])
    .load())


# %%
address = (spark.read.format('jdbc')
    .option('driver', properties['driver'])
    .option('url', properties['url'])
    .option('dbtable', 'address_table')
    .option('user', properties['user'])
    .load())


# %%
print(address.count())
print(return_EZOffcrDrctrTrstEmpl.count())


# %%
print('Number of partitions: {}'.format(address.rdd.getNumPartitions()))
print('Number of partitions: {}'.format(return_EZOffcrDrctrTrstEmpl.rdd.getNumPartitions()))


# %%
address = address.repartition(50)
return_EZOffcrDrctrTrstEmpl = return_EZOffcrDrctrTrstEmpl.repartition(50)

# %%
print('Number of partitions: {}'.format(address.rdd.getNumPartitions()))
print('Number of partitions: {}'.format(return_EZOffcrDrctrTrstEmpl.rdd.getNumPartitions()))


# %%
spark.sql("USE irs990")
address.write.saveAsTable("address_table")
return_EZOffcrDrctrTrstEmpl.write.saveAsTable('return_EZOffcrDrctrTrstEmpl')


# %%
spark.sql('SHOW TABLES IN irs990').show()


# %%
# https://github.com/jsfenfen/990-xml-database/blob/master/directors.sh

# DROP TABLE IF EXISTS tmp_990ez_employees;
# SELECT address_table.*,
# 	'/IRS990EZ' as form,
#    "PrsnNm",
#    "TtlTxt",
#    "CmpnstnAmt" 
#    INTO temporary table tmp_990EZ_employees
#    FROM return_EZOffcrDrctrTrstEmpl
# 	LEFT JOIN address_table ON return_EZOffcrDrctrTrstEmpl.ein = address_table.ein
# 	AND return_EZOffcrDrctrTrstEmpl.object_id= address_table.object_id;


# %%
temp = spark.sql('select * from tmp_990ez_employees')
temp.limit(20).toPandas()


