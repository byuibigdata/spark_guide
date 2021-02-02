# Notes on spark configuration

## Memory management

- https://www.pgs-soft.com/blog/spark-memory-management-part-1-push-it-to-the-limits/#:~:text=Off%2Dheap%20refers%20to%20objects,processed%20by%20the%20garbage%20collector).
- https://www.tutorialdocs.com/article/spark-memory-management.html
- https://g1thubhub.github.io/spark-memory.html
- https://stackoverflow.com/questions/43330902/spark-off-heap-memory-config-and-tungsten
- https://www.programmersought.com/article/78025859942/

### WSL 2

https://stackoverflow.com/questions/62405765/memory-allocation-to-docker-containers-after-moving-to-wsl-2-in-windows

## Partition management

https://luminousmen.com/post/spark-partitions

## database configuration

```
    .set("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=/home/jovyan/data/metastore_db;create=true")
    .set("spark.driver.extraJavaOptions", "-Dderby.system.home=/home/jovyan/data/")
```

- https://stackoverflow.com/questions/45819568/why-there-are-many-spark-warehouse-folders-got-created
- https://db.apache.org/derby/papers/DerbyTut/embedded_intro.html
- https://dzone.com/articles/detailed-guide-setup-apache-spark-development-envi
- https://stackoverflow.com/questions/38377188/how-to-get-rid-of-derby-log-metastore-db-from-spark-shell