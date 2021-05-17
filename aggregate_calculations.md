# Completing aggregated calculations using Spark

Generally, we will want to perform calculations by some grouping within our data.  For example, we may want to know the number of employees in each business unit or the amount of sales by month. 

Sometimes, we want a new table based on the grouping variable.  Other times, we will want to keep our observational unit of the original table but add additional columns with the summary variable appended. In SQL, we will differentiate the two calculations by the `GROUP BY` method and the `WINDOW` method.

![](images/groupby-window.png)

## GROUP BY

When using 'GROUP BY' functions or methods in the varied languages of data science the observational unit (row) of the resulting table is defined by the levels of the variable used in the 'GROUP BY' argument. We move from many rows to fewer rows.  

![](images/groupby.png)

### Language specific help files

- [SQL: GROUP BY](https://www.w3schools.com/sql/sql_groupby.asp)
- [dplyr: group_by()](https://dplyr.tidyverse.org/reference/group_by.html)
- [Pandas: df.groupby()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)
- [Pyspark: df.groupBy()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html)

The `GROUP BY` methods of each language must be combined with their respective calculation process.

- [SQL: calcluated fields](https://joequery.me/notes/sql-calculated-fields/)
- [dplyr: summarize()](https://dplyr.tidyverse.org/reference/mutate.html) and read their [window example](https://dplyr.tidyverse.org/articles/window-functions.html)
- [Pandas: .agg()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.agg.html)
- [Pyspark: .agg()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html)


### Examples

Here are example calculations using Pyspark and SQL.

We have our default `DataFrame`


|   Section | Student   |   Score |
|----------:|:----------|--------:|
|         1 | a         |      90 |
|         2 | b         |      85 |
|         2 | c         |      75 |
|         3 | d         |      95 |
|         3 | e         |      65 |
|         3 | f         |      98 |

And we want the following table.

|   Section | Student   |   Score |   rank |   min |
|----------:|:----------|--------:|-------:|------:|
|         1 | a         |      90 |      1 |    90 |
|         2 | b         |      85 |      1 |    75 |
|         2 | c         |      75 |      2 |    75 |
|         3 | d         |      95 |      2 |    65 |
|         3 | e         |      65 |      3 |    65 |
|         3 | f         |      98 |      1 |    65 |


#### Pyspark

```python
from pyspark.sql import Window
import pyspark.sql.functions as F
import pandas as pd

# create pandas dataframe
pdf = pd.DataFrame({'Section':[1,2,2,3,3,3], 'Student':['a','b','c', 'd', 'e','f'], 'Score':[90, 85, 75, 95, 65, 98]})
# convert to spark dataframe
df = spark.createDataFrame(pdf)

window_order = Window.partitionBy('Section').orderBy(F.col('Score').desc())
window = Window.partitionBy('Section')

df.withColumn("rank", F.rank().over(window_order)) \
  .withColumn("min", F.min('Score').over(window)) \
  .sort('Student') \
  .show()
```

#### SQL



## Window

> At its core, a window function calculates a return value for every input row of a table based on a group of rows, called the Frame. Every input row can have a unique frame associated with it. This characteristic of window functions makes them more powerful than other functions and allows users to express various data processing tasks that are hard (if not impossible) to be expressed without window functions in a concise way. Now, let’s take a look at two examples. [ref](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)

### Language specific help files

- [SQL: OVER(PARTITION BY <column>)](https://mode.com/sql-tutorial/sql-window-functions/)
- [dplyr: mutate()](https://dplyr.tidyverse.org/articles/window-functions.html)
- [Pandas: transform()](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.transform.html)
- [Pyspark: .over() with pyspark.sql.Window()](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.Column.over.html) and [this Databricks guide](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)

### Examples

Here are example calculations using Pyspark and SQL.

#### Pyspark

#### SQL

## References

- https://stackoverflow.com/questions/53647644/how-orderby-affects-window-partitionby-in-pyspark-dataframe
- 
