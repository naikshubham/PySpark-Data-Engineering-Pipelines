# Introduction-to-PySpark
Spark is a tool for doing parallel computation with large datasets and it integrates well with Python.

## Spark
- Spark is a platform for cluster computing. Spark lets us spread data and computations over clusters with multiple nodes(think of each node as a seperate computer).Splitting up the data makes it easier to work with very large datasets because each node only works with small amount of data.
- As each node works on its own subset of the total data, it also carries out a part of the toatl calculations required, so that both data processing and computation are performed in parallel over the nodes in the cluster.Parallel computation can make certain types of programming tasks much faster.
- However, with greater computing power comes greater complexity. Deciding whether or not Spark is the best solution for your problem takes some experience, but we can consider questions like:
- Is our data too big to work with on a single machine ?
- Can our calculations be easily parallelized ?

### Using Spark in Python
- The first step in using Spark is connecting to a cluster.
- In practise, the cluster will be hosted on a remote machine that's connected to all other nodes. There will be one computer, called the master that manages splitting up the data and the computations. The master is connected to the rest of the computers in the cluster, which are called worker. The master sends the workers data and calculations to run, and they send their results back to master.
- When we are just getting started with Spark it's simpler to just run a cluster locally.Creating the connection is as simple as creating an instance of the **`SparkContext`** class. The class constructor takes a few optional arguments that allow us to specify the attributes of the cluster we are connecting to.
- An object holding all these attributes can be created with the **`SparkConf()`** constructor.
- **Connect to a Spark cluster from PySpark by creating an instance of the `SparkContext` class**

## `SparkContext`
- SparkContext is the entry point to any spark functionality. When we run any Spark application, a driver program starts, which has the main function and SparkContext gets initiated here. The driver program then runs the operations inside the executors on worker nodes.
- By default, PySpark has SparkContext available as **`sc`**, so creating a new SparkContext won't work.

```python
from pyspark import SparkContext
sc = SparkContext("local", "First App")
```

## Using Spark DataFrames
- Spark's core data structure is the Resilient Distributed Dataset(RDD). This is a low level object that lets Spark work its magic by splitting data across multiple nodes in the cluster. However, RDDs are hard to work with directly. Here we are using **`Spark DataFrame abstraction built on top of RDDs`**.
- Spark DataFrame was designed to behave a lot like a **`SQL table`** (a table with varaibles in the columns and observations in the rows). Not only are they easier to understand, DataFrames are also more optimized for complicated operations than RDDs.
- When we start modifying and combining columns & rows of data, there are many ways to arrive at the same result, but some often take much longer than others. When using RDDs, its upto the data scientist to figure out the way to optimize the query, but the DataFrame implementation has much of its optimization built in!
- To start working with SparkDataFrames, we first have to create a **`SparkSession`** object from the **`SparkContext`**. We can think of the **`SparkContext` as the connection to the cluster** and the **`SparkSession`** as the interface with that connection.


### Creating SparkSession
- Use **`SparkSession.builder.getOrCreate()`** method to create `SparkSession`. This returns an existing **`SparkSession`** if there's already one in the environment or creates a new one if necessary.

```python
from pyspark.sql import SparkSession

my_spark = SparkSession.builder.getOrCreate()

print(my_spark)
```


### Viewing tables
- Once we have created a `SparkSession`, we can start poking around to see what data is in the cluster! The `SparkSession` has an attribute called `catlog` which lists all the data inside the cluster. This attribute has a few methods for extracting different pieces of information.
- One of the most useful is the **`.listTables()`** method, which returns the names of all the tables in the cluster as a list.


### Creating RDDs
- Parallelizing an exisiting collection of objects
- From external datasets:
1. Files in HDFS
2. Objects in Amazon S3 bucket
3. lines in a text file
4. from existing RDDs

#### Parallelized collection
- `parallelize()` for creating RDDs from python lists

```python
numRDD = sc.parallellize([1,2,3,4])
helloRDD = sc.parallelize("Hello World")
```

#### From external datasets
- `textFile()` for creating RDDs from external datasets

```python
fileRDD = sc.textFile("README.md")
```

#### Understanding Partioning in PySpark
- A partition is a logical division of a large distributed dataset
- `parallelize()` method

```python
numRDD = sc.parallelize(range(10), minPartitions = 6)

fileRDD = sc.textFile("README.md", minPartitions = 6)s
```

### PySpark operations
- Transformations create new RDDs
- Actions perform computation on the RDDs

#### map() transformation
- map() transformation applies a function to all elements in the RDD

```python
RDD = sc.parallilize([1,2,3,4])
RDD_map = RDD.map(lambda x:x*x)
```

#### flatmap() transformation
- flatmap() transformation returns multiple values for each element in the original RDD.

<img src="data/flatmap.JPG" width="350" title="flatmap">

```python
RDD = sc.parallelize(["hello world", "how are you"])
RDD_flatmap = RDD.flatMap(lambda x:x.split(" "))
```

#### union() transformation
- Returns the union of one RDD with another RDD.

<img src="data/union.JPG" width="350" title="union">

```python
inputRDD = sc.textFile("logs.txt")
errorRDD = inputRDD.filter(lambda x:"error" in x.split())
warningRDD = inputRDD.filter(lambda x:"warnings" in x.split())
combinedRDD = errorRDD.union(warningsRDD)
```

### RDD Actions
- Operation return a value after running a computation on the RDD.
- Basic RDD Actions
1. collect()
2. take(N)
3. first()
4. count()

#### collect() and take() Actions
- collect() return all the elements of the dataset as an array.
- take(N) returns an array with the first N elements of the dataset.

```python
RDD_map.collect()

RDD_map.take(2)

RDD_map.first()

RDD_flatmap.count()
```

### Working with Pair RDDs in PySpark
- Work with RDDs of Key/value pairs, which are common datatype required for many operations in Spark. 
- **Pair RDD** : Key is the identifier and value is data.

#### Creating pair RDDs
- From a list of key-value tuple
- From a regular RDD

```python
my_tuple = [('Sam', 23), ('Mary', 34), ('Peter', 25)]
pairRDD_tuple = sc.parallelize(my_tuple)

# create pair RDD from regular RDD
my_list = ['Sam 23', 'Mary 34', 'Peter 25']
regularRDD = sc.parallelize(my_list)
pairRDD_RDD = regularRDD.map(lambda s: (s.plit(' ')[0], s.split(' ')[1]))
```

#### Transformations on pair RDDs
- All regular transformations work on pair RDD
- Have to pass functions that operate on key, value pairs rather than on individual elements
- Examples of pair RDD transformations

1. reduceByKey(): Combine values with the same key
2. groupByKey() : Group values with the same key
3. sortByKey()  : Return an RDD sorted by the key
4. join()       : Join two pairs RDDs based on their key

#### reduceByKey() transformation
- It runs parallel operations for each key in the dataset
- It is a transformation and not action

```python
regularRDD = sc.parallelize([("Messi", 23), ("Ronaldo", 34), ("Neymar", 22), ("Messi", 24)])
pairRDD_reducebykey = regularRDD.reduceByKey(lambda x,y : x+y)
pairRDD_reducebykey.collect()
```

#### sortByKey() transformation
- `sortByKey()` operation orders pair RDD by key
- It returns an RDD sorted by key in ascending or descending order

```python
pairRDD_reduceByKey_rev = pairRDD_reducebykey.map(lambda x: (x[1], x[0]))
pairRDD_reducebykey_rev.sortByKey(ascending=False).collect()
```

#### groupByKey() transformation
- `groupByKey()` groups all the values with the same key in the pair RDD

```python
airports = [('US':'JFK'), ('UK':'LHR'), ('FR':'CDG'), ('US':'SFO')]
regularRDD = sc.parallelize(airports)
pairRDD_group = regularRDD.groupByKey().collect()
for cont, air in pairRDD_group:
    print(cont, list(air))
```

#### join() transformation
- joins the two pair RDDs based on their key

```python
RDD1 = sc.parallelize([("Messi", 34), ("Ronaldo", 32), ("Neymar", 24)])
RDD2 = sc.parallelize([("Ronaldo", 80), ("Neymar", 120), ("Messi", 100)])

RDD1.join(RDD2).collect()
```

### Advanced RDD Actions

#### reduce() action
- reduce(func) action is used for aggregating the elements of a regular RDD.
- The function should be commutative(changing the order of the operands does not change the result) and associative

```python
x = [1,3,4,6]
RDD = sc.parallelize(x)
RDD.reduce(lambda x,y : x+y)
```

#### saveAsTextFile() action
- It is not advisable to run collect action on RDDs because of the huge size of the data. In these case, it's common to write data out to a distributed storage systems such as HDFS or Amazon S3.
- `saveAsTextFile()` action saves RDD into a text file inside a directory with each partition as a separate file.
- `coalesce()` method can be used to save RDD as a single text file.

```python
RDD.saveAsTextFile("tempFile")

RDD.coalesce(1).saveAsTextFile("tempFile")
```

#### countByKey() action
- `countByKey()` only available for type(K,V). countByKey should only be used on a dataset whose size is small enough to fit in memory.
- It counts the number of elements for each key.

```python
rdd = sc.parallelize([("a", 1), ("b", 1) , ("a", 1)])
for key, val in rdd.countByKey().items():
    print(key, val)
```

#### collectAsMap() action
- Returns the key-value pairs in the RDD as a dictionary

```python
sc.parallelize([(1,2), (3,4)]).collectAsMap()
```

### PySpark DataFrames
- PySpark SQL is Spark's high level API for working with structured data. PySpark SQL is a Spark Library for structured data. It provides more info about the structure of data and computation.
- PySpark SQL provides a programming abstraction called DataFrames. A DataFrame is an immutable distributed collection of data with named columns. It is similar to a table in SQL.
- DataFrames are designed to process a large collection of structured data such as relational database and semi-structured data such as JSON. 
- DataFrames in PySpark support both SQL queries `(SELECT * from table)` or expression methods `(df.select())`

#### SparkSession - Entry point for DataFrame API
- SparkContext is the main entry point for creating RDDs.
- Similarlily, **SparkSession** provides a single point of entry to interact with Spark DataFrames.
- The SparkSession does for DataFrames what the SparkContext does for RDDs.
- SparkSession is used to create DataFrame, register DataFrames, execute SQL queries.
- SparkSession is available in PySpark shell as **spark**

#### Creating DataFrames in PySpark
- Two different methods of creating DataFrames in PySpark.

1. From existing RDDs using SparkSession's DataFrame() method
2. From various data sources (CSV, JSON, TXT) using SparkSession's read method.

- Schema is the structure of data in DataFrame and helps Spark to optimize queries on the data more efficiently. Schema provides details such as column name, the type of data in the column, and whether null or empty values are allowed in the column.

#### Creating a DataFrame from RDD
- Pass an RDD and a schema into SparkSession's create DataFrame method.

```python
iphones_RDD = sc.parallelize([("XS", 2018, 5.65, 2.79, 6.24),
                              ("XR", 2018, 5.94, 2.98, 6.84)])
names = ['Model', 'Year', 'Height', 'Width', 'Weight']

iphones_df = spark.createDataFrame(iphones_RDD, schema=names)
type(iphones_df)
```

#### Create a DataFrame from reading a CSV/JSON/TXT

```python
df_csv = spark.read.csv("people.csv", header=True, inferSchema=True)

df_json = spark.read.json("people.json", header=True, inferSchema=True)

df_txt = spark.read.txt("people.txt", header=True, inferSchema=True)
```

### Interacting with PySpark DataFrames
- PySpark DataFrame provides operations to filter, group, or compute aggregates, and can be used with PySpark SQL.
- DataFrame Transformations : select, filter, groupby, orderby, dropDuplicates, withColumnRenamed, printSchema, show ,count ,columns, describe.

#### select() and show() operations
- `select()` transformation subsets the columns in the DataFrame.
- `show()` action prints first 20 rows in the DataFrame

```python
df_id_age = test.select('Age')
df_id_age.show(3)
```

#### filter() and show() operations
- `filter()` transformation filters out the rows based on a condition.

```python
new_df_age21 = new_df.filter(new_df.Age > 21)
new_df_age21.show(3)
```

#### groupby() and count() operations
- `groupby()` operation can be used to group a variable

```python
test_df_age_group = test_df.groupby("Age")
test_df_age_group.count().show(3)
```

#### orderby() transformation
- `oederby()` transformation sorts the DataFrame based on one or more columns.

```python
test_df_age_group.count().orderBy('Age').show(3)
```

#### dropDuplicates()
- `dropDuplicates()` removes the duplicate rows of a DataFrame

```python
test_df_no_dup = test_df.select('User_ID', 'Gender', 'Age').dropDuplicates()
test_df_no_dup.count()
```

#### withColumnRenamed Transformations
- `withColumnRenamed()` renames a column in the DataFrame

```python
test_df_sex = test_df.withColumnRenamed('Gender', 'Sex')
test_df_sex.show(3)
```

#### printSchema(), columns, describe()
- `printSchema()` operation prints the types of columns in the DataFrame
- `describe()` operation computes summary statistics of numerical columns in the DataFrame

```python
test_df.printSchema()
test_df.columns
test_df.describe().show()
```
































