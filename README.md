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



































