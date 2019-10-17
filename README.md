# Introduction-to-PySpark
Spark is a tool for doing parallel computation with large datasets and it integrates well with Python.

## Spark
- Spark is a platform for cluster computing. Spark lets us spread data and computations over clusters with multiple nodes(think of each node as a seperate computer).Splitting up the data makes it easier to work with very large datasets because each node only works with small amount of data.
- As each node works on its own subset of the total data, it also carries out a part of the toatl calculations required, so that both data processing and computation are performed in parallel over the nodes in the cluster.Parallel computation can make certain types of programming tasks much faster.
- However, with greater computing power comes greater complexity. Deciding whether or not Spark is the best solution for your problem takes some experience, but we can consider questions like:
- Is our data too big to work with on a single machine ?
- Can our calculations be easily parallelized ?