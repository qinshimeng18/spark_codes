# 知识点
- RDD的相关概念，了解HDFS的分布式存储，mapreduce的思想，程序的并行化进行
- Spark分为转换操作和动作，DAG的资源计算图只有在action的时候才会触发运算，只有中间结果被cache、persis的时候才会暂存，否则会重复计算
- broadcast和accumulator共享变量，局部的global也是local的
- spark的DF操作
- spark的graph操作
- spark的机器学习库
- stream操作
- Hbase和Hive

### 并行程序问题： 
- 死锁问题：信号量+互斥锁  
- restricted - 约束   deterministic - 确定的
### RDD  
Spark 核心的概念是 Resilient Distributed Dataset (RDD)：一个可并行操作的有容错机制的数据集合。有 2 种方式创建 RDDs：第一种是在你的驱动程序中并行化一个已经存在的集合；另外一种是引用一个外部存储系统的数据集，例如共享的文件系统，HDFS，HBase或其他 Hadoop 数据格式的数据源。（textfile）    
partition是spark rdd计算的最小单元。  
Spark RDD主要由Dependency、Partition、Partitioner组成。  
    1. Partition记录了数据split的逻辑，  
    2. Dependency记录的是transformation操作过程中Partition的演化，  
    3. Partitioner是shuffle过程中key重分区时的策略，即计算key决定k-v属于哪个分区  
#### 并行化  
并行集合 (Parallelized collections) 的创建是通过在一个已有的集合(Scala Seq)上调用 SparkContext 的 parallelize 方法实现的。集合中的元素被复制到一个可并行操作的分布式数据集中。例如，这里演示了如何在一个包含 1 到 5 的数组中创建并行集合：
```python
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
```
### Map-Reduce  
- store: GFS-google distribute file system  
### Hadoop  
- open source of google GFS and MR  
- Master(task tracker+name node | 64MB chunk 字典) + Slave(task tracker+data node 存储chunk,复制三份，在不同的slaver node中)  
- HDFS+Hadoop MR  
- 心跳 
- Map-> 分配到不同的worker|Prase-hash : convert str into a number(means the index of woker) -> Reduce
- 缺点： 
> lost transaction features etc.  
> 暴力代替索引  
- 组成  
> master: **job tracker**    NameNode  
> worker(slaver):map-reduce+ DataNode  **task tracker**  

### Spark
- features:  
rich api;in-memory storage;**execution graph**;interactive shell  
- Structure and features  
线性计算流图，计算完毕会丢弃结果，再使用会根据计算图从如何产生的地方再计算一遍
- 转换操作： Transform:,map,filter,repartition,union,distinct
    - map等，每一个数据集元素传递给一个函数并且返回一个新的 RDD，并不会立即进行计算，并且转换的中间结果默认一次性
- Action：reduce,count,first,takeSample
    - reduce 是一个动作，集合所有元素并返回
- still HDFS layer  
RDD store 1 copy not 3 like hdfs  
master separate RDDs  
data transpart via pipeline,to learner formed different rdds, no need to store intermediate results  
if one failed, just recompute in parallel and in different nodes from compute graph, not rollback.  
lines.cache()/persist(): 暂存，计算完不会立马被丢弃，从而避免计算多遍  
LRU 丢弃最久未使用的旧数据 .即使原始数据变了，也没有关系  
python global varibale 会被送到每一个executor,python函数也会被发送到不同rdd上


```python   
val lines = sc.textFile("data.txt")  
val lineLengths = lines.map(s => s.length)  
val totalLength = lineLengths.reduce((a, b) => a + b)
```
> 第一行是定义来自于外部文件的 RDD。这个数据集并没有加载到内存或做其他的操作：lines仅仅是一个指向文件的指针。第二行是定义 lineLengths，它是 map转换(transformation)的结果。同样，lineLengths由于懒惰模式也没有立即计算。最后，我们执行 reduce，它是一个动作(action)。在这个地方，Spark把计算分成多个任务(task)，并且让它们运行在多个机器上。每台机器都运行自己的 map 部分和本地reduce 部分。然后仅仅将结果返回给驱动程序。  



Internal:  
### Partition：

查看JOB：http://127.0.0.1:4040/jobs/job/?id=0  
每一个worker（machine）可以包含多个partition，partition一般是处理器核心的数量   
1、repartition类的操作：比如repartition、repartitionAndSortWithinPartitions、coalesce等  
2、byKey类的操作：比如reduceByKey、groupByKey、sortByKey、countByKey等  
3、join类的操作：比如join、cogroup等  

Spark支持：  
Two kinds of partitioning available in Spark:  
- Hash partitioning  
- Range partitioning  

数据调用hashCode函数分配数据：  
In general, hash partitioning allocates tuple (k, v) to partition p where 
p = k.hashCode() % numPartitions
Usually works well but be aware of bad inputs!  
例如：
We see that it hashed all numbers x such that x mod 8 = 1 to partition #1    

def glom(): RDD[Array[T]]

该函数是将RDD中每一个分区中类型为T的元素转换成Array[T]，这样每一个分区就只有一个数组元素   
when the default partition function (hashing) doesn’t work well -》 a custom partition function  
Specify the partition function in transformations like reduceByKey, groupByKey


Map:这里有一个性能问题，map因为可能会改变key，而partition是按照key来分的，所以就需要收回然后再hash->partition分下去执行；而mapvalues只变value，所以不用再partition  
- map(function)   
map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。  
- mapPartitions(function)   
map()的输入函数是应用于RDD中每个元素，而mapPartitions()的输入函数是应用于每个分区  
- mapValues(function)   
原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。因此，该函数只适用于元素为KV对的RDD  
- flatMap(function)   
与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素，***会消去一级进行合并，变成一个list***  

- 分区操作  
（1）mapPartitions
def mapPartitions[U](f: (Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
该函数和map函数类似，只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。如果在映射的过程中需要频繁创建额外的对象（如数据库连接对象），使用mapPartitions要比map高效的多。
比如，将RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，这样开销很大，如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。

依赖问题：
wide dependencies：不在一个partition上面，需要shuffle？
narrow；；

shuffle: 提前做好平衡，费资源
repartition: same tumple column is in the same partition


Job：真正的运行起来动作
很多个stage组成一个job,每一个thread可以运行一个job达到并行的效果

一个RDD 只能是一个key-value pair，可以被分成多个partition，一般来说每个worker(一般是core 的数量，即JVM)运行两个，但是spark会把慢的partition传到快的worker上运行

## 数据倾斜
- 为skew的key增加随机前/后缀  
原理
为数据量特别大的Key增加随机前/后缀，使得原来Key相同的数据变为Key不相同的数据，从而使倾斜的数据集分散到不同的Task中，彻底解决数据倾斜问题。Join另一则的数据中，与倾斜Key对应的部分数据，与随机前缀集作笛卡尔乘积，从而保证无论数据倾斜侧倾斜Key如何加前缀，都能与之正常Join。
- 将Reduce side Join转变为Map side Join  
- 自定义Partitioner  
原理
- 使用自定义的Partitioner（默认为HashPartitioner），将原本被分配到同一个Task的不同Key分配到不同Task。  
- 使用Partitioner必须满足两个前提，1、RDD是k-v形式，如RDD[(K, V)]，2、有shuffle操作。常见的触发shuffle的操作有： 
    1.combineByKey(groupByKey, reduceByKey , aggregateByKey) 
    2. sortByKey 
    3. join(leftOuterJoin, rightOuterJoin, fullOuterJoin) 
    4. cogroup 
    5. repartition(coalesce(shuffle=true)) 
    6. groupWith 
    7. repartitionAndSortWithinPartitions


## Classical Divide-and-Conquer:  

只有全部的结果，没有中间的或者prefix result（前p个原元素的和）  
计算prefix的parallel：针对序列计算，如何进行并行化
Algorithm:
- Compute sum for each partition 每个partition的小sum
- Compute the prefix sums of the 𝑝 
- Compute prefix sums in each partition

x = [1, 4, 3, 5, 6, 7, 0, 1]  
sum = [5, 8, 13, 1] mapPartitions
prefix_sum = [1, 5, 8, 13, 19, 26, 26, 27]  mapPartitionsWithIndex  

### 共享变量
spark将函数和变量的独立副本传递给workers，更新不会返回给驱动程序，是的跨任务读写速度很慢
- 广播变量（broadcast variable）
SparkContext.broadcast(v)方法从一个初始变量v中创建。广播变量是v的一个包装变量，它的值可以通过value方法访问
- 累加器（accumulator）
通过调用SparkContext.accumulator(v)方法从一个初始变量v中创建。运行在集群上的任务可以通过add方法或者使用+=操作来给它加值。然而，它们无法读取这个值。只有驱动程序可以使用value方法来读取累加器的值

# set

set有两种类型，set和frozenset。

set是可变的，有add（），remove（）等方法。既然是可变的，所以它不存在哈希值。

frozenset是冻结的集合，它是不可变的，存在哈希值，好处是它可以作为字典的key，也可以作为其它集合的元素。缺点是一旦创建便不能更改，没有add，remove方法。


[img](http://dongguo.me/images/personal/engineering/spark/spark-components.png)


### 正确地使用广播变量(broadcast variables)
- 广播变量允许程序员将一个只读的变量缓存在每台机器上，而不用在任务之间传递变量。广播变量可被用于有效地给每个节点一个大输入数据集的副本。
一个Executor只需要在第一个Task启动时，获得一份Broadcast数据，之后的Task都从本节点的BlockManager中获取相关数据。

- 如果我们有一份const数据，需要在executors上用到，一个典型的例子是Driver从数据库中load了一份数据dbData，在很多RDD操作中都引用了dbData，这样的话，每次RDD操作，driver node都需要将dbData分发到各个executors node一遍（分享1中已经介绍了背景），这非常的低效，特别是dbData比较大且RDD操作次数较多时。Spark的广播变量使得Driver可以提前只给各个executors node传一遍（spark内部具体的实现可能是driver传给某几个executors，这几个executors再传给其余executors）。使用广播变量有一个我犯过的错误如下：

``` javascript
 val brDbData = sparkContext.broadcast(dbData) //broadcast dbDataA, and name it as brDbData
 val dbDataB = brDbData.value //no longer broadcast variable
 oneRDD.map(x=>{dbDataB.getOrElse(key, -1); …})
 ```
第一行将dbData已经广播出去且命名为brDbData，一定要在RDD操作中直接使用该广播变量，如果提前提取出值，第三行的RDD操作还需要将dbData传送一遍。正确的代码如下

 ``` javascript
 val brDbData = sparkContext.broadcast(dbData) //broadcast dbDataA, and name it as brDbData
 oneRDD.map(x=>{brDbData.value.getOrElse(key, -1); …})
 ```
Spark本身支持做batch的计算，比如每天机器学习模型的训练，各种数据的处理；
Spark Streaming可以用来做realtime计算和数据处理，Spark Streaming的API和Spark的比较类似，其实背后的实现也是把一段段的realtime数据用batch的方式去处理；
MLlib实现了常用的机器学习和推荐算法，可以直接用或者作为baseline；
Spark SQL使得可以通过SQL来对Hive表，Json文件等数据源进行查询，查询会被转变为一个Spark job；

- 一般可以设置task的数量是core的2-3倍，让CPU不空闲
- 
### stream  
- streamingContext.start()  
Start receiving data and processing it using 
- streamingContext.awaitTermination()  
Wait for the processing to be stopped 
- streamingContext.stop()  
(manually or due to any error) using The processing can be manually stopped using streamingContext.stop().  
. To stop only the StreamingContext： ssc.stop(False)
- DStreams can be created   
    - from input data streams 
    - by applying high-level operations on other DStreams.
#### Streaming MG analysis


### Hbase table 列存储
高随机读写不支持join，
An open-source version of Google BigTable  
HBase is a distributed column-oriented data store built on top of HDFS  
HBase is an Apache open source project whose goal is to provide storage for Hadoop Distributed Computing   
Data is logically organized into tables, rows and columns 
- Each row has a Key
- Each record is divided into Column Families
- Each column family consists of one or more Columns
- HBase schema consists of several Tables  
    Each table consists of a set of Column Families  
    Columns are not part of the schema 
#### Hive: data warehousing application in Hadoop 类似sql  
Query language is HQL, variant of SQL  
Tables stored on HDFS as flat files  
Developed by Facebook, now open source  
Hive looks similar to an SQL database
Support Joins

### 函数：
glom：  
将RDD中每一个分区中类型为T的元素转换成Array[T]，这样每一个分区就只有一个数组元素。


Platform as a Service: 平台即服务, 是面向软件开发者的服务, 云计算平台提供硬件,
SaaS: 软件即服务, 是面向软件消费者的

# 常用代码和问题


```python 
# 在Notebook中运行spark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)

# 分区两种方式  可以用 partitionBy(4,lambda x:x%4)
rdd = sc.parallelize([('a', 1), ('a', 2), ('b', 1), ('b', 3), ('c',1), ('ef',5)])
rdd1 = rdd.repartition(4)
rdd1.glom().collect()

# reduceByKey 相同的key相加，False是降序
 pairs.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1], False).zipWithIndex().map(lambda x:(x[1],x[0]))
 
 #graph 操作
 rule12 = g.find('(a)-[]->(b)').filter('a.id == "h"').select('b.name')
 
 # DF操作  .toDF()
TotalPrice = dfDetail.select('*',(dfDetail.UnitPrice*dfDetail.OrderQty*(1-dfDetail.UnitPriceDiscount)).alias('netprice'))\
            .groupBy('SalesOrderID').sum('netprice')\
                .withColumnRenamed('sum(netprice)','TotalPrice')

```

```python

#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
import csv
import random
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import time


def genTransactions(inpath):
    """ Generates a list of transactions, where each transaction is a set of items. 
    """
    ids, ts = list(), list()
    with open(inpath, 'r') as f:
        for line in f:
            i, t = line.strip().split('\t')
            ids.append(i)
            ts.append(set(t.split(',')))
    return ids, ts


def prune(curr, F):
    for i in curr:
        tmp = curr - {i}
        if not tmp in F: return False
    return True


def genCand(F):
    C = set()
    s = len(F[0]) + 1
    for i in xrange(len(F)-1): 
        for j in xrange(i+1, len(F)):
            curr = F[i].union(F[j])
            if len(curr) == s:
                C.add(curr) # tuple: (frozenset(shared_elems), new_elem)
    return list(C)


def getSupport(x, n, values):
    c = 0
    for t in values:
        if x.issubset(t):
            c += 1
    return (frozenset(x), c/n)


def check(x, values):
    for v in values:
        if x.issubset(v):
            return (frozenset([x]),1)
    return (frozenset([x]),0)



def genToyTransactions():
    """ Generates a list of transactions, where each transaction is a set of items. 
    """
    T = list()
    T.append({1,3,4})
    T.append({2,3,5})
    T.append({1,2,3,5})
    T.append({2,5})
    return T


sc = SparkContext()

def apriori(T,minsup,niters):

    n = float(len(T))
    results = dict() #results[k] saves k-item frequent set

    trans = sc.parallelize(T,NUM_PARTITIONS)
    trans_set = trans.map(lambda x: frozenset(x)).cache()

    # Gets one-item candidate set
    trans_set_one = trans.flatMap(lambda x: x)
    unique_one = trans_set_one.map(lambda x: (frozenset([x]),1)).reduceByKey(lambda x,y:x+y)

    # Gets one-item frequent set
    freq_set_values = unique_one.filter(lambda x: x[1]/n>=minsup)
    freq_set_one = freq_set_values.map(lambda x:x[0]).collect()
    trans_set_bc = sc.broadcast(trans_set.collect())

    c = 1
    results[c] = freq_set_one
    while c < niters+1:
        if len(results[c]) == 0:
            print c
            break
        print ('== %s th iteration ==' % str(c))
        t0 = time.time()
        cand = genCand(results[c])
        print 'join set time = ', time.time() - t0
        candidates = sc.parallelize(cand,NUM_PARTITIONS)
        # result_c_bc = sc.broadcast(results[c])
        # cand_set_pruned = candidates.filter(lambda x: prune(x,result_c_bc.value)).cache()
        cand_set_pruned = candidates.filter(lambda x: prune(x,results[c])).cache()

        t0 = time.time()
        freq_set = cand_set_pruned\
                               .map(lambda x: getSupport(x, n, trans_set_bc.value))\
                                                   .filter(lambda x:x[1]>=minsup)\
                                                                .map(lambda x: x[0]).collect()
        # freq_set = cand_set_pruned.map(lambda x: check(x, trans_set_bc.value)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]/n>=minsup).map(lambda x: x[0]).collect()

        print 'freq time = ', time.time()-t0

        c += 1
        results[c] = freq_set
    return results 

NUM_PARTITIONS = 1
if __name__ == '__main__':
    
    minsup = 0.001 #minimum support ratio
    niters = 2 #number of iterations
    maxlength = 5
    

    inpath = 'data1/transactions.txt'
    ids, T = genTransactions(inpath)
    del ids
    newT = list()
    for i in range(len(T)):
        if len(T[i]) < maxlength:
            newT.append(T[i])
    T = newT
    

    # T = genToyTransactions()

    print '\nNUM PARTITIONS: ', NUM_PARTITIONS
    print 'MUN TRANSACTIONS = ', len(T)
    print 'MINSUP = ', minsup
    print 

    t_start = time.time()
    results = apriori(T,minsup,niters)
    t_end = time.time()
    # print results 
    print 'TOTAL TIME USED = ', t_end-t_start

    # print results




```
