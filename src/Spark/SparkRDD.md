# Spark RDD

<!-- toc -->

## 概述

在Apache Spark中，RDD（Resilient Distributed Dataset，弹性分布式数据集）是Spark的核心概念之一，是一种不可变的分布式数据集。RDD为大规模数据处理提供了一种高效且容错的方式。

RDD：分布式计算模型

1. 一定是一个对象
2. 一定封装了大量方法和属性（计算逻辑）
3. 一定需要适合进行分布式处理（降低数据规模，实现并行计算）

分布式集群中心化基础架构——主从架构（Master-Slave）

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20241126153006.png)

## 详细介绍

以下是对RDD的详细介绍：

### 1. 基本概念

- **弹性**：RDD支持在节点故障时自动恢复丢失的数据。它通过记录数据的来源（血统信息）来实现这一点。
- **分布式**：RDD的数据会被分散存储在集群的多个节点上，利用并行计算提高数据处理的效率。
- **不可变性**：一旦创建，RDD中的数据不能被改变。这意味着对RDD的操作会返回一个新的RDD，而不是修改原有的RDD。

### 2. 创建RDD

RDD可以通过以下几种方式创建：
- **从已有的集合**：可以直接从本地的Python、Scala等集合创建RDD。例如，在Scala中使用`sc.parallelize()`方法。
- **从外部存储**：可以从HDFS、S3等外部存储系统读取数据，比如使用`sc.textFile()`方法来读取文本文件。

### 3. RDD操作

RDD支持两类操作：转换和行动。

#### 转换（Transformations）

转换是指对RDD进行的一种操作，生成一个新的RDD。转换是惰性执行的，即只有在需要结果时才会被计算。常见的转换操作包括：
- `map(func)`：对RDD中的每个元素应用`func`函数，返回一个新的RDD。
- `filter(func)`：根据给定的`func`函数进行筛选，返回满足条件的元素的新RDD。
- `flatMap(func)`：类似于`map`，但每个输入元素可以映射到零个或多个输出元素。
- `union(otherRDD)`：返回一个新的RDD，包括两个RDD的所有元素。
- `join(otherRDD)`：对两个RDD进行连接操作，返回一个包含键值对的新RDD。

#### 行动（Actions）

行动是指对RDD进行的操作，会触发计算并返回结果。不像转换，行动会立即执行。常见的行动操作包括：
- `collect()`：从所有节点收集数据到驱动程序，返回一个包含所有数据的数组。
- `count()`：返回RDD中元素的数量。
- `first()`：返回RDD中的第一个元素。
- `take(n)`：返回RDD中的前n个元素。
- `saveAsTextFile(path)`：将RDD的数据保存到指定路径的文本文件中。

### 4. 容错机制

RDD的容错机制是通过血统（Lineage）来实现的。每个RDD都maintains了其转换操作的一个有向无环图。在发生节点故障时，Spark可以根据这个血统信息重新计算丢失的分区，确保数据的完整性和可靠性。

### 5. 计算模型

RDD采用延迟计算的模型，实际的计算只有在执行行动操作时才会发生。这种特性可以优化job的执行效率，使得Spark能够更好地利用集群的资源。

### 6. 性能优化

Spark中的RDD通过各种特性来提升性能，包括：
- 数据的分区（Partitioning）：合理的分区可以提高并行度，减少任务之间的Shuffle。
- 缓存（Caching）：经常使用的RDD可以被缓存到内存中，以提速后续计算。

### 总结

RDD是Apache Spark的基础数据结构，提供了强大的分布式计算能力和容错特性。它的设计使得开发者可以用简洁的操作来处理大规模数据，从而提高了数据处理的效率和可扩展性。在使用Spark进行大数据处理时，了解RDD的原理和操作是非常重要的。

RDD模型可以封装数据的处理逻辑，但是这个逻辑不能太复杂，类似于字符串。

RDD的功能类似于字符串的功能，需要通过大量的RDD对象组合在一起实现复杂的功能。

RDD和字符串的区别：

1. 字符串的功能是单点操作，功能一旦调用，就会马上执行 
2. RDD的功能是分布式操作，功能调用但不会马上执行








# RDD 编程 

## 2.1RDD的创建

在Spark中创建RDD的创建方式可以分为三种：从集合中创建RDD、从外部存储创
建RDD、从其他RDD创建。 

## 2.1.1IDEA环境准备

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory_Partition {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark01_Env");
        conf.setMaster("local[*]");
        conf.set("spark.default.parallelism", "4");

        // 2. 创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu");
        JavaRDD<String> rdd = jsc.parallelize(names, 3);
        rdd.saveAsTextFile("output");
        // 4. 关闭sc
        jsc.stop();
    }
}

```

parallelize方法可以传递2个参数的

第一个参数表示对接的数据源集合

第二个参数表示切片（分区）的数量

可以不需要指定，spark会采用默认值进行分区（切片）

numSlices =scheduler.conf.getInt("spark.default.parallelism",totalcores) 

从配置对象中获取配置参数：spark.default.parallelism（默认并行度）

如果配置参数不存在，那么默认取值为totalCores（当前环境总的虚拟核数），


Kafka可以将数据进行切片（减小规模），也称之为分区，这个分区操作是底层完成的。

Local环境中，分区数量和环境核数相关，但是一般不推荐分区数量需要手动设定

Spark在读取集合数据时，分区设定存在3种不同场合

1. 优先使用方法参数
2. 使用配置参数：spark.default.parallelism 
3. 采用环境默认值








TOD0文件数据源分区设定也存在多个位置

1.textFile可以传递第二个参数：minPartitions（最小分区数）

参数可以不需要传递的，那么Spark会采用默认值

`minPartitions =math.min(defaultParallelism，2)`

2.使用配置参数：`spark.default.parallelism=>1=>math.min（参数，2）`

3.采用环境默认总核值=>math.min（总核数，2）

Spark框架基于MR开发的。

Spark框架文件的操作没有自已的实现的。采用MR库（Hadoop）来实现当前读取文件的切片数量不是由Spark决定的，而是由Hadoop决定

Hadoop切片规则：

```
totalsize:7byte
goalsize:totalsize/min-part-num=>7/3=2byte
part-num:totalsize/goalsize=>7/2=>3....1
```







```
finalList<Integer>names =Arrays.asList（1,2,3,4,5,6);

/*
【1】【2,3】【4】【5，6】

len=6,partnum=4

(0 until 4) => [0，1,2,3]

θ=>((i*length)/numSlices，(((i+1）*length)/numSlices)) 
=>（(0*6）/4，（((0+1）*6）/4))
（0，1）=>1

1 =>（(i*length）/numSlices，（((i+1）*length）/numSlices))
=> =>
=> 3=>
=>
（(1*6）/4，（((2)*6)/4))(1，3）=>2
（(i*length）/numSlices，（((i+1）*length）/numSlices))
/4，（((3）*6）/4))
*6)((2
4）=>1(3,
*length）/numSlices，（((i+1）*length）/numSlices))((i
（((4）*6）/4))
((3
6)
/4,
*
(4, 6) => 2
=>
```

Spark进行分区处理时，需要对每个分区的数据尽快能地平均分配

totalsize=7
goalsize=totalsize/minpartnum=7/2=3
partnum=totalsize/goalsize =7/3=2...1=>2+1=3 

Spark不支持文件操作的。文件操作都是由Hadoop完成的

Hadoop进行文件切片数量的计算和文件数据存储计算规则不一样。 

1.分区数量计算的时候，考虑的是尽可能的平均：按字节来计算 

2.分区数据的存储是考虑业务数据的完整性：按照行来读取

读取数据时，还需要考虑数据偏移量，偏移量从0开始的，相同偏移量对应的数据不能重复读取。




```
1．分区数量
goalsize =14/4=>3
= 14/3=4...2=>4+1=5
partnum 2．分区数据
[0,3][3，6][6，9][9，12][12, 14]
110 =>
=> => => => =>
0123
4567
22
=>
【11】【22】【33】【44】 4
891011
33@@
=>
44
1213
```


注意避免数据倾斜问题，以行为单位存储业务


# RDD方法

注意方法名、IN、OUT

两大类

RDD类似于数据管道，可以流转数据，但是不能保存数据

RDD有很多的方法可以将数据向下流转，称之为转换

RDD有很多的方法可以控制数据流转，称之为行动

算子（operate）：操作、方法


## 处理数据方法的分类

两大类：单值、键值

Scala中()构成元组


Scala语言中可以将无关的数据封装在一起，形成一个整体，称之为元素的组合，简称为【元组】 

如果想要访问元组中的数据，必须采用顺序号

如果元组中的数据只有2个的场合，称之为对偶元组，也称之为键值对

马丁将Scala融合到Java，形成的JDK1.8中的Tuple类

```scala
var kV=（0，1) 
var kv1 =("zhangsan", 20,1001) 
```


```java
import scala.Tuple1;  
import scala.Tuple2;



Tuple2<String, Integer> tuple = new Tuple2<>("zhangsan", 18);  
System.out.println(tuple._1 + " " + tuple._2;  
  
Tuple1<String> tuple1 = new Tuple1<>("zhangsan");  
System.out.println(tuple1._1());
```

Java中元组的最大数据容量为22






# Spark on YARN 部署搭建详细图文教程


https://blog.csdn.net/weixin_46560589/article/details/132898417











在您提供的代码中，最主要的问题出现在 `rdd.map` 调用中的 `Function` 接口的实现上。`Function` 接口是用于定义一个返回值的函数，但在 Spark 中的 `map` 操作中采用的是 `Function<T, R>` 接口类型。因此，您只需要实现 `apply` 方法，而没有必要实现 `call` 方法。 

以下是您代码中需要修改的地方：

### 1. **Function 接口的用法**
在 Spark Java API 中 `Function` 接口只有一个方法 `apply`，并且 `apply` 方法参数与返回类型匹配。这是不需要实现 `call` 方法的。

### 2. **选择合适的类型**
为了使 `apply` 方法返回 `Integer` 而不是 `Object`，你应该将 `mapRdd` 的类型定义为 `JavaRDD<Integer>`，而不是 `JavaRDD<Object>`。

### 3. **修正 `map` 的 lambda 表达式使用**
可以考虑使用 lambda 表达式来简化代码。

修正后的代码如下：

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_Operate_Transform_Map {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);

        // map操作，返回类型应为 JavaRDD<Integer>
        JavaRDD<Integer> mapRdd = rdd.map(integer -> integer * 2);
        
        // collect() 返回的是 List 类型，所以后续不能直接调用 foreach，要使用方法调用
        mapRdd.collect().forEach(System.out::println);

        System.out.println("---------------------------");
        System.out.println(nums);

        // 4. 关闭sc
        sc.stop();
    }
}
```





```
ToDo如果Java中接口采用注解oFunctionaLInterface声明，那么接口的使用就可以采用JDK提供的函数式编程的语法实现（lambda表达式）
1.return可以省略：map方法就需要返回值，所以不需要return
2.分号可以省略：可以采用换行的方式表示代码逻辑 
3.大括号可以省略：如果逻辑代码只有一行
4.小括号可以省略：参数列表中的参数只有一个
5.参数和箭头可以省略：参数在逻辑中只使用了一次（需要有对象来实现功能）
```

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
import java.util.function.Function;  
  
public class Spark02_Operate_Transform_Map {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[*]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
        // map操作  
        //JavaRDD<Integer> mapRdd = rdd.map(integer -> integer * 2);  
        JavaRDD<Integer> mapRdd = rdd.map(NumberTest::doubleNumber);  
        mapRdd.collect().forEach(System.out::println);  
  
        System.out.println("---------------------------");  
        System.out.println(nums);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
class NumberTest {  
    public static int doubleNumber(int num) {  
        return num * 2;  
    }  
}
```

函数式编程写法

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
  
public class Spark02_Operate_Transform_Map {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[*]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2)  
                .map(NumberTest::doubleNumber)  
                .collect()  
                .forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
class NumberTest {  
    public static int doubleNumber(int num) {  
        return num * 2;  
    }  
}
```

Map转换方法，将传递的数据转换为其他数据返回

默认情况下，新创建的RDD的分区数量和之前旧的RDD的数量保持一致

数据流转过程中，数据所在分区会如何变化？

默认情况下，数据流转，所在的分区编号不变。分组处理后会发生变化。

Spark处理不同分区数据时：分区内有序，分区间无序

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
  
public class Spark02_Operate_Transform_Map2 {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelize(Arrays.asList(1, 2, 3, 4), 2)  
                .map(NumberTest2::doubleNumber)  
                .collect();  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
class NumberTest2 {  
    public static int doubleNumber(int num) {  
        System.out.println("doubleNumber: @" + num);  
        return num * 2;  
    }  
}
```

```
doubleNumber: @3
doubleNumber: @1
doubleNumber: @4
doubleNumber: @2
```


一个RDD中处理完所有数据后，下一个RDD才会继续处理数据。

原因：RDD不保存数据

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Map2 {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        JavaRDD<Integer> mapRdd1 = rdd.map(num -> {  
            System.out.println("map1: @" + num);  
            return num;  
        });  
        JavaRDD<Integer> mapRdd2 = mapRdd1.map(num -> {  
            System.out.println("map1: #####" + num);  
            return num;  
        });  
        mapRdd2.foreach(num -> System.out.println("foreach: " + num));  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
map1: @3
map1: @1
map1: #####3
map1: #####1
foreach: 1
foreach: 3
map1: @2
map1: @4
map1: #####4
map1: #####2
foreach: 4
foreach: 2
```









## Filter

```java
package com.zzw.bigdata.spark.rdd.operate;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Filter {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[*]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        rdd.filter(num -> num % 2 == 0).collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```


## FlatMap

将整体数据拆分成个体来使用的操作称为扁平化操作


```java
package com.zzw.bigdata.spark.rdd.operate;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import org.apache.spark.api.java.function.FlatMapFunction;  
  
import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.Iterator;  
import java.util.List;  
  
public class Spark02_Operate_Transform_FlatMap {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<List<Integer>> data = Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));  
        JavaRDD<List<Integer>> rdd = sc.parallelize(data, 2);  
        //rdd.flatMap(list -> list.iterator()).collect().forEach(System.out::println);  
        JavaRDD<Integer> flatMapRdd = rdd.flatMap(new FlatMapFunction<List<Integer>, Integer>(){  
  
            public Iterator<Integer> call(List<Integer> list) throws Exception {  
                List<Integer> nums = new ArrayList<>();  
                list.forEach(num -> nums.add(num * 2));  
                return nums.iterator();  
            }  
        });  
        flatMapRdd.collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
2
4
6
8
10
12
```


map方法只负责转换数据（A->B[B1，B2，B3])，不能将数据拆分后独立使用

flatMap方法可以将数据拆分后独立使用（A->B1，B2，B3）


```java
package com.zzw.bigdata.spark.rdd.operate;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import org.apache.spark.api.java.function.FlatMapFunction;  
  
import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.Iterator;  
import java.util.List;  
  
public class Spark02_Operate_Transform_FlatMap1 {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
  
        JavaRDD<String> rdd = sc.textFile("spark/src/main/resources/data/test.txt");  
        JavaRDD<String> flatMapRdd = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());  
        flatMapRdd.collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
hadoop
flume
spark
hive
mysql
navicat
oracle
bazaar
git
```


## groupby方法


```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Groupby {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        rdd.groupBy(num -> num % 5).collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

输出结果如下所示：

```
(4,[4, 9])
(0,[5, 10])
(2,[2, 7])
(1,[1, 6])
(3,[3, 8])
```


如何解决数据倾斜？

重新分组？



默认情况下，数据处理后，所在的分区不会发生变化，但是groupBy方法例外 

Spark在数据处理中，要求同一个组的数据必须在同一个分区中

所以分组操作会将**数据分区中的数据**打乱重新组合，在Spark中这个操作被称为Shuffle


一个分区可以存放多个组

Spark要求所有的数据必须分组后才能继续执行后续操作

RDD对象不能保存数据

当前groupBy操作会将数据保存到磁盘文件中，保证数据全部分组后执行后续操作

Shuffle操作一定会落盘，但可能会导致资源浪费

Spark中含有Shuffle操作的方法都有改变分区的能力

RDD的分区和Task之间有关系。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20241231155313.png)


Shuffle会将完整的计算流程一分为二，其中一部分任务会写磁盘，另外一部分的任务会读磁盘

写磁盘的操作不完成，不允许读磁盘

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Groupby {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        rdd.groupBy(num -> num % 5).collect().forEach(System.out::println);  
  
        System.out.println("执行结束");  
  
  
        //监控页面：http://localhost:4040  
        Thread.sleep(300000);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```


## distinct

distinct 分布式去重，采用了分组+Shuffle的处理方式

hashSet 单点去重

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Distinct {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 1,1,12,2,2);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 3);  
        rdd.distinct().collect().forEach(System.out::println);  
  
        System.out.println("执行结束");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```



## sortBy

传递三个参数

第一个参数表示排序规则——Spark会为每一个数据增加一个标记，然后按照标记对数据进行排序

第二个参数表示排序的方式

第三个参数表示分区数量


```java
package com.zzw.bigdata.spark.rdd.operate;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_SortBy {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 4,9,12,2,2);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 3);  
        rdd.distinct().sortBy(num -> num, true, 2).collect().forEach(System.out::println);  
  
        System.out.println("执行结束");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```java
package com.zzw.bigdata.spark.rdd.operate;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_SortBy {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 4,9,12,2,2);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 3);  
        rdd.distinct().sortBy(num -> "" + num, true, 2).collect().forEach(System.out::println);  
  
        System.out.println("执行结束");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

输出结果如下所示：

```
1
12
2
4
9
执行结束
```

字符串按照字典顺序排列


# KV类型


```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        //KV类型一般表示二元组，如(key, value)，SparkRDD中对数据整体的处理称为单值操作，对数据中的每个元素进行操作称为KV操作。  
        Tuple2<String, Integer> t1 = new Tuple2<String, Integer>("zhangsan", 18);  
        Tuple2<String, Integer> t2 = new Tuple2<String, Integer>("lisi", 20);  
        Tuple2<String, Integer> t3 = new Tuple2<String, Integer>("wangwu", 22);  
        Tuple2<String, Integer> t4 = new Tuple2<String, Integer>("zhaoliu", 25);  
  
        List<Tuple2<String, Integer>> list = Arrays.asList(t1, t2, t3, t4);  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(list);  
  
        //单值处理  
        rdd.map(t -> new Tuple2<>(t._1, t._2 * 2)).collect().forEach(System.out::println);  
  
        // 3.1 KV操作  
        // 3.1.1 mapValues  
        JavaPairRDD<String, Integer> pairRdd = sc.parallelizePairs(list);  
        pairRdd.mapValues(value -> value * 2).collect().forEach(System.out::println);  
  
        System.out.println("执行结束");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(zhangsan,36)
(lisi,40)
(wangwu,44)
(zhaoliu,50)
(zhangsan,36)
(lisi,40)
(wangwu,44)
(zhaoliu,50)
执行结束
```

## 单值类型转换为KV类型

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV_1 {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);  
        JavaRDD<Integer> rdd = sc.parallelize(nums);  
  
        rdd.mapToPair(num -> new Tuple2<>(num, num * 2)).mapValues(num -> num * 2).collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(1,4)
(2,8)
(3,12)
(4,16)
(5,20)
```

## groupBy和KV类型


```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV_GroupBy {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
        JavaPairRDD<Integer, Iterable<Integer>> groupRDD = rdd.groupBy(num -> num % 2);  
        groupRDD.mapValues(iter -> {  
            int sum = 0;  
            for (Integer num : iter) {  
                sum += num;  
            }  
            return sum;  
        }).collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(0,6)
(1,4)
```


## WordCount

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV_WordCount {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
  
        JavaRDD<String> lineRDD = sc.textFile("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\word.txt");  
        JavaRDD<String> wordRDD = lineRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());  
        JavaPairRDD<String, Iterable<String>> wordGroupRDD = wordRDD.groupBy(word -> word);  
        JavaPairRDD<String, Integer> wordCountRDD = wordGroupRDD.mapValues(iter -> {  
            int count = 0;  
            for (String s : iter) {  
                count++;  
            }  
            return count;  
        });  
        wordCountRDD.collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(Flink,1)
(Zookeeper,1)
(Kafka,3)
(Cassandra,1)
(Spark,2)
(Flume,2)
(Redis,1)
(HBase,1)
(Hadoop,2)
```

## GroupByKey（按Key对Value进行分组）

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV_GroupByKey {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3), new Tuple2<>("c", 4)));  
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = rdd.groupBy(t -> t._1);  
        groupRDD.collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

输出结果如下所示：

```
(b,[(b,2)])
(a,[(a,1), (a,3)])
(c,[(c,4)])
```

简单写法如下所示：

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV_GroupByKey {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3), new Tuple2<>("c", 4))).groupByKey().collect().forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

输出结果如下所示：

```
(b,[2])
(a,[1, 3])
(c,[4])
```

加入求和逻辑：

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_KV_GroupByKey {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3), new Tuple2<>("c", 4)))  
                .groupByKey().mapValues(iter -> {  
                    int sum = 0;  
                    for (int i : iter) {  
                        sum += i;  
                    }  
                    return sum;  
                })  
                .collect()  
                .forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(b,2)
(a,4)
(c,4)
```

## ReduceByKey（按照key对value两两聚合）

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
  
public class Spark02_Operate_Transform_KV_ReduceByKey {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelize(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3), new Tuple2<>("c", 4)))  
                .mapToPair(t -> t).reduceByKey(Integer::sum)  
                .collect()  
                .forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(b,2)
(a,4)
(c,4)
```

## sortByKey（按照key排序）

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
  
public class Spark02_Operate_Transform_KV_SortByKey {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelize(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3), new Tuple2<>("c", 4)))  
                .mapToPair(t -> t).sortByKey(false)  
                .collect()  
                .forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(c,4)
(b,2)
(a,1)
(a,3)
```

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
  
public class Spark02_Operate_Transform_KV_SortByKey {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        sc.parallelize(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("a", 3), new Tuple2<>("a", 4)))  
                .mapToPair(t -> new Tuple2<>(t._2, t)).sortByKey(false).map(t -> t._2)  
                .collect()  
                .forEach(System.out::println);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(a,4)
(a,3)
(a,2)
(a,1)
```


## 优化shuffle性能

1. 花钱
2. 增加磁盘读写缓冲区
3. 如果不影响最终结果的话，那么磁盘读写的数据越少，性能越高。（reduceByKey可以在Shuffle之前就预先聚合Combine，极大地提高性能）


## 缩减分区（coalesce）

该方法没有Shuffle功能，所以数据不会被打乱然后重新组合。

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Partition {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        JavaRDD<Integer> filterRDD = rdd.filter(num -> num % 2 == 0);  
  
        JavaRDD<Integer> coalesceRDD = filterRDD.coalesce(1);  
        coalesceRDD.saveAsTextFile("./output");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

该方法默认无法扩大分区，只能缩减分区。

设定shuffle参数为true，可以实现扩大分区

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Partition {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        JavaRDD<Integer> filterRDD = rdd.filter(num -> num % 2 == 0);  
  
        JavaRDD<Integer> coalesceRDD = filterRDD.coalesce(3, true);  
        coalesceRDD.saveAsTextFile("./output");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

## repartition（扩大分区）

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Transform_Partition {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
        JavaRDD<Integer> filterRDD = rdd.filter(num -> num % 2 == 0);  
  
        filterRDD.repartition(3).saveAsTextFile("./output");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```


# Action算子

## 如何区分转换算子和行动算子？

不能用是否启动Job来区分

转换算子目的：将旧的RDD转换成新的RDD来组合多个RDD的功能，格式：RDD（In）->RDD(Out)

行动算子目的：返回具体执行结果

sortBy()方法会sample，然后提前执行一下collect()方法，故而有Job进行


## collect()方法

将Executor端执行的结果按照分区的顺序拉取（采集）到Driver端，将结果组合成集合对象



特殊情况：文件内容存在于HDFS

collect()方法可能会将多个Executor的数据大量拉取到Driver端，导致内存溢出。生产环境下慎用

## 其他方法

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Operate_Action {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
        List<Integer> collect = rdd.collect();  
        rdd.count();  
        Integer first = rdd.first();  
        List<Integer> take = rdd.take(3);  
        System.out.println("collect: " + collect);  
        System.out.println("count: " + rdd.count());  
        System.out.println("first: " + first);  
        System.out.println("take: " + take);  
  
        System.out.println("执行结束");  
  
  
        //监控页面：http://localhost:4040  
        //Thread.sleep(300000);  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
collect: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
first: 1
take: [1, 2, 3]
执行结束
```

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Spark02_Operate_Action {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);

        JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair((x) -> new Tuple2<>(x, x * x));
        Map<Integer, Long> integerLongMap = pairRDD.countByKey();
        System.out.println(integerLongMap);

        // 4. 关闭sc
        sc.stop();

    }
}
```

```
{4=1, 2=1, 1=1, 3=1}
```

### `saveAsTextFile`VS`saveAsObjectFile`

后者用于保存对象比如User

### collect()和foreach()联动


foreach()在Executor端输出，分布式无序输出

collect()和foreach()联动之后在Driver端单点有序输出

foreachPartition执行效率高，但是受到内存大小限制

```java
SparkConf conf = new SparkConf();  
conf.setMaster("local[2]");  
conf.setAppName("sparkCore");  
  
// 2. 创建sparkContext  
JavaSparkContext sc = new JavaSparkContext(conf);  
  
// 3. 编写代码  
List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
rdd.collect().forEach(System.out::println);  
System.out.println("--------------------------");  
rdd.foreach(System.out::println);  
System.out.println("------------------");  
rdd.foreachPartition(  
        list -> {  
            System.out.println(list);  
        }  
);  
  
// 4. 关闭sc  
sc.stop();
```

执行会报错`Exception in thread "main" org.apache.spark.SparkException: Task not serializable`

错误原因：foreach里面使用了lambda表达式

正确代码如下所示：

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
import java.util.List;  
import java.util.Map;  
  
public class Spark02_Operate_Action {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
        rdd.collect().forEach(System.out::println);  
        System.out.println("--------------------------");  
        rdd.foreach(num -> System.out.println(num));  
        System.out.println("------------------");  
        rdd.foreachPartition(  
                list -> {  
                    System.out.println(list);  
                }  
        );  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

输出结果如下所示：

```
1
2
3
4
--------------------------
1
3
4
2
------------------
IteratorWrapper(<iterator>)
IteratorWrapper(<iterator>)
```


## 序列化

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.io.Serializable;  
import java.util.Arrays;  
import java.util.List;  
import java.util.Map;  
  
public class Spark02_Operate_Action {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
        JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
        Student student = new Student();  
  
        rdd.foreach(num -> System.out.println(student.name + " " + num));  
        System.out.println("------------------");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
class Student implements Serializable {  
    public String name = "baozi";  
}
```

在Executor端循环遍历的时候使用到了Driver端对象

运行过程中就需要将Driver端的对象通过网络传递到Executor端，否则无法使用

这里传输的对象必须要实现可序列化接口，否则无法传递

RDD算子的逻辑代码是在Executor端执行的，其他的代码都在Driver端执行

下面两份代码都是正确的：

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.io.Serializable;  
import java.util.Arrays;  
import java.util.List;  
import java.util.Map;  
  
public class Spark02_Operate_Action {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<String> list = Arrays.asList("Hive", "Hadoop", "Spark", "Flink");  
        JavaRDD<String> rdd = sc.parallelize(list, 2);  
  
        new Search("H").match(rdd);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
class Search implements Serializable {  
    private String query;  
    public Search(String query){  
        this.query = query;  
    }  
    public void match(JavaRDD<String> rdd){  
        rdd.filter(line -> line.startsWith(query)).collect().forEach(System.out::println);  
    }  
}
```

```
Hive
Hadoop
```

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.io.Serializable;  
import java.util.Arrays;  
import java.util.List;  
import java.util.Map;  
  
public class Spark02_Operate_Action {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<String> list = Arrays.asList("Hive", "Hadoop", "Spark", "Flink");  
        JavaRDD<String> rdd = sc.parallelize(list, 2);  
  
        new Search("H").match(rdd);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
class Search{  
    private String query;  
    public Search(String query){  
        this.query = query;  
    }  
    public void match(JavaRDD<String> rdd){  
        String keyword = this.query;  
        rdd.filter(line -> line.startsWith(keyword)).collect().forEach(System.out::println);  
    }  
}
```

String默认实现了Serializable接口

```java
SparkConf conf = new SparkConf();  
conf.setMaster("local[2]");  
conf.setAppName("sparkCore");  
  
// 2. 创建sparkContext  
JavaSparkContext sc = new JavaSparkContext(conf);  
  
// 3. 编写代码  
List<Integer> nums = Arrays.asList(1, 2, 3, 4);  
JavaRDD<Integer> rdd = sc.parallelize(nums, 2);  
  
rdd.collect().forEach(System.out::println);  
System.out.println("--------------------------");  
rdd.foreach(System.out::println);  
System.out.println("------------------");  
rdd.foreachPartition(  
        list -> {  
            System.out.println(list);  
        }  
);  
  
// 4. 关闭sc  
sc.stop();
```

执行会报错`Exception in thread "main" org.apache.spark.SparkException: Task not serializable`

错误原因：foreach中System.out是一个Driver端的对象

```java
PrintStream out = System.out;  
rdd.foreachPartition(  
        out::println  
);
```

Java1.8的函数式编程是通过对象模拟出来的，不是真正的函数式编程。







