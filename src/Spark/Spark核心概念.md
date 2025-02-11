# Spark核心概念

<!-- toc -->



# 序列化

- Kryo——速度快，序列化后体积小；缺点是跨语言支持较复杂
- Protostuff——速度快，基于protobuf；缺点是需静态编译
- Protostuff-Runtime,无需静态编译，但序列化前需预先传入schema;缺点是不支持无默认构造函数的类，反序列化时需用户自己初始化序列化后的对象，其只负责将该对象进行赋值
- Java——使用方便，可序列化所有类；缺点是速度慢，占空间

具体的对比可以参考这个基线图：

Results - JVM Serializer Benchmarks

 效率对比直观图：

An Introduction and Comparison of Several Common Java Serialization Frameworks - Alibaba Cloud Community

首选序列化：Kryo、Protostuff

# Kryo序列化框架


```java
package com.zzw;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.BeanSerializer;

import java.io.*;

public class KryoTest {
    public static void main(String[] args) {
        User user = new User("Tom", 20);
        //javaSerial(user, "D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.dat");
        //kryoSerial(user, "D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user1.kryo");
        User user1 = kryoDeSerial(User.class, "D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user1.kryo");
        System.out.println(user1.getName() + " " + user1.getAge());

    }
    public static void javaSerial(Serializable s, String path) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
            oos.writeObject(s);
            oos.flush();
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static <T> T kryoDeSerial(Class<T> c, String path) {
        try {
            Kryo kryo = new Kryo();
            kryo.register(c, new BeanSerializer(kryo, c));
            Input input = new Input(new BufferedInputStream(new FileInputStream(path)));
            T obj = kryo.readObject(input, c);
            input.close();
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static void kryoSerial(Serializable s, String path) {
        try {
            Kryo kryo = new Kryo();
            kryo.register(s.getClass(), new BeanSerializer(kryo, s.getClass()));
            Output output = new Output(new BufferedOutputStream(new FileOutputStream(path)));
            kryo.writeObject(output, s);
            output.flush();
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
class User implements Serializable {
    private String name;
    private int age;

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }
    public User() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
```

## 使用Kyro

```java
package com.atguigu.serializable;

import com.atguigu.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class Test02_Kryo {
    public static void main(String[] args) throws ClassNotFoundException {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore")
                // 替换默认的序列化机制
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // 注册需要使用kryo序列化的自定义类
                .registerKryoClasses(new Class[]{Class.forName("com.atguigu.bean.User")});

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        User zhangsan = new User("zhangsan", 13);
        User lisi = new User("lisi", 13);

        JavaRDD<User> userJavaRDD = sc.parallelize(Arrays.asList(zhangsan, lisi), 2);

        JavaRDD<User> mapRDD = userJavaRDD.map(new Function<User, User>() {
            @Override
            public User call(User v1) throws Exception {
                return new User(v1.getName(), v1.getAge() + 1);
            }
        });

        mapRDD. collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}

```


# 依赖

RDD依赖：Spark中相邻的两个RDD之间存在的依赖关系

连续的依赖关系被称为血缘关系

Spark中的每一个RDD都保存了依赖关系和血缘关系，方便出问题时可以溯源或重试

```java
package com.zzw.bigdata.spark.rdd.dep;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark01_WordCount {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码

        JavaRDD<String> lineRDD = sc.textFile("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\word.txt");
        System.out.println(lineRDD.toDebugString());
        System.out.println("*************");
        JavaRDD<String> wordRDD = lineRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println(wordRDD.toDebugString());
        System.out.println("*************");
        JavaPairRDD<String, Iterable<String>> wordGroupRDD = wordRDD.groupBy(word -> word);
        System.out.println(wordGroupRDD.toDebugString());
        System.out.println("*************");
        JavaPairRDD<String, Integer> wordCountRDD = wordGroupRDD.mapValues(iter -> {
            int count = 0;
            for (String s : iter) {
                count++;
            }
            return count;
        });
        System.out.println(wordCountRDD.toDebugString());
        System.out.println("*************");
        wordCountRDD.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();

    }
}
```

输出结果如下所示：

```
(2) D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt MapPartitionsRDD[1] at textFile at Spark01_WordCount.java:22 []
 |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt HadoopRDD[0] at textFile at Spark01_WordCount.java:22 []
*************
(2) MapPartitionsRDD[2] at flatMap at Spark01_WordCount.java:25 []
 |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt MapPartitionsRDD[1] at textFile at Spark01_WordCount.java:22 []
 |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt HadoopRDD[0] at textFile at Spark01_WordCount.java:22 []
*************
(2) MapPartitionsRDD[5] at groupBy at Spark01_WordCount.java:28 []
 |  ShuffledRDD[4] at groupBy at Spark01_WordCount.java:28 []
 +-(2) MapPartitionsRDD[3] at groupBy at Spark01_WordCount.java:28 []
    |  MapPartitionsRDD[2] at flatMap at Spark01_WordCount.java:25 []
    |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt MapPartitionsRDD[1] at textFile at Spark01_WordCount.java:22 []
    |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt HadoopRDD[0] at textFile at Spark01_WordCount.java:22 []
*************
(2) MapPartitionsRDD[6] at mapValues at Spark01_WordCount.java:31 []
 |  MapPartitionsRDD[5] at groupBy at Spark01_WordCount.java:28 []
 |  ShuffledRDD[4] at groupBy at Spark01_WordCount.java:28 []
 +-(2) MapPartitionsRDD[3] at groupBy at Spark01_WordCount.java:28 []
    |  MapPartitionsRDD[2] at flatMap at Spark01_WordCount.java:25 []
    |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt MapPartitionsRDD[1] at textFile at Spark01_WordCount.java:22 []
    |  D:\Documents\items\BigData\BigDataCode\spark\src\main\resources\data\word.txt HadoopRDD[0] at textFile at Spark01_WordCount.java:22 []
*************
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

```java
List<Tuple2<String, Integer>> list = Arrays.asList(t1, t2, t3, t4);  
JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(list);  
  
System.out.println(rdd.rdd().dependencies());
```

## 宽依赖和窄依赖

在Driver端准备计算逻辑（RDD的关系）->由Spark对关系进行判断决定人物的数量和关系->计算逻辑是在Executor端执行

RDD中的依赖关系本质上并不是RDD对象的关系，而是RDD对象中分区数据的关系

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250105145756.png)

OneToOneDependency 一对一（窄依赖）：计算中上游RDD的一个分区数据被下游RDD的一个分区所独享

不是窄依赖的就是宽依赖

ShuffleDependency （宽依赖）：计算中上游RDD的一个分区数据被下游RDD的多个分区所共享

宽依赖会将分区数据打乱重新组合，所以底层实现存在Shuffle操作


## 作业、阶段和任务的关系

作业（Job）：行动算子执行时，会触发作业的执行（Active Job）

阶段（Stage）：一个Job中RDD的计算流程，默认就一个完整的阶段，但是如果计算流程中存在shuffle，那么流程就会分为二

分开的每一段就称之为Stage（阶段），前一个阶段不执行完，后一个阶段不允许执行

阶段的数量=1+shuffle依赖的数量

任务（Task）：每个Executor执行的计算单元

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250105154242.png)

`0 until numPartitions` 左闭右开，每个分区对应一个`new Task`

任务的数量其实就是每个阶段最后一个RDD分区的数量之和


![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250105155313.png)


## 任务数量（分区数量）

怎么设置任务数量？简单来说，任务数量最好等于资源核数

但这样容易出问题——计算资源空置

移动数据不如移动计算

一般推荐分区数量为资源核数的2~3倍

## 持久化和序列化


持久化：长时间保存对象

序列化：内存中对象=>byte序列（byte数组）


### Cache（重复使用RDD时使用）

代码流程设计存在问题：数据重复、计算重复

改变流向没用，因为数据不能倒流转

RDD不保存数据，如果重复使用同一个RDD，那么数据就会从头执行，导致数据重复、计算重复

解决措施：cache

对应代码为`mapRDD.cache();`

未使用cache时

```java
package com.zzw.bigdata.spark.rdd.dep;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark02_Dep {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Tuple2<String, Integer>> nums = new ArrayList<>();  
        nums.add(new Tuple2<>("a", 1));  
        nums.add(new Tuple2<>("a", 2));  
        nums.add(new Tuple2<>("a", 3));  
        nums.add(new Tuple2<>("a", 4));  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);  
  
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {  
            System.out.println("*************");  
            return kv;  
        });  
  
        //mapRDD.cache();  
  
        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(Integer::sum);  
  
        wordCountRDD.collect();  
        System.out.println("计算1结束");  
        System.out.println("############################");  
  
        JavaPairRDD<String, Iterable<Integer>> groupRDD = mapRDD.groupByKey();  
        groupRDD.collect();  
        System.out.println("计算2结束");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
*************
*************
*************
*************
计算1结束
############################
*************
*************
*************
*************
计算2结束
```

使用cache后

```java
package com.zzw.bigdata.spark.rdd.dep;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Spark02_Dep {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<Tuple2<String, Integer>> nums = new ArrayList<>();
        nums.add(new Tuple2<>("a", 1));
        nums.add(new Tuple2<>("a", 2));
        nums.add(new Tuple2<>("a", 3));
        nums.add(new Tuple2<>("a", 4));
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);

        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {
            System.out.println("*************");
            return kv;
        });

        mapRDD.cache();

        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(Integer::sum);

        wordCountRDD.collect();
        System.out.println("计算1结束");
        System.out.println("############################");

        JavaPairRDD<String, Iterable<Integer>> groupRDD = mapRDD.groupByKey();
        groupRDD.collect();
        System.out.println("计算2结束");

        // 4. 关闭sc
        sc.stop();

    }
}
```

```
*************
*************
*************
*************
计算1结束
############################
计算2结束
```

cache其实是把数据保存到内存中，所以会受到内存大小的影响
### Persist（File，落盘）

使用`mapRDD.persist()`，传参`StorageLevel`

例如`mapRDD.persist(StorageLevel.MEMORY_ONLY());`和`mapRDD.cache();`等同

使用`mapRDD.unpersist();`释放缓存

```java
@DeveloperApi  
public static StorageLevel fromString(final String s) {  
    return StorageLevel$.MODULE$.fromString(s);  
}  
  
public static StorageLevel OFF_HEAP() {  
    return StorageLevel$.MODULE$.OFF_HEAP();  
}  
  
public static StorageLevel MEMORY_AND_DISK_SER_2() {  
    return StorageLevel$.MODULE$.MEMORY_AND_DISK_SER_2();  
}  
  
public static StorageLevel MEMORY_AND_DISK_SER() {  
    return StorageLevel$.MODULE$.MEMORY_AND_DISK_SER();  
}  
  
public static StorageLevel MEMORY_AND_DISK_2() {  
    return StorageLevel$.MODULE$.MEMORY_AND_DISK_2();  
}  
  
public static StorageLevel MEMORY_AND_DISK() {  
    return StorageLevel$.MODULE$.MEMORY_AND_DISK();  
}  
  
public static StorageLevel MEMORY_ONLY_SER_2() {  
    return StorageLevel$.MODULE$.MEMORY_ONLY_SER_2();  
}  
  
public static StorageLevel MEMORY_ONLY_SER() {  
    return StorageLevel$.MODULE$.MEMORY_ONLY_SER();  
}  
  
public static StorageLevel MEMORY_ONLY_2() {  
    return StorageLevel$.MODULE$.MEMORY_ONLY_2();  
}  
  
public static StorageLevel MEMORY_ONLY() {  
    return StorageLevel$.MODULE$.MEMORY_ONLY();  
}  
  
public static StorageLevel DISK_ONLY_3() {  
    return StorageLevel$.MODULE$.DISK_ONLY_3();  
}  
  
public static StorageLevel DISK_ONLY_2() {  
    return StorageLevel$.MODULE$.DISK_ONLY_2();  
}  
  
public static StorageLevel DISK_ONLY() {  
    return StorageLevel$.MODULE$.DISK_ONLY();  
}  
  
public static StorageLevel NONE() {  
    return StorageLevel$.MODULE$.NONE();  
}
```

SER表示将数据序列化之后再保存，2表示备份份数
### checkpoint（多个进程共享数据）

Spark的持久化操作只对当前应用程序有效，其他应用程序无法访问

使用HDFS生成中间件（检查点）

执行`mapRDD.checkpoint();`

需要设定检查点目录，推荐使用HDFS共享文件系统，也可以使用本地文件路径

执行`sc.setCheckpointDir();`

```java
package com.zzw.bigdata.spark.rdd.dep;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import org.apache.spark.storage.StorageLevel;  
import scala.Tuple2;  
  
import java.util.ArrayList;  
import java.util.List;  
  
public class Spark03_Dep {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
        sc.setCheckpointDir("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\checkpoint");  
  
        // 3. 编写代码  
        List<Tuple2<String, Integer>> nums = new ArrayList<>();  
        nums.add(new Tuple2<>("a", 1));  
        nums.add(new Tuple2<>("a", 2));  
        nums.add(new Tuple2<>("a", 3));  
        nums.add(new Tuple2<>("a", 4));  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);  
  
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {  
            System.out.println("*************");  
            return kv;  
        });  
  
        //mapRDD.cache();  
        //mapRDD.persist(StorageLevel.MEMORY_ONLY());        mapRDD.checkpoint();  
  
        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(Integer::sum);  
  
        wordCountRDD.collect();  
        System.out.println("计算1结束");  
        System.out.println("############################");  
  
        JavaPairRDD<String, Iterable<Integer>> groupRDD = mapRDD.groupByKey();  
        groupRDD.collect();  
        System.out.println("计算2结束");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```
```
*************
*************
*************
*************
*************
*************
*************
*************
计算1结束
############################
计算2结束
```

检查点操作目的是希望RDD结果长时间的保存，所以需要保证数据的安全，会从头再跑一遍，性能比较低。

上面共有八行星号

为了提高效率，Spark推荐再检查点之前，执行cache方法，将数据缓存。

```java
mapRDD.cache();  
mapRDD.checkpoint();
```

```
*************
*************
*************
*************
计算1结束
############################
计算2结束
```
### shuffle算子的持久化

cache方法会在血缘关系中增加依赖关系

checkpoint方法切断（改变）血缘关系

```java
package com.zzw.bigdata.spark.rdd.dep;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import org.apache.spark.storage.StorageLevel;  
import scala.Tuple2;  
  
import java.util.ArrayList;  
import java.util.List;  
  
public class Spark03_Dep {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
        sc.setCheckpointDir("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\checkpoint");  
  
        // 3. 编写代码  
        List<Tuple2<String, Integer>> nums = new ArrayList<>();  
        nums.add(new Tuple2<>("a", 1));  
        nums.add(new Tuple2<>("a", 2));  
        nums.add(new Tuple2<>("a", 3));  
        nums.add(new Tuple2<>("a", 4));  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);  
  
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {  
            return kv;  
        });  
  
        mapRDD.cache();  
        //mapRDD.persist(StorageLevel.MEMORY_ONLY());  
        //mapRDD.checkpoint();  
        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(Integer::sum);  
        System.out.println(wordCountRDD.toDebugString());  
        System.out.println("############################");  
  
        wordCountRDD.collect();  
        System.out.println(wordCountRDD.toDebugString());  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

```
(2) ShuffledRDD[2] at reduceByKey at Spark03_Dep.java:40 []
 +-(2) MapPartitionsRDD[1] at mapToPair at Spark03_Dep.java:32 []
    |  ParallelCollectionRDD[0] at parallelize at Spark03_Dep.java:30 []
############################
(2) ShuffledRDD[2] at reduceByKey at Spark03_Dep.java:40 []
 +-(2) MapPartitionsRDD[1] at mapToPair at Spark03_Dep.java:32 []
    |      CachedPartitions: 2; MemorySize: 304.0 B; DiskSize: 0.0 B
    |  ParallelCollectionRDD[0] at parallelize at Spark03_Dep.java:30 []

```

```
(2) ShuffledRDD[2] at reduceByKey at Spark03_Dep.java:40 []
 +-(2) MapPartitionsRDD[1] at mapToPair at Spark03_Dep.java:32 []
    |  ParallelCollectionRDD[0] at parallelize at Spark03_Dep.java:30 []
############################
(2) ShuffledRDD[2] at reduceByKey at Spark03_Dep.java:40 []
 +-(2) MapPartitionsRDD[1] at mapToPair at Spark03_Dep.java:32 []
    |  ReliableCheckpointRDD[3] at collect at Spark03_Dep.java:44 []
```

cache可能丢失，而checkpoint共享

所有的shuffle操作性能是非常低，所以Spark为了提升shuffle算子的性能，每个shuffle算子都是自动含有缓存如果重复调用相同规则的shuffle算子，那么第二次shuffle算子不会有shuffle操作.


## 分区器

reduceBySum 先分区内求和，然后分区间求和

数据分区的规则

计算后数据所在的分区是通过Spark的内部计算（分区）完成，尽可能让数据均衡（散列）一些，但是不是平均分。

reduceByKey方法需要传递两个参数

1.第一个参数表示数据分区的规则，参数可以不用传递，使用时，会使用默认值（默认分区规则Hashpartitioner） 

2.第二个参数表示数据聚合的逻辑

HashPartitioner中有个方法getPartition

getPartition需要传递一个参数Key，然后方法需要返回一个值，表示分区编号，分区编号从0开始。

```scala
val rawMod = x % mod;
```

逻辑：`分区编号<=Key.hashCode%partNum（哈希取余）`


### 自定义分区器

自定义分区器流程：

1. 创建自定义类
2. 继承抽象类Partitioner 
3. 重写方法（2+2)Partitioner(2)+0bject(2) 
4. 构建对象，在算子中使用


```java
import org.apache.spark.Partitioner;  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.ArrayList;  
import java.util.List;  
  
public class Spark04_Partitioner {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Tuple2<String, Integer>> nums = new ArrayList<>();  
        nums.add(new Tuple2<>("nba", 1));  
        nums.add(new Tuple2<>("wnba", 2));  
        nums.add(new Tuple2<>("nba", 3));  
        nums.add(new Tuple2<>("cba", 4));  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);  
  
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {  
            return kv;  
        });  
  
        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(new MyPartitioner(), Integer::sum);  
  
        wordCountRDD.saveAsTextFile("output");  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
  
class MyPartitioner extends Partitioner {  
    @Override  
    public int numPartitions() {  
        return 3;  
    }  
  
    @Override  
    public int getPartition(Object key) {  
        if(key.equals("nba")){  
            return 0;  
        }else if(key.equals("wnba")){  
            return 1;  
        }else{  
            return 2;  
        }  
    }  
}
```

## 注意事项

没有重写分区器方法时，两次连续reduceByKey()中的shuffle不起作用

则Stage数量=1+1=2

Task数量= 2 + 2 = 4

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250107152628.png)
```java
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Spark04_Partitioner {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<Tuple2<String, Integer>> nums = new ArrayList<>();
        nums.add(new Tuple2<>("nba", 1));
        nums.add(new Tuple2<>("wnba", 2));
        nums.add(new Tuple2<>("nba", 3));
        nums.add(new Tuple2<>("cba", 4));
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);

        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {
            return kv;
        });

        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey(Integer::sum);
        JavaPairRDD<String, Integer> reduceRDD1 = reduceRDD.reduceByKey(Integer::sum);

        reduceRDD1.collect();

        System.out.println("执行结束");

        Thread.sleep(1000000L);

        // 4. 关闭sc
        sc.stop();

    }
}

class MyPartitioner extends Partitioner {
    private Integer numOfPartitioner;

    public MyPartitioner() {}
    public MyPartitioner(Integer numOfPartitioner) {
        this.numOfPartitioner = numOfPartitioner;
    }
    @Override
    public int numPartitions() {
        return this.numOfPartitioner;
    }

    @Override
    public int getPartition(Object key) {
        if(key.equals("nba")){
            return 0;
        }else if(key.equals("wnba")){
            return 1;
        }else{
            return 2;
        }
    }
}
```



![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250107152357.png)


重写之后Stage数量=2+1=3

Task数量=2 + 3  + 3=8

```java
package com.zzw.bigdata.spark.rdd.dep;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Spark04_Partitioner {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<Tuple2<String, Integer>> nums = new ArrayList<>();
        nums.add(new Tuple2<>("nba", 1));
        nums.add(new Tuple2<>("wnba", 2));
        nums.add(new Tuple2<>("nba", 3));
        nums.add(new Tuple2<>("cba", 4));
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);

        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {
            return kv;
        });

        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey(new MyPartitioner(3), Integer::sum);
        JavaPairRDD<String, Integer> reduceRDD1 = reduceRDD.reduceByKey(new MyPartitioner(3), Integer::sum);

        reduceRDD1.collect();

        System.out.println("执行结束");

        Thread.sleep(1000000L);

        // 4. 关闭sc
        sc.stop();

    }
}

class MyPartitioner extends Partitioner {
    private Integer numOfPartitioner;

    public MyPartitioner() {}
    public MyPartitioner(Integer numOfPartitioner) {
        this.numOfPartitioner = numOfPartitioner;
    }
    @Override
    public int numPartitions() {
        return this.numOfPartitioner;
    }

    @Override
    public int getPartition(Object key) {
        if(key.equals("nba")){
            return 0;
        }else if(key.equals("wnba")){
            return 1;
        }else{
            return 2;
        }
    }
}
```

解释：

`PairRDDFunctions.scala`

```scala
if(self.partitioner == Some(partitioner)){
	self.mapPartitions( iter => {
	val context = TaskContext.get()
	new InterruptibleIterator(context，aggregator.combineValuesByKey(iter，context))}，preservesPartitioning = true)
}else{
	new ShuffledRDD[K，V，C](self，partitioner)
	.setSerializer(serializer)
	.setAggregator(aggregator)
	.setMapSideCombine(mapSideCombine)
	}
}
```

判断分区器是否相同`self.partitioner == Some(partitioner)`

要使两次连续reduceByKey()不影响任务数量则需要重写hashcode()和equals()方法

```java
import org.apache.spark.Partitioner;  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import scala.Tuple2;  
  
import java.util.ArrayList;  
import java.util.List;  
  
public class Spark04_Partitioner {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        List<Tuple2<String, Integer>> nums = new ArrayList<>();  
        nums.add(new Tuple2<>("nba", 1));  
        nums.add(new Tuple2<>("wnba", 2));  
        nums.add(new Tuple2<>("nba", 3));  
        nums.add(new Tuple2<>("cba", 4));  
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(nums, 2);  
  
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(kv -> {  
            return kv;  
        });  
  
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey(new MyPartitioner(3), Integer::sum);  
        JavaPairRDD<String, Integer> reduceRDD1 = reduceRDD.reduceByKey(new MyPartitioner(3), Integer::sum);  
  
        reduceRDD1.collect();  
  
        System.out.println("执行结束");  
  
        Thread.sleep(1000000L);  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}  
  
class MyPartitioner extends Partitioner {  
    private Integer numOfPartitioner;  
  
    public MyPartitioner() {}  
    public MyPartitioner(Integer numOfPartitioner) {  
        this.numOfPartitioner = numOfPartitioner;  
    }  
    @Override  
    public int numPartitions() {  
        return this.numOfPartitioner;  
    }  
  
    @Override  
    public int getPartition(Object key) {  
        if(key.equals("nba")){  
            return 0;  
        }else if(key.equals("wnba")){  
            return 1;  
        }else{  
            return 2;  
        }  
    }  
  
    @Override  
    public int hashCode() {  
        return this.numOfPartitioner.hashCode();  
    }  
  
    @Override  
    public boolean equals(Object obj) {  
        if(obj instanceof MyPartitioner){  
            return ((MyPartitioner)obj).numOfPartitioner.equals(this.numOfPartitioner);  
        }else{  
            return false;  
        }  
    }  
}
```

## RDD的局限性（两个例子）和广播变量

### 为什么同时使用collect()和forEach()？

RDD在foreach循环时，逻辑代码和操作全部都是在Executor端完成的，那么结果不会拉取回到Driver端 

RDD无法实现数据拉取操作

collect

如果Executor端使用了Driver端数据，那么需要从Driver端将数据拉取到Executor端数据拉取的单位是Task（任务）

如果数据不是以Task为传输单位，而是以Executor为单位进行传输，那么性能会提高

RDD不能以Executor为单位进行数据传输

### 广播变量

Spark需要采用特殊的数据模型实现数据传输：广播变量

默认数据传输以Task为单位进行传输，如果想要以Executor为单位传输，那么需要进行包装（封装） 

`final Broadcast<List<String>> broadcast = jsc.broadcast(okList);`

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import org.apache.spark.broadcast.Broadcast;  
  
import java.util.Arrays;  
import java.util.List;  
  
public class Spark_Broadcast {  
    public static void main(String[] args) throws InterruptedException {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setMaster("local[2]");  
        conf.setAppName("sparkCore");  
  
        // 2. 创建sparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hadoop", "spark", "flink", "hive", "hbase"));  
  
        List<String> list = Arrays.asList("hadoop", "spark", "flink");  
  
        Broadcast<List<String>> broadcast = sc.broadcast(list);  
  
        rdd.filter(x -> broadcast.value().contains(x)).collect().forEach(System.out::println);  
  
        // 注意：如果broadcast的value是一个不可序列化的对象，则会报错：  
        // java.io.NotSerializableException: org.apache.spark.sql.catalyst.expressions.UnsafeRow  
  
        // 4. 关闭sc  
        sc.stop();  
  
    }  
}
```

输出结果如下所示：

```
hadoop
spark
flink
```

### 处理JSON数据

数据格式：JSON

1. 每一行就是一个JSON格式的数据，而且表示一个对象，对象内容必须包含在中 
2. 对象中的多个属性必须采用逗号隔开
3. 每一个属性，属性名和属性值之间采用冒号隔开
4. 属性名必须采用双引号声明，属性值如果为字符串类型，也需要采用双引号包含

重点关注RDD的功能和方法，数据格式不是我们的学习重点。

在特殊场景中，Spark对数据处理的逻辑进行了封装来简化开发。

SparkSQL就是对RDD的封装



