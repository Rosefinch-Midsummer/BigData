# Spark Streaming

<!-- toc -->


## 为什么使用Spark Streaming？

Spark Streaming用于流式数据的处理。

Spark Streaming支持的数据输入源很多，例如：Kafka、Flume、HDFS等。

数据输入后可以用Spark的高度抽象原语如：map丶reduce、join、window等进行运算。

结果也能保存在很多地方，如HDFS、数据库等。

### 有界数据流和无界数据流

有界数据流可以计算

无界数据流无法计算

Spark Streaming通过数据采集器将无界数据流切分成有界数据流，方便计算

SparkStreaming底层还是SparkCore，就是在流式数据处理中进行的封装

### 实时计算、离线计算、流式计算、批量计算

从数据处理方式的角度

- 流式数据处理：一个数据一个数据的处理
- 微批量数据处理：一小批数据一小批数据的处理
- 批量数据处理：一批数据一批数据的处理
  从数据处理延迟的角度
- 实时数据处理：数据处理的延迟以毫秒为单位
- 准实时数据处理：数据处理的延迟以秒、分钟为单位
- 离线数据处理：数据处理的延迟以小时，天为单位

Spark批量、离线

Spark Streaming微批量、准实时

一小批不是按个数而是按时间来定义比如3s内数据作为一小批，对应数据模型为离散化流dstream

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250114162016.png)

### 什么是DStream？

SparkCore => RDD

SparkSQL => DataFrame、DataSet

Spark Streaming使用离散化流（Discretized Stream）作为抽象表示，叫作DStream。 

DStream是随时间推移而收到的数据的序列。

在DStream内部，每个时间区间收到的数据都作为RDD存在，而DStream是由这些RDD所组成的序列（因此得名“离散化"）。

所以简单来讲，DStream就是对RDD在实时数据处理场景的一种封装。

### 架构图

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250115165321.png)

整体架构图

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250115165412.png)

SparkStreaming架构图

### 方法VS算子

RDD算子是分布式计算，效率更高

### SparkStreaming VS RDD算子

SparkStreaming有三个地方可以写代码，SparkStreaming的方法被称为原语。

DStream 上的操作与 RDD 的类似，分为转换和输出两种，此外转换操作中还有一些比较特殊的原语，如：transform()以及各种Window 相关的原语。

RDD算子只有两个地方可以写代码，Driver端和Executor端

```java
//int i= 10;(Driver 1)
        wordCountDS.foreachRDD(
                rdd -> {
                    //int j = 20;(Driver X)
                    rdd.foreach(
                            (num) -> {
                                //int k=30;(Executor N)
                                System.out.println(num);
                            }
                );}
        );
```

## Spark Streaming环境搭建

Spark在流式数据的处理场景中对核心功能环境进行了封装

```java
package com.zzw.bigdata.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingEnv01 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingEnv01");
        conf.setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3 * 1000));
        javaStreamingContext.start();
//        等待数据采集器的结束，如果采集器停止运行，那么main线程会继续执行
        javaStreamingContext.awaitTermination();
//        数据采集器是一个长期执行的任务，所以不能停止，也不能释放资源，只能等待任务结束
//        javaStreamingContext.close();
//        while (true) {
//        }
    }
}
```

上面的代码执行会抛出异常`Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute`

原因：没有启动行动算子。

Spark Streaming对应的是无界数据流，没有启动行动算子比如print()就会报错。

Socket数据不会保留，不输出就会出问题。

你的代码抛出 `java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute` 的异常是因为在创建 `JavaStreamingContext` 后没有注册任何输出操作（如接收输入流，处理数据等），Spark Streaming 在没有任何操作的情况下无法执行任务。

**解决方案**：

1. 需要创建一个输入流，比如通过 `socketTextStream` 或者其他接收源。
2. 注册一些输出操作，例如调用 `print()`、`saveAsTextFiles()` 等方法来处理数据。

以下是一个修正后的代码示例，假设使用 `socketTextStream` 来接收数据：

```java
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.Arrays;

public class SparkStreamingEnv01 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingEnv01");
        conf.setMaster("local[2]");
        
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3000));
        
        // 创建一个输入流，假设我们在 localhost:9999 上接收数据
        JavaInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);
        
        // 注册输出操作，例如打印接收到的每一行
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                // 打印每一行
                rdd.foreach(line -> System.out.println(line));
            }
        });
        
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
```

注意事项：

1. **输入源**：你需要在运行前确保有一个 socket 服务在 `localhost:9999` 监听。可以使用 `nc -lk 9999` 命令在终端开启一个简单的字符发送服务。
2. **依赖**：确保你的项目能找到 Spark 的相关依赖。

这段代码创建了一个接收来自 `localhost` 在 9999 端口的文本流，并且会将接收到的每一行打印到控制台。这种方式可以确保有输出操作被注册，避免抛出上述异常。



## Socket VS Kafka

版本选型

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250115160549.png)

注意：目前spark3.0.0以上版本只有Direct模式。

### 网络（socket）数据流处理

下载`netcat`工具，执行`nc -lp 9999`，运行下面的代码：

```java
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingSocket {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingEnv01");
        conf.setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3 * 1000));

        //通过环境对象对接socket数据源，获取数据模型，进行数据处理
        JavaReceiverInputDStream<String> socketDS = javaStreamingContext.socketTextStream("localhost", 9999);

        socketDS.print();

        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();

    }
}
```

输出结果如下所示：

```
-------------------------------------------
Time: 1736923509000 ms
-------------------------------------------
a
a
a
a
a

-------------------------------------------
Time: 1736923512000 ms
-------------------------------------------
wwww
```

调用print()才生成时间戳。

### Kafka数据流处理

先启动zookeeper，再启动kafka

使用offset explorer工具

相关代码如下所示：

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;  
import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.spark.api.java.function.Function;  
import org.apache.spark.streaming.Duration;  
import org.apache.spark.streaming.api.java.JavaInputDStream;  
import org.apache.spark.streaming.api.java.JavaStreamingContext;  
import org.apache.spark.streaming.kafka010.ConsumerStrategies;  
import org.apache.spark.streaming.kafka010.KafkaUtils;  
import org.apache.spark.streaming.kafka010.LocationStrategies;  
  
import java.util.ArrayList;  
import java.util.HashMap;  
  
  
public class SparkStreamingKafka {  
    public static void main(String[] args) throws InterruptedException {  
        // 创建流环境  
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "HelloWorld", Duration.apply(3000));  
  
        // 创建配置参数  
        HashMap<String, Object> map = new HashMap<>();  
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");  
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");  
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");  
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu");  
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");  
  
        // 需要消费的主题  
        ArrayList<String> strings = new ArrayList<>();  
        strings.add("topic_db");  
  
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferBrokers(), ConsumerStrategies.<String, String>Subscribe(strings, map));  
  
        directStream.map(new Function<ConsumerRecord<String, String>, String>() {  
            @Override  
            public String call(ConsumerRecord<String, String> v1) throws Exception {  
                return v1.value();  
            }  
        }).print(100);  
  
        // 执行流的任务  
        javaStreamingContext.start();  
        javaStreamingContext.awaitTermination();  
    }  
}
```

更改日志打印级别

如果不希望运行时打印大量日志，可以在resources文件夹中添加`log4j2.properties`文件，并添加日志配置信息

```properties
# Set everything to be logged to the console

rootLogger.level = ERROR

rootLogger.appenderRef.stdout.ref = console

# In the pattern layout configuration below, we specify an explicit `%ex` conversion

# pattern for logging Throwables. If this was omitted, then (by default) Log4J would

# implicitly add an `%xEx` conversion pattern which logs stacktraces with additional

# class packaging information. That extra information can sometimes add a substantial

# performance overhead, so we disable it in our default logging config.

# For more information, see SPARK-39361.

appender.console.type = Console

appender.console.name = console

appender.console.target = SYSTEM_ERR

appender.console.layout.type = PatternLayout

appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex

# Set the default spark-shell/spark-sql log level to WARN. When running the

# spark-shell/spark-sql, the log level for these classes is used to overwrite

# the root logger's log level, so that the user can have different defaults

# for the shell and regular Spark apps.

logger.repl.name = org.apache.spark.repl.Main

logger.repl.level = warn

logger.thriftserver.name = org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver

logger.thriftserver.level = warn

# Settings to quiet third party logs that are too verbose

logger.jetty1.name = org.sparkproject.jetty

logger.jetty1.level = warn

logger.jetty2.name = org.sparkproject.jetty.util.component.AbstractLifeCycle

logger.jetty2.level = error

logger.replexprTyper.name = org.apache.spark.repl.SparkIMain$exprTyper

logger.replexprTyper.level = info

logger.replSparkILoopInterpreter.name = org.apache.spark.repl.SparkILoop$SparkILoopInterpreter

logger.replSparkILoopInterpreter.level = info

logger.parquet1.name = org.apache.parquet

logger.parquet1.level = error

logger.parquet2.name = parquet

logger.parquet2.level = error

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support

logger.RetryingHMSHandler.name = org.apache.hadoop.hive.metastore.RetryingHMSHandler

logger.RetryingHMSHandler.level = fatal

logger.FunctionRegistry.name = org.apache.hadoop.hive.ql.exec.FunctionRegistry

logger.FunctionRegistry.level = error

# For deploying Spark ThriftServer

# SPARK-34128: Suppress undesirable TTransportException warnings involved in THRIFT-4805

appender.console.filter.1.type = RegexFilter

appender.console.filter.1.regex = .*Thrift error occurred during processing of message.*

appender.console.filter.1.onMatch = deny appender.console.filter.1.onMismatch = neutral
```

启动生产者生产数据

```
[atguigu@hadoop102 ~]$ kafka-console-producer.sh --broker-list hadoop102:9092 --topic topicA hello spark
```

在IDEA控制台输出如下内容

```
-------------------------------------------

Time: 1602731772000 ms

-------------------------------------------

hello spark
```

### 案例解析

DStream是Spark Streaming的基础抽象，代表持续性的数据流和经过各种Spark算子操作后的结果数据流。

在内部实现上，每一批次的数据封装成一个RDD，一系列连续的RDD组成了DStream。对这些RDD的转换是由Spark引擎来计算。

说明：DStream中批次与批次之间计算相互独立。如果批次设置时间小于计算时间会出现计算任务叠加情况，需要多分配资源。通常情况，批次设置时间要大于计算时间。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250115164944.png)

## DStream转换

DStream上的操作与RDD的类似，分为转换和输出两种，此外转换操作中还有一些比较特殊的原语，如：transform()以及各种Window相关的原语。

### 无状态转化操作

无状态转化操作：就是把RDD转化操作应用到DStream每个批次上，每个批次相互独立，自己算自己的。

DStream的部分无状态转化操作列在了下表中，都是DStream自己的API。

注意，只有JavaPairDStream<Key, Value>才能使用xxxByKey()类型的转换算子。

|               |                                                              |                              |
| ------------- | ------------------------------------------------------------ | ---------------------------- |
| **函数名称**  | **目的**                                                     | **函数类型**                 |
| map()         | 对DStream中的每个元素应用给定函数，返回由各元素输出的元素组成的DStream。 | Function<in, out>            |
| flatMap()     | 对DStream中的每个元素应用给定函数，返回由各元素输出的迭代器组成的DStream。 | FlatMapFunction<in, out>     |
| filter()      | 返回由给定DStream中通过筛选的元素组成的DStream               | Function<in, Boolean>        |
| mapToPair()   | 改变DStream的分区数                                          | PairFunction<in, key, value> |
| reduceByKey() | 将每个批次中键相同的记录规约。                               | Function2<in, in, in>        |
| groupByKey()  | 将每个批次中的记录根据键分组。                               | ds.groupByKey()              |

需要记住的是，尽管这些函数看起来像作用在整个流上一样，但事实上每个DStream在内部是由许多RDD批次组成，且无状态转化操作是分别应用到每个RDD(一个批次的数据)上的。

### DStream和RDD

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.streaming.Duration;  
import org.apache.spark.streaming.api.java.JavaDStream;  
import org.apache.spark.streaming.api.java.JavaPairDStream;  
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;  
import org.apache.spark.streaming.api.java.JavaStreamingContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
  
public class SparkStreamingFunction {  
    public static void main(String[] args) throws Exception {  
        SparkConf conf = new SparkConf();  
        conf.setAppName("SparkStreamingEnv01");  
        conf.setMaster("local[2]");  
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3 * 1000));  
  
        //通过环境对象对接socket数据源，获取数据模型，进行数据处理  
        JavaReceiverInputDStream<String> socketDS = javaStreamingContext.socketTextStream("localhost", 9999);  
        JavaDStream<String> flatDS = socketDS.flatMap(  
                line -> Arrays.asList(line.split(" ")).iterator()  
        );  
  
        JavaPairDStream<String, Integer> wordDS = flatDS.mapToPair(  
                word -> new Tuple2<>(word, 1)  
        );  
  
        JavaPairDStream<String, Integer> wordCountDS = wordDS.reduceByKey(Integer::sum);  
  
        //DStream确实就是对RDD的封装，但是不是所有的方法都进行了分装。有些方法不能使用：sortBy，sortByKey  
        //如果特定场合下，就需要使用这些方法，那么就需要将DStream转换为RDD使用  
  
        //wordCountDS.print();  
        wordCountDS.foreachRDD(  
            rdd -> {  
                rdd.sortByKey().collect().forEach(System.out::println);  
            }  
        );  
  
        javaStreamingContext.start();  
  
        javaStreamingContext.awaitTermination();  
  
    }  
}
```

## 窗口操作

生产环境中，窗口操作主要应用于这样的需求：最近N时间，每个M时间的数据变化

需求：最近1个小时，每10分钟，气温的变化趋势

### 滑动窗口及窗口参数

窗口可以移动的称之为移动窗口，但是窗口移动是有幅度的，默认移动幅度就是采集周期

数据窗口范围扩大（6s），但是窗口移动幅度不变（3s），数据可能会有重复

数据窗口范围和窗口移动幅度一致（3s），数据不会有重复

窗口：其实就是数据的范围（时间）

window方法可以改变窗口的数据范围（默认数据范围为采集周期） window方法可以传递2个参数

第一个参数表示窗口的数据范围（时间）

第二个参数表示窗口的移动幅度（时间），可以不用传递，默认使用的就是采集周期

SparkStreaming在窗口移动时计算结果。

执行`nc -lp 9999`

```java
import org.apache.spark.SparkConf;  
import org.apache.spark.streaming.Duration;  
import org.apache.spark.streaming.api.java.JavaDStream;  
import org.apache.spark.streaming.api.java.JavaPairDStream;  
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;  
import org.apache.spark.streaming.api.java.JavaStreamingContext;  
import scala.Tuple2;  
  
import java.util.Arrays;  
  
public class SparkStreamingWindows {  
    public static void main(String[] args) throws Exception {  
        SparkConf conf = new SparkConf();  
        conf.setAppName("SparkStreamingEnv01");  
        conf.setMaster("local[2]");  
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3 * 1000));  
  
        //通过环境对象对接socket数据源，获取数据模型，进行数据处理  
        JavaReceiverInputDStream<String> socketDS = javaStreamingContext.socketTextStream("localhost", 9999);  
        JavaDStream<String> flatDS = socketDS.flatMap(  
                line -> Arrays.asList(line.split(" ")).iterator()  
        );  
  
        JavaPairDStream<String, Integer> wordDS = flatDS.mapToPair(  
                word -> new Tuple2<>(word, 1)  
        );  
  
        JavaPairDStream<String, Integer> windowDS = wordDS.window(new Duration(6 * 1000), new Duration(6 * 1000));  
  
        JavaPairDStream<String, Integer> wordCountDS = windowDS.reduceByKey(Integer::sum);  
  
        wordCountDS.print();  
//        wordCountDS.foreachRDD(  
//                rdd -> {  
//                    rdd.sortByKey().collect().forEach(System.out::println);  
//                }  
//        );  
  
        javaStreamingContext.start();  
  
        javaStreamingContext.awaitTermination();  
  
    }  
}
```

### WindowOperations

Window Operations可以设置窗口的大小和滑动窗口的间隔来动态的获取当前Streaming的允许状态。所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长。

窗口时长：计算内容的时间范围；

滑动步长：隔多久触发一次计算。

注意：这两者都必须为采集批次大小的整数倍。

如下图所示WordCount案例：窗口大小为批次的2倍，滑动步等于批次大小。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250115160633.png)

### Window

1）基本语法：

window(windowLength, slideInterval): 基于对源DStream窗口的批次进行计算返回一个新的DStream。

2）需求：

统计WordCount：3秒一个批次，窗口12秒，滑步6秒。

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class Test02_Window {
    public static void main(String[] args) throws InterruptedException {
        // 创建流环境
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "window", Duration.apply(3000));
        // 创建配置参数
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"atguigu");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 需要消费的主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("topicA");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferBrokers(), ConsumerStrategies.<String, String>Subscribe(strings,map));

        JavaDStream<String> stringJavaDStream = directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                String[] split = stringStringConsumerRecord.value().split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        JavaPairDStream<String, Integer> javaPairDStream = stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> window = javaPairDStream.window(Duration.apply(12000), Duration.apply(6000));
        window.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).print();

        // 执行流的任务
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
```

4）测试

```
[atguigu@hadoop102 ~]$ kafka-console-producer.sh --broker-list hadoop102:9092 --topic topicA hello spark
```

5）如果有多批数据进入窗口，最终也会通过window操作变成统一的RDD处理。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250115164720.png)

### reduceByKeyAndWindow

1）基本语法

`reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks])`：当在一个(K,V)对的DStream上调用此函数，会返回一个新(K,V)对的DStream，此处通过对滑动窗口中批次数据使用reduce函数来整合每个key的value值。

2）需求：

统计WordCount：3秒一个批次，窗口12秒，滑步6秒。

3）代码编写：

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class Test03_ReduceByKeyAndWindow {
    public static void main(String[] args) throws InterruptedException {
        // 创建流环境
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "window", Duration.apply(3000L));
        // 创建配置参数
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"atguigu");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 需要消费的主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("topicA");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferBrokers(), ConsumerStrategies.<String, String>Subscribe(strings,map));

        JavaDStream<String> stringJavaDStream = directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                String[] split = stringStringConsumerRecord.value().split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        JavaPairDStream<String, Integer> javaPairDStream = stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        javaPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        },Duration.apply(12000L),Duration.apply(6000L)).print();

        // 执行流的任务
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
```

2）测试

```
[atguigu@hadoop102 ~]$ kafka-console-producer.sh --broker-list hadoop102:9092 --topic topicA hello spark
```

## DStream输出

DStream通常将数据输出到，外部数据库或屏幕上。

DStream与RDD中的惰性求值类似，如果一个DStream及其派生出的DStream都没有被执行输出操作，那么这些DStream就都不会被求值。如果StreamingContext中没有设定输出操作，整个Context就都不会启动。

1）输出操作API如下：

`saveAsTextFiles(prefix, [suffix])`：以text文件形式存储这个DStream的内容。每一批次的存储文件名基于参数中的prefix和suffix。“`prefix-Time_IN_MS[.suffix]`”。

注意：**以上操作都是每一批次写出一次，会产生大量小文件，在生产环境，很少使用。**

`print()`：在运行流程序的驱动结点上打印DStream中每一批次数据的最开始10个元素。这用于开发和调试。

`foreachRDD(func)`：这是最通用的输出操作，即将函数func用于产生DStream的每一个RDD。其中参数传入的函数func应该实现将每一个RDD中数据推送到外部系统，如将RDD存入文件或者写入数据库。

在企业开发中通常采用`foreachRDD()`，它用来对DStream中的RDD进行任意计算。这和transform()有些类似，都可以让我们访问任意RDD。在foreachRDD()中，可以重用我们在Spark中实现的所有行动操作(action算子)。比如，常见的用例之一是把数据写到如MySQL的外部数据库中。

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.function.Function2;

import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.streaming.Duration;

import org.apache.spark.streaming.api.java.JavaDStream;

import org.apache.spark.streaming.api.java.JavaInputDStream;

import org.apache.spark.streaming.api.java.JavaPairDStream;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;

import org.apache.spark.streaming.kafka010.KafkaUtils;

import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import java.util.ArrayList;

import java.util.Arrays;

import java.util.HashMap;

import java.util.Iterator;

/**

 * @author yhm

 * @create 2022-09-01 16:47

 */

public class Test04_Save {

    public static void main(String[] args) throws InterruptedException {

        // 创建流环境

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "window", Duration.apply(3000L));

        // 创建配置参数

        HashMap<String, Object> map = new HashMap<>();

        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");

        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        map.put(ConsumerConfig.GROUP_ID_CONFIG,"atguigu");

        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 需要消费的主题

        ArrayList<String> strings = new ArrayList<>();

        strings.add("topicA");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferBrokers(), ConsumerStrategies.<String, String>Subscribe(strings,map));

        JavaDStream<String> stringJavaDStream = directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {

            @Override

            public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {

                String[] split = stringStringConsumerRecord.value().split(" ");

                return Arrays.asList(split).iterator();

            }

        });

        JavaPairDStream<String, Integer> javaPairDStream = stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {

            @Override

            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<>(s, 1);

            }

        });

        JavaPairDStream<String, Integer> resultDStream = javaPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

            @Override

            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;

            }

        }, Duration.apply(12000L), Duration.apply(6000L));

        resultDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

            @Override

            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {

                // 获取mysql连接

                // 写入到mysql中

                // 关闭连接

            }

        });

        // 执行流的任务

        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();

    }

}
```

## 关闭main线程

### 新增线程方式

```java
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingClose {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingEnv01");
        conf.setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3 * 1000));

        //通过环境对象对接socket数据源，获取数据模型，进行数据处理
        JavaReceiverInputDStream<String> socketDS = javaStreamingContext.socketTextStream("localhost", 9999);

        socketDS.print();

        javaStreamingContext.start();

        //close方法就是用于释放资源，关闭环境，但不能在当前main方法中调用，需要在另外一个线程中调用，否则会导致程序卡死
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3 * 1000);
                    javaStreamingContext.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        javaStreamingContext.awaitTermination();

    }
}
```

这种方式会抛出异常。

```
-------------------------------------------
Time: 1736931924000 ms
-------------------------------------------

Exception in thread "receiver-supervisor-future-0" java.lang.Error: java.lang.InterruptedException: sleep interrupted
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1155)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.lang.InterruptedException: sleep interrupted
	at java.lang.Thread.sleep(Native Method)
```

### 较为优雅的方式（stop）

`javaStreamingContext.stop(true, true);`

```java
package com.zzw.bigdata.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingClose {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingEnv01");
        conf.setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3 * 1000));

        //通过环境对象对接socket数据源，获取数据模型，进行数据处理
        JavaReceiverInputDStream<String> socketDS = javaStreamingContext.socketTextStream("localhost", 9999);

        socketDS.print();

        javaStreamingContext.start();

        //close方法就是用于释放资源，关闭环境，但不能在当前main方法中调用，需要在另外一个线程中调用，否则会导致程序卡死
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3 * 1000);
                    javaStreamingContext.stop(true, true);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        javaStreamingContext.awaitTermination();

    }
}

```

输出结果如下所示：

```
-------------------------------------------
Time: 1736932401000 ms
-------------------------------------------

Exception in thread "receiver-supervisor-future-0" java.lang.Error: java.lang.InterruptedException: sleep interrupted
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1155)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.lang.InterruptedException: sleep interrupted
	at java.lang.Thread.sleep(Native Method)
	at org.apache.spark.streaming.receiver.ReceiverSupervisor.$anonfun$restartReceiver$1(ReceiverSupervisor.scala:196)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.concurrent.Future$.$anonfun$apply$1(Future.scala:659)
	at scala.util.Success.$anonfun$map$1(Try.scala:255)
	at scala.util.Success.map(Try.scala:213)
	at scala.concurrent.Future.$anonfun$map$1(Future.scala:292)
	at scala.concurrent.impl.Promise.liftedTree1$1(Promise.scala:33)
	at scala.concurrent.impl.Promise.$anonfun$transform$1(Promise.scala:33)
	at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:64)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	... 2 more
-------------------------------------------
Time: 1736932404000 ms
-------------------------------------------

-------------------------------------------
Time: 1736932407000 ms
-------------------------------------------
```

### 使用kafka时优雅关闭

流式任务需要7\*24小时执行，但是有时涉及到升级代码需要主动停止程序，但是分布式程序没办法做到一个个进程去杀死，所以配置优雅的关闭就显得至关重要了。

关闭方式：使用外部文件系统来控制内部程序关闭。

1）主程序

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class Test05_Close {
    public static void main(String[] args) throws InterruptedException {
        // 创建流环境
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "window", Duration.apply(3000L));
        // 创建配置参数
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"atguigu");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 需要消费的主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("topicA");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferBrokers(), ConsumerStrategies.<String, String>Subscribe(strings,map));

        JavaDStream<String> stringJavaDStream = directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                String[] split = stringStringConsumerRecord.value().split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        JavaPairDStream<String, Integer> javaPairDStream = stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        javaPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        },Duration.apply(12000L),Duration.apply(6000L)).print();

        // 开启监控程序
        new Thread(new MonitorStop(javaStreamingContext)).start();

        // 执行流的任务
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

    public static class MonitorStop implements Runnable {

        JavaStreamingContext javaStreamingContext = null;

        public MonitorStop(JavaStreamingContext javaStreamingContext) {
            this.javaStreamingContext = javaStreamingContext;
        }

        @Override
        public void run() {
            try {
                FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "atguigu");
                while (true){
                    Thread.sleep(5000);
                    boolean exists = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"));
                    if (exists){
                        StreamingContextState state = javaStreamingContext.getState();
                        // 获取当前任务是否正在运行
                        if (state == StreamingContextState.ACTIVE){
                            // 优雅关闭
                            javaStreamingContext.stop(true, true);
                            System.exit(0);
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}

```

2）测试

（1）发送数据

```
[atguigu@hadoop102 ~]$ kafka-console-producer.sh --broker-list hadoop102:9092 --topic topicA hello spark
```

（2）启动Hadoop集群

```
[atguigu@hadoop102 hadoop-3.1.3]$ sbin/start-dfs.sh

[atguigu@hadoop102 hadoop-3.1.3]$ hadoop fs -mkdir /stopSpark
```