# Spark数据源

<!-- toc -->

# Spark 数据源概览

Spark 是一个强大的分布式计算框架，能够从多种数据源读取、处理和写入数据。下面详细介绍 Spark 支持的数据源以及相关的示例代码。

## 1. 内存数据

- **RDD**（Resilient Distributed Dataset）：Spark 的基础数据结构，可以从已有的集合（如 List、Array、等）创建。
- **DataFrame** 和 **Dataset**：更高级的抽象和结构化的数据表示。

## 2. 文件数据

- **文本文件**：Spark 能够读取文本文件（如 CSV、JSON、Parquet 等），通常使用 SparkContext 的 `textFile` 或 `spark.read` 方法。
  
  示例代码：
  ```scala
  // 读取文本文件
  val textRDD = spark.sparkContext.textFile("path/to/textfile.txt")
  val textDF = spark.read.text("path/to/textfile.txt")
  ```
  
## 3. JSON 数据

- Spark 支持读取 JSON 格式的数据，使用 DataFrame API 进行加载。

  示例代码：
  ```scala
  // 读取 JSON 文件
  val jsonDF = spark.read.json("path/to/data.json")
  jsonDF.show()
  ```

## 4. Parquet 文件

- Parquet 是一种列式存储格式，具有高压缩率和高性能，适合与 Spark 一起使用。

  示例代码：
  ```scala
  // 读取 Parquet 文件
  val parquetDF = spark.read.parquet("path/to/data.parquet")
  parquetDF.show()
  ```

## 5. Hive 表

- Spark 能够直接访问存储在 Hive 的表，需要配置 Hive 支持。

  示例代码：
  ```scala
  // 读取 Hive 表
  spark.sql("USE database_name")
  val hiveDF = spark.sql("SELECT * FROM table_name")
  hiveDF.show()
  ```

## 6. JDBC 数据库

- Spark 支持通过 JDBC 从关系型数据库（如 MySQL、PostgreSQL、Oracle 等）读取和写入数据。

  示例代码：
  ```scala
  // JDBC 读取数据
  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://hostname:port/dbname")
    .option("dbtable", "table_name")
    .option("user", "username")
    .option("password", "password")
    .load()

  jdbcDF.show()
  ```

## 7. Kafka

- Spark Streaming 可以与 Kafka 集成，读取实时流数据。

  示例代码：
  ```scala
  // 从 Kafka 读取数据
  val kafkaDF = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "topic_name")
    .load()

  kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show()
  ```

## 8. 其他数据源

Spark 还支持其他多种数据源，如：

- ORC 文件
- Avro 文件
- Cassandra 数据库
- Redis
- HBase
- 以及其他可以通过 Spark 的 DataSource API 进行扩展的源

## 读取数据示例代码总结

以下示例代码整合了不同数据源的读取方法：

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Spark Data Sources Example")
  .getOrCreate()

// 1. 从文本文件读取
val textDF = spark.read.text("path/to/textfile.txt")
textDF.show()

// 2. 从 JSON 文件读取
val jsonDF = spark.read.json("path/to/data.json")
jsonDF.show()

// 3. 从 Parquet 文件读取
val parquetDF = spark.read.parquet("path/to/data.parquet")
parquetDF.show()

// 4. 从 Hive 表读取
spark.sql("USE database_name")
val hiveDF = spark.sql("SELECT * FROM table_name")
hiveDF.show()

// 5. 从 JDBC 数据库读取
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://hostname:port/dbname")
  .option("dbtable", "table_name")
  .option("user", "username")
  .option("password", "password")
  .load()
jdbcDF.show()

// 6. 从 Kafka 读取
val kafkaDF = spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "topic_name")
  .load()
kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show()

spark.stop()
```

## 总结

Spark 提供强大的数据源连接能力，涵盖了从传统的文件读取到实时流处理的广泛场景。使用 Spark 的 DataFrame 和 SQL API，可以方便地对数据进行处理和分析。

# 附录

## CSV文件

CSV文件就是将数据采用逗号分隔的数据文件。

### read

读取示例代码：

```java
final Dataset<Row> csv= sparkSession.read()
.option("header"，"true")//配置
.option("sep","_")// 配置：It=>tsv，csv.csv(path:"data/user.csv");
.csv.show();
```


```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
public class SQL03_SQL_Source_CSV {  
    public static void main(String[] args) {  
        // 创建SparkSession  
        SparkSession sparkSession = SparkSession.builder()  
                .appName("SQL01_Model")  
                .master("local[2]")  
                .getOrCreate();  
  
        // 读取CSV文件  
        Dataset<Row> csvDF = sparkSession.read()  
                .format("csv")  
                .option("header", "true").option("sep", ",")
                .csv("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.csv");  
  
        // 显示数据  
        csvDF.show();  
  
  
        sparkSession.stop();  
  
    }  
}
```

输出结果如下所示：

```
+---+----+---+
| id|name|age|
+---+----+---+
|  1|John| 25|
|  2|Jane| 30|
|  3| Bob| 40|
+---+----+---+
```

### write

保存文件示例代码：

```java
csvDF.write().csv("output");
```

含有分区文件`part-00000-7122d0ef-0e2e-4507-a9cd-74b297e635f2-c000.csv`的目录

设置mode，实现覆盖写或是追加，相关源代码如下所示：

```scala
public DataFrameWriter<T> mode(final String saveMode) {
        String var4 = saveMode.toLowerCase(Locale.ROOT);
        if ("overwrite".equals(var4)) {
            return this.mode(SaveMode.Overwrite);
        } else if ("append".equals(var4)) {
            return this.mode(SaveMode.Append);
        } else if ("ignore".equals(var4)) {
            return this.mode(SaveMode.Ignore);
        } else if ("error".equals(var4) ? true : ("errorifexists".equals(var4) ? true : "default".equals(var4))) {
            return this.mode(SaveMode.ErrorIfExists);
        } else {
            throw new IllegalArgumentException((new StringBuilder(114)).append("Unknown save mode: ").append(saveMode).append(". Accepted ").append("save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'.").toString());
        }
    }
```

完整向CSV文件写入内容的代码如下所示：

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQL03_SQL_Source_CSV {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .appName("SQL01_Model")
                .master("local[2]")
                .getOrCreate();

        // 读取CSV文件
        Dataset<Row> csvDF = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("sep", ",")
                .csv("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.csv");

        // 显示数据
        csvDF.show();

        csvDF.write()
                .format("csv")
                .option("header", "true")
                .mode("overwrite")
                .csv("output");


        sparkSession.stop();

    }
}
```


## JSON文件

JSON:JavaScript ObjectNotation

对象：{}

数组：[]

JSON文件：整个文件的数据格式符合JSON格式，不是一行数据符合JSON格式

SparkSQL其实就是对SparkCoreRDD的封装。RDD读取文件采用的是Hadoop，Hadoop按行读取文件内容。SparkSQL只需要保证JSON文件中一行数据符合JSON格式即可，无需整个文件符合JSON格式。

Spark基于HDFS按行读取JSON格式文件，所以使用的JSON格式要按行存储，不能保存为一个完整的JSON文件，否则读取的大部分数据都为空。相关报错信息如下所示：

```
org.apache.spark.sql.AnalysisException:

SinceSpark2.3，thequeriesfromrawJsoN/csVfilesaredisallowedwhenthe referencedcolumnsonlyincludetheinternalcorruptrecord column
(named _corrupt_record by default)
```

### read

```java
final Dataset<Row>json =sparkSession.read().json(path:"data/user.json"); json.show();
```

完整代码如下所示：

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
public class SQL03_SQL_Source_JSON {  
    public static void main(String[] args) {  
        // 创建SparkSession  
        SparkSession sparkSession = SparkSession.builder()  
                .appName("SQL01_Model")  
                .master("local[2]")  
                .getOrCreate();  
  
        // 读取CSV文件  
        Dataset<Row> jsonDF = sparkSession.read()  
                .json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");  
  
        // 显示数据  
        jsonDF.show();  
  
        jsonDF.write().parquet("people.parquet");  
  
  
  
        sparkSession.stop();  
  
    }  
}
```

### JSON和CSV相互转换

通过Dataset

### 行式存储的文件（Primary Key）

查询快，统计慢

### 列式存储的文件

查询快，统计快

HBase使用列式存储


## Parquet文件

列式存储的数据自带列分割。

`part-00000-6b7cb14a-b49f-4b68-b24e-05ed6955c863-c000.snappy.parquet`中snappy表示压缩格式（Hadoop）

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
public class SQL03_SQL_Source_Parquet {  
    public static void main(String[] args) {  
        // 创建SparkSession  
        SparkSession sparkSession = SparkSession.builder()  
                .appName("SQL01_Model")  
                .master("local[2]")  
                .getOrCreate();  
  
        // 读取CSV文件  
        Dataset<Row> parquetDF = sparkSession.read()  
                .parquet("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.parquet");  
  
        // 显示数据  
        parquetDF.show();  
  
        //parquetDF.write().parquet("people.parquet");  
  
  
  
        sparkSession.stop();  
  
    }  
}
```

## MySQL&JDBC

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
import java.util.Properties;  
  
public class SQL03_SQL_Source_MySQL {  
    public static void main(String[] args) {  
        // 创建SparkSession  
        SparkSession sparkSession = SparkSession.builder()  
                .appName("SQL01_Model")  
                .master("local[2]")  
                .getOrCreate();  
  
        Properties properties = new Properties();  
        properties.setProperty("user", "root");  
        properties.setProperty("password", "root123");  
  
        // 读取MySQL数据  
        Dataset<Row> dataset = sparkSession.read()  
               .jdbc("jdbc:mysql://localhost:3306/tiandi", "users", properties);  
  
        dataset.show();  
		dataset.write().jdbc("jdbc:mysql://localhost:3306/tiandi", "users2", properties);
  
  
        sparkSession.stop();  
  
    }  
}
```

## HIVE

SparkSQL可以采用内嵌Hive（spark开箱即用的 hive），也可以采用外部 Hive。企业开发中，通常采用外部Hive。

2）拷贝 hive-site.xml到resources 目录（如果需要操作Hadoop，需要拷贝 hdfs-site.xml、 core-site.xml、 yarn-site.xml）

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!--配置Hive保存元数据信息所需的 MySQL URL地址-->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://localhost:3306/metastore?useSSL=false&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;allowPublicKeyRetrieval=true</value>
    </property>

    <!--配置Hive连接MySQL的驱动全类名-->
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.cj.jdbc.Driver</value>
    </property>

    <!--配置Hive连接MySQL的用户名 -->
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>root</value>
    </property>

    <!--配置Hive连接MySQL的密码 -->
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>root123</value>
    </property>

    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>

    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>

    <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
    </property>

    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>hadoop100</value>
    </property>

    <property>
        <name>hive.metastore.event.db.notification.api.auth</name>
        <value>false</value>
    </property>
    
    <property>
        <name>hive.cli.print.header</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.cli.print.current.db</name>
        <value>true</value>
    </property>
</configuration>


```

hive-site.xml完整配置文件内容如下所示：

```xml
<configuration>
  <!-- Hive元数据存储的URI -->
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://myhost:9083</value>
  </property>

  <!-- Hive元数据客户端套接字超时时间（以毫秒为单位） -->
  <property>
    <name>hive.metastore.client.socket.timeout</name>
    <value>300</value>
  </property>

  <!-- Hive数据仓库目录 -->
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>

  <!-- 子目录是否继承权限 -->
  <property>
    <name>hive.warehouse.subdir.inherit.perms</name>
    <value>true</value>
  </property>

  <!-- 自动转换连接类型的Join操作 -->
  <property>
    <name>hive.auto.convert.join</name>
    <value>true</value>
  </property>

  <!-- 自动转换连接类型的Join操作时条件不满足的最大数据量（以字节为单位） -->
  <property>
    <name>hive.auto.convert.join.noconditionaltask.size</name>
    <value>20971520</value>
  </property>

  <!-- 是否优化Bucket Map Join的Sorted Merge -->
  <property>
    <name>hive.optimize.bucketmapjoin.sortedmerge</name>
    <value>false</value>
  </property>

  <!-- SMB Join操作缓存的行数 -->
  <property>
    <name>hive.smbjoin.cache.rows</name>
    <value>10000</value>
  </property>

  <!-- 是否启用Hive Server2日志记录操作 -->
  <property>
    <name>hive.server2.logging.operation.enabled</name>
    <value>true</value>
  </property>

  <!-- Hive Server2操作日志的存储位置 -->
  <property>
    <name>hive.server2.logging.operation.log.location</name>
    <value>/var/log/hive/operation_logs</value>
  </property>

  <!-- MapReduce作业的Reduce任务数 -->
  <property>
    <name>mapred.reduce.tasks</name>
    <value>-1</value>
  </property>

  <!-- 每个Reduce任务的数据量（以字节为单位） -->
  <property>
    <name>hive.exec.reducers.bytes.per.reducer</name>
    <value>67108864</value>
  </property>

  <!-- 最大允许复制文件的大小（以字节为单位） -->
  <property>
    <name>hive.exec.copyfile.maxsize</name>
    <value>33554432</value>
  </property>

  <!-- 同时运行的最大Reduce任务数 -->
  <property>
    <name>hive.exec.reducers.max</name>
    <value>1099</value>
  </property>

  <!-- Vectorized Group By操作的检查间隔 -->
  <property>
    <name>hive.vectorized.groupby.checkinterval</name>
    <value>4096</value>
  </property>

  <!-- Vectorized Group By操作的Flush比例 -->
  <property>
    <name>hive.vectorized.groupby.flush.percent</name>
    <value>0.1</value>
  </property>

  <!-- 是否使用统计信息来优化查询计划 -->
  <property>
    <name>hive.compute.query.using.stats</name>
    <value>false</value>
  </property>

  <!-- 是否启用向量化执行引擎 -->
  <property>
    <name>hive.vectorized.execution.enabled</name>
    <value>true</value>
  </property>

  <!-- 是否在Reduce阶段启用向量化执行 -->
  <property>
    <name>hive.vectorized.execution.reduce.enabled</name>
    <value>true</value>
  </property>

  <!-- 是否使用向量化输入格式 -->
  <property>
    <name>hive.vectorized.use.vectorized.input.format</name>
    <value>true</value>
  </property>

  <!-- 是否使用检查表达式的向量化执行 -->
  <property>
    <name>hive.vectorized.use.checked.expressions</name>
    <value>true</value>
  </property>

  <!-- 是否使用向量化序列化和反序列化 -->
  <property>
    <name>hive.vectorized.use.vector.serde.deserialize</name>
    <value>false</value>
  </property>

  <!-- 向量化适配器的使用模式 -->
  <property>
    <name>hive.vectorized.adaptor.usage.mode</name>
    <value>chosen</value>
  </property>

  <!-- 排除的向量化输入格式列表 -->
  <property>
    <name>hive.vectorized.input.format.excludes</name>
    <value>org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat</value>
  </property>

  <!-- 是否合并Map输出的小文件 -->
  <property>
    <name>hive.merge.mapfiles</name>
    <value>true</value>
  </property>

  <!-- 是否合并MapReduce输出的小文件 -->
  <property>
    <name>hive.merge.mapredfiles</name>
    <value>false</value>
  </property>

  <!-- 是否启用CBO优化 -->
  <property>
    <name>hive.cbo.enable</name>
    <value>false</value>
  </property>

  <!-- Fetch任务转换级别 -->
  <property>
    <name>hive.fetch.task.conversion</name>
    <value>minimal</value>
  </property>

  <!-- 触发Fetch任务转换的数据量阈值（以字节为单位） -->
  <property>
    <name>hive.fetch.task.conversion.threshold</name>
    <value>268435456</value>
  </property>

  <!-- Limit操作的内存使用百分比 -->
  <property>
    <name>hive.limit.pushdown.memory.usage</name>
    <value>0.1</value>
  </property>

  <!-- 是否合并Spark任务输出的小文件 -->
  <property>
    <name>hive.merge.sparkfiles</name>
    <value>true</value>
  </property>

  <!-- 合并小文件时的平均大小（以字节为单位） -->
  <property>
    <name>hive.merge.smallfiles.avgsize</name>
    <value>16777216</value>
  </property>

  <!-- 每个任务合并的数据量（以字节为单位） -->
  <property>
    <name>hive.merge.size.per.task</name>
    <value>268435456</value>
  </property>

  <!-- 是否启用重复消除优化 -->
  <property>
    <name>hive.optimize.reducededuplication</name>
    <value>true</value>
  </property>

  <!-- 最小Reduce任务数以启用重复消除优化 -->
  <property>
    <name>hive.optimize.reducededuplication.min.reducer</name>
    <value>4</value>
  </property>

  <!-- 是否启用Map端聚合 -->
  <property>
    <name>hive.map.aggr</name>
    <value>true</value>
  </property>

  <!-- Map端聚合的哈希表内存比例 -->
  <property>
    <name>hive.map.aggr.hash.percentmemory</name>
    <value>0.5</value>
  </property>

  <!-- 是否优化动态分区排序 -->
  <property>
    <name>hive.optimize.sort.dynamic.partition</name>
    <value>false</value>
  </property>

  <!-- Hive执行引擎类型（mr、tez、spark） -->
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
  </property>

  <!-- Spark Executor的内存大小 -->
  <property>
    <name>spark.executor.memory</name>
    <value>2572261785b</value>
  </property>

  <!-- Spark Driver的内存大小 -->
  <property>
    <name>spark.driver.memory</name>
    <value>3865470566b</value>
  </property>

  <!-- 每个Spark Executor的核心数 -->
  <property>
    <name>spark.executor.cores</name>
    <value>4</value>
  </property>

  <!-- Spark Driver的内存Overhead -->
  <property>
    <name>spark.yarn.driver.memoryOverhead</name>
    <value>409m</value>
  </property>

  <!-- Spark Executor的内存Overhead -->
  <property>
    <name>spark.yarn.executor.memoryOverhead</name>
    <value>432m</value>
  </property>

  <!-- 是否启用动态资源分配 -->
  <property>
    <name>spark.dynamicAllocation.enabled</name>
    <value>true</value>
  </property>

  <!-- 动态资源分配的初始Executor数量 -->
  <property>
    <name>spark.dynamicAllocation.initialExecutors</name>
    <value>1</value>
  </property>

  <!-- 动态资源分配的最小Executor数量 -->
  <property>
    <name>spark.dynamicAllocation.minExecutors</name>
    <value>1</value>
  </property>

  <!-- 动态资源分配的最大Executor数量 -->
  <property>
    <name>spark.dynamicAllocation.maxExecutors</name>
    <value>2147483647</value>
  </property>

  <!-- 是否在Hive元数据存储中执行setugi操作 -->
  <property>
    <name>hive.metastore.execute.setugi</name>
    <value>true</value>
  </property>

  <!-- 是否支持并发操作 -->
  <property>
    <name>hive.support.concurrency</name>
    <value>true</value>
  </property>

  <!-- ZooKeeper服务器列表 -->
  <property>
    <name>hive.zookeeper.quorum</name>
    <value>myhost04,myhost03,myhost02</value>
  </property>

  <!-- ZooKeeper客户端端口号 -->
  <property>
    <name>hive.zookeeper.client.port</name>
    <value>2181</value>
  </property>

  <!-- Hive使用的ZooKeeper命名空间 -->
  <property>
    <name>hive.zookeeper.namespace</name>
    <value>hive_zookeeper_namespace_hive</value>
  </property>

  <!-- 集群委派令牌存储类 -->
  <property>
    <name>hive.cluster.delegation.token.store.class</name>
    <value>org.apache.hadoop.hive.thrift.MemoryTokenStore</value>
  </property>

  <!-- 是否启用Hive Server2用户代理模式 -->
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>true</value>
  </property>

  <!-- 是否启用Hive元数据存储的SASL认证 -->
  <property>
    <name>hive.metastore.sasl.enabled</name>
    <value>true</value>
  </property>

  <!-- Hive Server2的认证方式 -->
  <property>
    <name>hive.server2.authentication</name>
    <value>kerberos</value>
  </property>

  <!-- Hive元数据存储的Kerberos主体名称 -->
  <property>
    <name>hive.metastore.kerberos.principal</name>
    <value>hive/_HOST@MY.COM</value>
  </property>

  <!-- Hive Server2的Kerberos主体名称 -->
  <property>
    <name>hive.server2.authentication.kerberos.principal</name>
    <value>hive/_HOST@MY.COM</value>
  </property>

  <!-- 是否启用Spark Shuffle服务 -->
  <property>
    <name>spark.shuffle.service.enabled</name>
    <value>true</value>
  </property>

  <!-- 是否在没有Limit操作的OrderBy语句中执行严格检查 -->
  <property>
    <name>hive.strict.checks.orderby.no.limit</name>
    <value>false</value>
  </property>

  <!-- 是否在没有分区过滤条件的查询中执行严格检查 -->
  <property>
    <name>hive.strict.checks.no.partition.filter</name>
    <value>false</value>
  </property>

  <!-- 是否执行严格的类型安全性检查 -->
  <property>
    <name>hive.strict.checks.type.safety</name>
    <value>true</value>
  </property>

  <!-- 是否执行严格的笛卡尔积检查 -->
  <property>
    <name>hive.strict.checks.cartesian.product</name>
    <value>false</value>
  </property>

  <!-- 是否执行严格的桶排序检查 -->
  <property>
    <name>hive.strict.checks.bucketing</name>
    <value>true</value>
  </property>
</configuration>
```



