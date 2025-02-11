# SparkSQL

<!-- toc -->

# SparkSQL 简介

SparkSQL 是 Spark 生态系统的重要组成部分，用于处理结构化数据。它允许使用 SQL 查询语言访问和管理数据，同时结合了 Spark 的强大分布式计算能力，支持处理大量数据集。以下将详细介绍 SparkSQL 的关键特点、组成部分、基本用法以及一些示例代码。

## 1. SparkSQL 的特点

- **统一的数据处理**：既可以使用 DataFrame 和 Dataset API 进行数据操作，也可以用 SQL 语句直接查询数据。
- **与 Hive 集成**：支持访问 Hive 表和查询，能够与现有 Hive 生态系统无缝集成。
- **高效的查询执行**：通过使用 Catalyst 查询优化器，提供高效的查询执行计划。
- **与多种数据源兼容**：支持读取和写入多种数据源，包括 JSON、Parquet、ORC、JDBC、Kafka 等。
- **可扩展性**：支持分布式计算，可扩展到大规模数据集。

## 2. SparkSQL 的组成部分

### 2.1 DataFrame

- DataFrame 是 SparkSQL 中的核心数据结构，类似于 Pandas 的 DataFrame，具有行和列的结构，表格形式的数据表示。可以通过各种方式创建 DataFrame，例如从 RDD、JSON 文件、Hive 表等。

### 2.2 Dataset

- Dataset 是结合了 DataFrame 的优点与强类型的 API，属于 SparkSQL 的扩展。它提供了编译时类型检查，借此确保数据的类型安全性。

### 2.3 Catalyst Optimizer

- Catalyst 是 SparkSQL 的查询优化器，它对 SQL 查询进行分析、优化和编译，确保有效率高的执行计划。

### 2.4 Tungsten 执行引擎

- Tungsten 是 Spark 的一个执行引擎，针对内存管理、物理计划生成等方面进行了优化，能够提高 SparkSQL 的执行性能。

## 3. 基本用法

### 3.1 创建 SparkSession

要使用 SparkSQL，首先需要创建一个 `SparkSession` 对象，它是 Spark 应用程序的入口点。

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Spark SQL Example")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()
```

### 3.2 创建 DataFrame

可以从多种数据源创建 DataFrame。

```scala
// 从 JSON 文件创建 DataFrame
val jsonDF = spark.read.json("path/to/data.json")

// 从 CSV 文件创建 DataFrame
val csvDF = spark.read.option("header", "true").csv("path/to/data.csv")

// 从 RDD 创建 DataFrame
import spark.implicits._
val rdd = spark.sparkContext.parallelize(Seq(("Alice", 1), ("Bob", 2)))
val dfFromRDD = rdd.toDF("name", "id")
```

### 3.3 SQL 查询

在 DataFrame 上使用 SQL 查询，首先需将 DataFrame 注册为临时视图。

```scala
// 注册一个临时视图
jsonDF.createOrReplaceTempView("people")

// 使用 SQL 查询数据
val resultDF = spark.sql("SELECT * FROM people WHERE age > 21")
resultDF.show()
```

### 3.4 DataFrame 操作

可以使用 DataFrame API 对数据进行多种操作，包括选择、过滤、分组、聚合等。

```scala
// 选择特定列
val selectedDF = jsonDF.select("name", "age")

// 过滤数据
val filteredDF = jsonDF.filter($"age" > 21)

// 分组聚合
val groupByDF = jsonDF.groupBy("age").count()
```

### 3.5 写入数据

可以将处理后的 DataFrame 写入不同的数据源。

```scala
// 写入为 Parquet 格式
resultDF.write.parquet("path/to/output.parquet")

// 写入为 CSV 格式
resultDF.write.option("header", "true").csv("path/to/output.csv")
```

## 4. 示例代码

以下是一个完整的 SparkSQL 示例，涵盖从数据读取、处理到结果写入的全过程：

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Spark SQL Example")
  .getOrCreate()

// 1. 读取 JSON 数据
val jsonDF = spark.read.json("path/to/data.json")

// 2. 显示 DataFrame 内容
jsonDF.show()

// 3. 创建临时视图以便使用 SQL
jsonDF.createOrReplaceTempView("people")

// 4. 使用 SQL 查询
val resultDF = spark.sql("SELECT name, age FROM people WHERE age > 21")
resultDF.show()

// 5. 使用 DataFrame API 进行处理
val groupedDF = jsonDF.groupBy("age").count()
groupedDF.show()

// 6. 写入处理结果
groupedDF.write.parquet("path/to/output.parquet")

spark.stop()
```

## 5. 结论

SparkSQL 是处理结构化数据的一种灵活、高效的方式。它结合了 SQL 的易用性和 Spark 的高性能计算能力，非常适合大数据分析和实时数据处理。

# SparkSQl 环境搭建

## 来源

Spark + Hive => Shark => Spark On Hive => SparkSQL
Spark + Hive => Shark => Hive On Spark => Hive -> SQL -> RDD

Shark =>Spark On Hive=>SparkSQL=> Spark parse SQL

Shark =>HiveOnSpark=>数据仓库=>Hive parse SQL

## 环境搭建

没有JavaSparkSession，只有sparkSession

sparkSession底层使用的仍旧是Scala语言

推荐使用构建器模式构建SparkSQL环境对象，少用new

```java
import org.apache.spark.sql.SparkSession;  
  
public class SQL01_Env {  
    public static void main(String[] args) {  
        SparkSession sparkSession = SparkSession.builder().appName("SQL01_Env").master("local").getOrCreate();  
        System.out.println("Spark SQL 环境创建成功！");  
        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}
```

思考：close()和stop()有何区别？

## 模型

SparkSQL中对数据模型也进行了封装：RDD->Dataset

对接文件数据源时，会将文件中的一行数据封装为Row对象

```java
import org.apache.spark.rdd.RDD;  
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
public class SQL01_Model {  
    public static void main(String[] args) {  
        SparkSession sparkSession = SparkSession.builder().appName("SQL01_Model").master("local[2]").getOrCreate();  
        System.out.println("Spark SQL 环境创建成功！");  
  
        Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.json");  
        RDD<Row> rdd = dataset.rdd();  
          
        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}
```

## 解决报错`Job aborted due to stage failure`

```
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (TH executor driver): java.lang.NoClassDefFoundError: com/fasterxml/jackson/core/StreamReadConstraints
	......
```

```
Exception in thread "main" org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column
(named _corrupt_record by default).
```

https://stackoverflow.org.cn/questions/54517775

[java - 在 Apache Spark 中解析 JSON 时出现奇怪的错误](https://stackoverflow.org.cn/questions/54517775)

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQL03_SQL {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .appName("SQL01_Model")
                .master("local[2]")
                .getOrCreate();

        System.out.println("Spark SQL 环境创建成功！");

        // 读取 JSON 数据
        Dataset<Row> dataset = sparkSession.read().format("json").option("multiline", "true")
                .load("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.json");

        // 注册临时视图
        dataset.createOrReplaceTempView("users");

        // 执行 SQL 查询
        String sqlText = "SELECT * FROM users";
        Dataset<Row> result = sparkSession.sql(sqlText);

        // 展示结果
        result.show();

        // 停止 SparkSession
        sparkSession.stop();
        System.out.println("Spark SQL 环境已停止！");
    }
}
```

```
Spark SQL 环境创建成功！
+---+----+----+
| id|姓名|年龄|
+---+----+----+
|  1|张三|  25|
|  2|李四|  30|
|  3|王五|  22|
+---+----+----+

Spark SQL 环境已停止！
```

因为我的JSON格式是多行的，只需要改为一行即可

```
**{
  "name": "Michael",
  "age": 12
}
{
  "name": "Andy",
  "age": 13
}
{
  "name": "Justin",
  "age": 8
}**
```

修改为：

```
**{"name": "Michael",  "age": 12}
{"name": "Andy",  "age": 13}
{"name": "Justin",  "age": 8}
```

我这创建的user.json内容如下所示：

```json
[
  {
    "id": 1,
    "姓名": "张三",
    "年龄": 25
  },
  {
    "id": 2,
    "姓名": "李四",
    "年龄": 30
  },
  {
    "id": 3,
    "姓名": "王五",
    "年龄": 22
  }
]
```

读取单行

```java
Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");
```

```json
{"id":1,"姓名":"张三","年龄":25}  
{"id":2,"姓名":"李四","年龄":30}  
{"id":3,"姓名":"王五","年龄":22}
```

row背后是数组

```java
dataset.foreach(  
        row -> {  
            System.out.println(row.getLong(0) + ", " + row.getString(1) + ", " + row.getLong(2));  
        }  
);  
  
//row背后是数组
```

```
Spark SQL 环境创建成功！
1, 张三, 25
2, 李四, 30
3, 王五, 22
+---+----+----+
| id|姓名|年龄|
+---+----+----+
|  1|张三|  25|
|  2|李四|  30|
|  3|王五|  22|
+---+----+----+
```


# 不同场景下环境对象的转换


## 环境之间的转换

## Core:SparkContext-> SQL:SparkSession

```java
new SparkSession(new SparkContext(conf));
```

### SQL:SparkSession-> Core:SparkContext

```java
final SparkContext sparkContext = sparkSession.sparkContext();
sparkcontext.parallelize();
```

### SQL:SparkSession -> Core:JavaSparkContext

```java
final SparkContext sparkContext= sparkSession.sparkContext();
final JavaSparkContext jsc = new JavaSparkContext(sparkContext); 
jsc.parallelize（Arrays.asList(1,2,3,4));
```

## 不同场景下模型数据对象的转换

### RDD

```java
Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user.json");  
RDD<Row> rdd = dataset.rdd();
```
### DataFrame

读取单行

```java
Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");
```

```json
{"id":1,"姓名":"张三","年龄":25}  
{"id":2,"姓名":"李四","年龄":30}  
{"id":3,"姓名":"王五","年龄":22}
```

row背后是数组

```java
dataset.foreach(  
        row -> {  
            System.out.println(row.getLong(0) + ", " + row.getString(1) + ", " + row.getLong(2));  
        }  
);  
  
//row背后是数组
```

```
Spark SQL 环境创建成功！
1, 张三, 25
2, 李四, 30
3, 王五, 22
+---+----+----+
| id|姓名|年龄|
+---+----+----+
|  1|张三|  25|
|  2|李四|  30|
|  3|王五|  22|
+---+----+----+
```


### 自定义类型（代码暂时无法正常运行）

将数据模型中的数据类型进行转换，将Row转换成其他对象进行处理

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Encoders;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
import java.io.Serializable;  
  
public class SQL01_Model {  
    public static void main(String[] args) {  
        SparkSession sparkSession = SparkSession.builder().appName("SQL01_Model").master("local[2]").getOrCreate();  
        System.out.println("Spark SQL 环境创建成功！");  
  
        Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");  
  
        Dataset<User> userDataset = dataset.as(Encoders.bean(User.class));  
        userDataset.foreach(  
                user -> {  
                    System.out.println(user.getName() );  
                }  
        );  
  
        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}  
class User implements Serializable {  
    private String name;  
    private Long age;  
    private Long id;  
  
    public User(String name, Long age, Long id) {  
        this.name = name;  
        this.age = age;  
        this.id = id;  
    }  
    public User() {  
    }  
  
    public String getName() {  
        return name;  
    }  
  
    public void setName(String name) {  
        this.name = name;  
    }  
  
    public Long getAge() {  
        return age;  
    }  
  
    public void setAge(Long age) {  
        this.age = age;  
    }  
  
    public Long getId() {  
        return id;  
    }  
  
    public void setId(Long id) {  
        this.id = id;  
    }  
}
```


报错信息：

```
Caused by: java.util.concurrent.ExecutionException: org.codehaus.commons.compiler.CompileException: File 'generated.java', Line 41, Column 8: failed to compile: org.codehaus.commons.compiler.CompileException: File 'generated.java', Line 41, Column 8: "com.zzw.bigdata.spark.sparksql.User" is inaccessible from this package
	......

```

可能原因：User类不是public的。

具体实践中错误地方：

```java
userDataset.foreach(
                user -> {
                    System.out.println(user.getName() );
                }
        );
```

执行这个语句就会报错。


# 模型对象的访问

## 使用SQL语法

将数据模型转换为二维的结构（行，列），可以通过SQL文进行访问视图：是表的查询结果集。表可以增加，修改，删除，查询。

视图不能增加，不能修改，不能删除，只能查询

`ds.createOrReplaceTempView(viewName:"user");`

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Encoders;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
public class SQL01_Model_1 {  
    public static void main(String[] args) {  
        SparkSession sparkSession = SparkSession.builder().appName("SQL01_Model_1").master("local[2]").getOrCreate();  
        System.out.println("Spark SQL 环境创建成功！");  
  
        Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");  
  
        dataset.createOrReplaceTempView("user");  
  
        Dataset<Row> result = sparkSession.sql("SELECT * FROM user");  
  
        result.show();  
  
        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}
```

JDK1.8字符串不能跨行

## 使用DSL语法

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Encoders;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
  
public class SQL01_Model_1 {  
    public static void main(String[] args) {  
        SparkSession sparkSession = SparkSession.builder().appName("SQL01_Model_1").master("local[2]").getOrCreate();  
        System.out.println("Spark SQL 环境创建成功！");  
  
        Dataset<Row> dataset = sparkSession.read().json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");  
  
        dataset.select("*").show();  
  
        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}
```

## SQL文的缺陷（concat和+）

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQL03_SQL3 {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .appName("SQL01_Model3")
                .master("local[2]")
                .getOrCreate();

        System.out.println("Spark SQL 环境创建成功！");

        // 读取 JSON 数据
        Dataset<Row> dataset = sparkSession.read()
                .json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");

        // 注册临时视图
        dataset.createOrReplaceTempView("users");

        // 执行 SQL 查询
        String sqlText = "SELECT concat('first_name',name) AS full_name FROM users";
        Dataset<Row> result = sparkSession.sql(sqlText);

        // 展示结果
        result.show();

        // 停止 SparkSession
        sparkSession.stop();
        System.out.println("Spark SQL 环境已停止！");
    }
}
```

concat不通用 

```java
String sql="select concat('Name:',name) from user";
//String sql="select'Name:'Ilname from user";//mysql，oracle(ll)，db2，sqlserver

final Dataset<Row> sqlDS = sparkSession.sql(sql); sqLDS.show();
```

# 自定义方法（UDF和UDAF）

SparkSQL提供了一种特殊的方式，可以在SQL中增加自定义方法来实现复杂的逻辑

## UDF

如果想要自定义的方法能够在SQL中使用，那么必须在SPark中进行声明和注册

register方法需要传递3个参数

第一个参数表示SQL中使用的方法名

第二个参数表示逻辑：IN=>OUT

第三个参数表示返回的数据类型：DataType类型数据，需要使用scala语法操作，需要特殊的使用方式。

```java
sparkSession.udf().register(name:"prefixName",new UDF1<String, String>() {
@Override
public String call(String name) throws Exception {
return "Name:"+ name;}
}，StringType$.MODULE$);

String sql="select prefixName(name) from user"; 
final Dataset<Row> sqlDS = sparkSession.sql(sql); 
sqLDS.show();
```

```java
import org.apache.spark.sql.Dataset;  
import org.apache.spark.sql.Row;  
import org.apache.spark.sql.SparkSession;  
import org.apache.spark.sql.types.StringType$;  
  
public class SQL03_SQL3 {  
    public static void main(String[] args) {  
        // 创建SparkSession  
        SparkSession sparkSession = SparkSession.builder()  
                .appName("SQL01_Model3")  
                .master("local[2]")  
                .getOrCreate();  
  
        System.out.println("Spark SQL 环境创建成功！");  
  
        // 读取 JSON 数据  
        Dataset<Row> dataset = sparkSession.read()  
                .json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");  
  
        // 注册临时视图  
        dataset.createOrReplaceTempView("users");  
  
        sparkSession.udf().register("prefixName", (String name) -> "first_name" + name, StringType$.MODULE$);  
  
        // 执行 SQL 查询  
        String sqlText = "SELECT prefixName(name) AS full_name FROM users";  
        Dataset<Row> result = sparkSession.sql(sqlText);  
  
        // 展示结果  
        result.show();  
  
        // 停止 SparkSession        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}
```

输出结果如下所示：

```
Spark SQL 环境创建成功！
+--------------+
|     full_name|
+--------------+
|first_name张三|
|first_name李四|
|first_name王五|
+--------------+

Spark SQL 环境已停止！
```

`sparkSession.udf().register("prefixName", (String name) -> "first_name" + name, StringType$.MODULE$); `另一种写法`sparkSession.udf().register("prefixName", (String name) -> "first_name" + name, DataTypes.StringType);`

也可以静态导入

## UDAF原理和自定义实现

每行数据都用一次UDF函数，类似map

所有数据共用一次UDAF函数，类似reduce

UDAF函数底层实现中需要存在一个缓冲区，用于临时存放数据

SparkSQL采用特殊的方式将UDAF转换成UDF使用

UDAF使用时需要创建自定义聚合对象 udaf方法需要传递2个参数

第一个参数表示UDAF对象

第二个参数表示UDAF对象

```java
sparkSession.udf().register(name:"avgAge",functions.udaf(
new MyAvgAgeUDAF()，Encoders.LoNG()
);
String sql ="select avgAge(age) from user";
final Dataset<Row> sqlDS = sparkSession.sql(sql);
sqLDS.show();
```

自定义UDAF函数，实现年龄的平均值

1.创建自定义的类

2.继承 `org.apache.spark.sql.expressions.Aggregator`

3．设定泛型

IN：输入数据类型

BUF：缓冲区的数据类型 

OUT：输出数据类型

4.重写方法（4（计算）+2（状态））

这里需要使用`import static org.apache.spark.sql.functions.udaf;`

文件写在一块会报错`Caused by: java.lang.IllegalAccessException: Class org.apache.spark.sql.catalyst.expressions.objects.InitializeJavaBean can not access a member of class com.zzw.bigdata.spark.sparksql.AvgAgeBuffer with modifiers "public`

```java
import org.apache.spark.sql.*;  
  
import static org.apache.spark.sql.functions.udaf;  
  
public class SQL03_SQL_UDAF {  
    public static void main(String[] args) {  
        // 创建SparkSession  
        SparkSession sparkSession = SparkSession.builder()  
                .appName("SQL01_Model_API")  
                .master("local[2]")  
                .getOrCreate();  
  
        System.out.println("Spark SQL 环境创建成功！");  
  
        // 读取 JSON 数据  
        Dataset<Row> dataset = sparkSession.read()  
                .json("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user2.json");  
  
        // 注册临时视图  
        dataset.createOrReplaceTempView("users");  
  
        sparkSession.udf().register("avg_age", udaf(new MyAvgAgeUDAF(), Encoders.LONG()));  
  
        // 执行 SQL 查询  
        String sqlText = "SELECT avg_age(age) AS avg_age FROM users";  
        Dataset<Row> result = sparkSession.sql(sqlText);  
  
        // 展示结果  
        result.show();  
  
        // 停止 SparkSession        sparkSession.stop();  
        System.out.println("Spark SQL 环境已停止！");  
    }  
}
```

```java
import org.apache.spark.sql.Encoder;  
import org.apache.spark.sql.Encoders;  
import org.apache.spark.sql.expressions.Aggregator;  
  
public class MyAvgAgeUDAF extends Aggregator<Long, AvgAgeBuffer, Long> {  
    @Override  
    //初始化缓冲区  
    public AvgAgeBuffer zero() {  
        return new AvgAgeBuffer(0L, 0L);  
    }  
  
    @Override  
    //聚合输入的年龄和缓冲区中的数据，更新缓冲区  
    public AvgAgeBuffer reduce(AvgAgeBuffer buffer, Long value) {  
        buffer.setSum(buffer.getSum() + value);  
        buffer.setCount(buffer.getCount() + 1);  
        return buffer;  
    }  
  
    @Override  
    //合并两个缓冲区，将两个缓冲区中的数据合并到一起  
    public AvgAgeBuffer merge(AvgAgeBuffer buffer1, AvgAgeBuffer buffer2) {  
        buffer1.setSum(buffer1.getSum() + buffer2.getSum());  
        buffer1.setCount(buffer1.getCount() + buffer2.getCount());  
        return buffer1;  
    }  
  
    @Override  
    //计算最终结果  
    public Long finish(AvgAgeBuffer buffer) {  
        return buffer.getSum()/buffer.getCount();  
    }  
    @Override  
    public Encoder<AvgAgeBuffer> bufferEncoder() {  
        return Encoders.bean(AvgAgeBuffer.class);  
    }  
  
    @Override  
    public Encoder<Long> outputEncoder() {  
        return Encoders.LONG();  
    }  
}
```

```java
import java.io.Serializable;  
  
public class AvgAgeBuffer  implements Serializable {  
    private long sum = 0;  
    private long count = 0;  
  
    public AvgAgeBuffer() {  
    }  
  
    public AvgAgeBuffer(long sum, long count) {  
        this.sum = sum;  
        this.count = count;  
    }  
  
    public long getSum() {  
        return sum;  
    }  
  
    public void setSum(long sum) {  
        this.sum = sum;  
    }  
  
    public long getCount() {  
        return count;  
    }  
  
    public void setCount(long count) {  
        this.count = count;  
    }  
}
```





