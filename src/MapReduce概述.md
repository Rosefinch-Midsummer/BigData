# MapReduce 概述

<!-- toc -->

## MapReduce 定义

MapReduce 是一个分布式运算程序的编程框架，是用户开发 “基于 Hadoop 的数据分析应用” 的核心框架。

MapReduce 核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序，并发运行在一个 Hadoop 集群上。

## MapReduce 优缺点

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102144435.png)

### 优点

1 ）MapReduce 易于编程

它简单的实现一些接口， 就可以完成一个分布式程序， 这个分布式程序可以分布到大量廉价的 PC 机器上运行。也就是说你写一个分布式程序，跟写一个简单的串行程序是一模一样的。就是因为这个特点使得 MapReduce 编程变得非常流行。

2 ） 良好的扩展性

当你的计算资源不能得到满足的时候， 你可以通过简单的增加机器来扩展它的计算能力。

3 ） 高容错性

MapReduce 设计的初衷就是使程序能够部署在廉价的 PC 机器上， 这就要求它具有很高的容错性。比如其中一台机器挂了，它可以把上面的计算任务转移到另外一个节点上运行，不至于这个任务运行失败， 而且这个过程不需要人工参与， 而完全是由 Hadoop 内部完成的。

4 ） 适合 PB 级以上海量数据的离线处理

可以实现上千台服务器集群并发工作，提供数据处理能力。

### 缺点

1 ） 不擅长实时计算

MapReduce 无法像 MySQL 一样，在毫秒或者秒级内返回结果。

2 ） 不擅长流式计算

流式计算的输入数据是动态的， 而 MapReduce 的输入数据集是静态的， 不能动态变化。
这是因为 MapReduce 自身的设计特点决定了数据源必须是静态的。

3 ） 不擅长 DAG （有向无环图）计算

多个应用程序存在依赖关系，后一个应用程序的输入为前一个的输出。在这种情况下，MapReduce 并不是不能做， 而是使用后， 每个 MapReduce 作业的输出结果都会写入到磁盘，会造成大量的磁盘 IO，导致性能非常的低下。

## MapReduce 核心编程思想

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102144500.png)

（1）分布式的运算程序往往需要分成至少 2 个阶段。

（2）第一个阶段的 MapTask 并发实例，完全并行运行，互不相干。

（3）第二个阶段的 ReduceTask 并发实例互不相干，但是他们的数据依赖于上一个阶段的所有 MapTask 并发实例的输出。

（4）MapReduce 编程模型只能包含一个 Map 阶段和一个 Reduce 阶段，如果用户的业务逻辑非常复杂，那就只能多个 MapReduce 程序，串行运行。

总结：分析 WordCount 数据流走向深入理解 MapReduce 核心思想。

## MapReduce 进程

任务、job、MR 都表示任务。

一个完整的 MapReduce 程序在分布式运行时有三类实例进程：

（1）MrAppMaster：负责整个程序的过程调度及状态协调。

（2）MapTask：负责 Map 阶段的整个数据处理流程。

（3）ReduceTask：负责 Reduce 阶段的整个数据处理流程。

## 官方 WordCount 源码

采用反编译工具反编译源码，发现 WordCount 案例有 Map 类、Reduce 类和驱动类，且数据的类型是 Hadoop 自身封装的序列化类型。

如何查看里面的代码程序呢？使用反编译工具 jd-gui

[java 反编译工具 jd-gui 下载与使用](https://blog.csdn.net/zlbdmm/article/details/104653823)

查看 `hadoop-mapreduce-examples-3.3.6.jar` 得到如下的源代码：

```java
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable (1);
    private Text word = new Text ();
      
    public void map (Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer (value.toString ());
      while (itr.hasMoreTokens ()) {
        word.set (itr.nextToken ());
        context.write (word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable ();

    public void reduce (Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get ();
      }
      result.set (sum);
      context.write (key, result);
    }
  }

  public static void main (String [] args) throws Exception {
    Configuration conf = new Configuration ();
    String [] otherArgs = new GenericOptionsParser (conf, args).getRemainingArgs ();
    if (otherArgs.length < 2) {
      System.err.println ("Usage: wordcount <in> [<in>...] <out>");
      System.exit (2);
    }
    Job job = Job.getInstance (conf, "word count");
    job.setJarByClass (WordCount.class);
    job.setMapperClass (TokenizerMapper.class);
    job.setCombinerClass (IntSumReducer.class);
    job.setReducerClass (IntSumReducer.class);
    job.setOutputKeyClass (Text.class);
    job.setOutputValueClass (IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath (job, new Path (otherArgs [i]));
    }
    FileOutputFormat.setOutputPath (job,
      new Path (otherArgs [otherArgs.length - 1]));
    System.exit (job.waitForCompletion (true) ? 0 : 1);
  }
}

```
## 常用数据序列化类型

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102145930.png)

| Java 类型  | Hadoop Writable 类型 |
| ------- | ----------------- |
| Boolean | BooleanWritable   |
| Byte    | ByteWritable      |
| Int     | IntWritable       |
| Float   | FloatWritable     |
| Long    | LongWritable      |
| Double  | DoubleWritable    |
| String  | Text              |
| Map     | MapWritable       |
| Array   | ArrayWritable     |
| Null    | NullWritable      |
## MapReduce 编程规范

用户编写的程序分成三个部分：Mapper、Reducer 和 Driver。

1．Mapper 阶段

（1）用户自定义的 Mapper 要继承自己的父类

```java
public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{}
```

（2）Mapper 的输入数据是 KV 对的形式（KV 的类型可自定义）

p.s. K 是这一行的首字符偏移量，V 是这一行的内容。

（3）Mapper 中的业务逻辑写在 map () 方法中

```java
public void map (Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer (value.toString ());
      while (itr.hasMoreTokens ()) {
        word.set (itr.nextToken ());
        context.write (word, one);
      }
    }
```

（4）Mapper 的输出数据是 KV 对的形式（KV 的类型可自定义）

（5）map () 方法（MapTask 进程）对每一个 < K,V > 调用一次

2．Reducer 阶段

（1）用户自定义的 Reducer 要继承自己的父类

```java
public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable>{}
```

（2）Reducer 的输入数据类型对应 Mapper 的输出数据类型，也是 KV

（3）Reducer 的业务逻辑写在 reduce () 方法中

```java
public void reduce (Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get ();
      }
      result.set (sum);
      context.write (key, result);
    }
```

（4）ReduceTask 进程对每一组相同 k 的 < k,v > 组调用一次 reduce () 方法

3．Driver 阶段

相当于 YARN 集群的客户端，用于提交我们整个程序到 YARN 集群，提交的是封装了 MapReduce 程序相关运行参数的 job 对象

## WordCount 案例实操

### 本地测试

1 ） 需求

在给定的文本文件中统计输出每一个单词出现的总次数，输出结果默认按照 utf-8 编码顺序排列

（1）输入数据

创建一个文件并写入想要测试的数据，例如：

```
Avengers Avengers
DC DC
Mavel Mavel
Iron_Man
Captain_America
Thor
Hulk
Black_Widow
Hawkeye
Black_Panther
Spider_Man
Doctor_Strange
Ant_Man
Vision
Scarlet_Witch
Winter_Soldier
Loki
Star_Lord
Gamora
Rocket_Raccoon
Groot
```

（2）期望输出数据（涉及输入的排序问题）

```
Ant_Man	1
Avengers	2
Black_Panther	1
Black_Widow	1
Captain_America	1
DC	2
Doctor_Strange	1
Gamora	1
Groot	1
Hawkeye	1
Hulk	1
Iron_Man	1
Loki	1
Mavel	2
Rocket_Raccoon	1
Scarlet_Witch	1
Spider_Man	1
Star_Lord	1
Thor	1
Vision	1
Winter_Soldier	1
```

2 ） 需求分析

按照 MapReduce 编程规范，分别编写 Mapper、Reducer、Driver 类。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102150832.png)

3 ） 环境准备

（1）创建 maven 工程 MapReduceDemo

（2）在 pom.xml 文件中添加版本信息以及相关依赖

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.TianHan</groupId>
    <artifactId>wordcount</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.3.6</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-api</artifactId>
            <version>5.11.1</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>2.0.16</version>
        </dependency>
    </dependencies>

</project>
```

（2）在项目的 src/main/resources 目录下，新建一个文件，命名为 “log4j.properties” 用于打印相关日志。在该文件中填入：

```
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=% d % p [% c] - % m% n
log4j.appender.logfile=org.apache.log4j.FileAppender
log4j.appender.logfile.File=target/spring.log
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout
log4j.appender.logfile.layout.ConversionPattern=% d % p [% c] - % m% n
```

（3）创建包 mapreduce.wordcount 然后创建三个类

4 ） 编写程序

（1）编写 Mapper 类

```java
package com.TianHan.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * KEYIN, map 阶段输入的 key 的类型：LongWritable
 * VALUEIN,map 阶段输入 value 类型：Text
 * KEYOUT,map 阶段输出的 Key 类型：Text
 * VALUEOUT,map 阶段输出的 value 类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text ();
    private IntWritable outV = new IntWritable (1);  //map 阶段不进行聚合

    @Override
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        //xxxxxx xxxxxx
        String line = value.toString ();

        // 2 切割 (取决于原始数据的中间分隔符)
        //xxxxxxx
        //xxxxxxx
        String [] words = line.split (" ");

        // 3 循环写出
        for (String word : words) {
            // 封装 outk
            outK.set (word);

            // 写出
            context.write (outK, outV);
        }
    }
}
```


（2）编写 Reducer 类

```java
package com.TianHan.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, reduce 阶段输入的 key 的类型：Text
 * VALUEIN,reduce 阶段输入 value 类型：IntWritable
 * KEYOUT,reduce 阶段输出的 Key 类型：Text
 * VALUEOUT,reduce 阶段输出的 value 类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable ();

    @Override
    protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        //xxxxxxx xxxxxxx ->(xxxxxxx,1),(xxxxxxx,1)
        //xxxxxxx, (1,1)
        // 将 values 进行累加
        for (IntWritable value : values) {
            sum += value.get ();
        }

        outV.set (sum);

        // 写出
        context.write (key,outV);
    }
}
```


（3）编写 Driver 驱动类

```java
package com.TianHan.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取 job
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);

        // 2 设置 jar 包路径
        job.setJarByClass (WordCountDriver.class);

        // 3 关联 mapper 和 reducer
        job.setMapperClass (WordCountMapper.class);
        job.setReducerClass (WordCountReducer.class);

        // 4 设置 map 输出的 kv 类型
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (IntWritable.class);

        // 5 设置最终输出的 kV 类型
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (IntWritable.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths (job, new Path ("E:\\BigData\\hadoop\\input"));
        FileOutputFormat.setOutputPath (job, new Path ("E:\\BigData\\hadoop\\output"));

        // 7 提交 job
        boolean result = job.waitForCompletion (true);

        System.exit (result ? 0 : 1);
    }
}
```

5 ） 本地测试

（1）由于这里通过 Maven 安装了 hadoop-client，所以不需要配置 HADOOP_HOME 变量以及 Windows 运行依赖即可成功运行程序。

（2）在 IDEA 上运行程序

注意：此时如果再运行一遍，会报错。在 mapreduce 中，如果输出路径存在会报错。

### WordCount 案例 Debug 调试

在以下几个地方打好断点：Mapper 类中 map 函数第一行、开始 setup、结束 cleanup

通过调试可以更清楚的理解机制。至少三遍。

### 提交到集群测试

刚刚上面的代码是在本地运行的，是通过下载了 hadoop 相关的依赖，运用本地模式运行的。

我们还需要把程序推送到生产环境（Linux 环境）中。

（1）用 Maven 打包，需要添加的打包插件依赖

将下面的代码放在之前配置的依赖后面（对应 pom.xml 文件）

```xml
<build> 
    <plugins> 
        <plugin> 
            <artifactId>maven-compiler-plugin</artifactId> 
            <version>3.6.1</version> 
            <configuration> 
                <source>1.8</source> 
                <target>1.8</target> 
            </configuration> 
        </plugin> 
        <plugin> 
            <artifactId>maven-assembly-plugin</artifactId> 
            <configuration> 
                <descriptorRefs> 
                    <descriptorRef>jar-with-dependencies</descriptorRef> 
                </descriptorRefs> 
            </configuration> 
            <executions> 
                <execution> 
                    <id>make-assembly</id> 
                    <phase>package</phase> 
                    <goals> 
                        <goal>single</goal> 
                    </goals> 
                </execution> 
            </executions> 
        </plugin> 
    </plugins> 
</build> 
```

`maven-compiler-plugin`：：打包但不带有所需 jar 包，jar 包小

`maven-assembly-plugin`：打包并带有所需 jar 包，jar 包大

这需要根据需求使用。

（2）将程序打成 jar 包

打包完毕，生成 jar 包，去文件夹里查看一下

（3）修改不带依赖的 jar 包名称为 wc.jar，并拷贝该 jar 包到 Hadoop 集群的 /opt/module/hadoop-3.3.6 路径

思考：刚刚的程序中，我们写的路径是本地 Windows 系统中的路径，上传到 Linux 环境后它其实没有这个路径，输入输出路径不存在，于是我们需要对它进行修改，改成对应的集群路径。

如果想更灵活一点 —— 根据传入的路径来确定输入的路径，需要使用 `args [0]` 和 `args [1]`

我们再创建一个 wordcount2 包，跟 wordcount 内容一致，就将输入输出路径修改了一下。

对于新改的程序，先点 clean 把前面的删掉，再点 package 进行打包。

将新的包按上面的操作更名 wc.jar

根据命令行设定输入输出路径，使用 `args [0]，args [1]`。

```java
// 6 设置输入路径和输出路径
FileInputFormat.setInputPaths (job, new Path (args [0]));
FileOutputFormat.setOutputPath (job, new Path (args [1]));
```

（4）启动 Hadoop 集群

```
start-dfs.sh
```

先在 HDFS 集群中设置刚刚要处理的源文件：

在集群中建一个 Marvel 文件夹，然后在文件夹中上传我们之前要处理的 Marvel.txt 源文件

（5）执行 WordCount 程序

生成 jar 包，导入 jar 包到集群，再重新运行程序

```
hadoop jar wc.jar com.TianHan.mapreduce.wordcount.WordCountDriver mapreduce/input mapreduce/output
```
