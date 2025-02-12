# MapReduce框架原理

<!-- toc -->

​![image-20221106213218443](https://image.3001.net/images/20221106/1667741555906.png)

## 3.1 InputFormat 数据输入

### 3.1.1 切片与 MapTask 并行度决定机制

**1**）问题引出

​MapTask 的并行度决定 Map 阶段的任务处理并发度，进而影响到整个 Job 的处理速度。

​思考：1G 的数据，启动 8 个 MapTask，可以提高集群的并发处理能力。那么 1K 的数据，也启动 8 个 MapTask，会提高集群性能吗？MapTask 并行任务是否越多越好呢？哪些因素影响了 MapTask 并行度？

**2**）MapTask 并行度决定机制

​ **数据块：** Block 是 HDFS 物理上把数据分成一块一块。数据块是 HDFS 存储数据单位。

​ **数据切片：** 数据切片只是在逻辑上对输入进行分片，并不会在磁盘上将其切分成片进行存储。数据切片是 MapReduce 程序计算输入数据的单位，一个切片会对应启动一个 MapTask。

![image-20221106213259161](https://image.3001.net/images/20221106/16677415962785.png)

### 3.1.2 Job 提交流程源码和切片源码详解

源码阅读三大要点：job.xml、xxx.jar、job.split

1）Job 提交流程源码详解

```java
waitForCompletion ()

submit ();

// 1 建立连接
connect ();	
	// 1）创建提交 Job 的代理
	new Cluster (getConfiguration ());
		// （1）判断是本地运行环境还是 yarn 集群运行环境
		initialize (jobTrackAddr, conf); 

	// 2 提交 job
	submitter.submitJobInternal (Job.this, cluster)

// 1）创建给集群提交数据的 Stag 路径
Path jobStagingArea = JobSubmissionFiles.getStagingDir (cluster, conf);

// 2）获取 jobid ，并创建 Job 路径
JobID jobId = submitClient.getNewJobID ();

// 3）拷贝 jar 包到集群
copyAndConfigureFiles (job, submitJobDir);	
rUploader.uploadFiles (job, jobSubmitDir);

// 4）计算切片，生成切片规划文件
writeSplits (job, submitJobDir);
	maps = writeNewSplits (job, jobSubmitDir);
	input.getSplits (job);

// 5）向 Stag 路径写 XML 配置文件
writeConf (conf, submitJobFile);
conf.writeXml (out);

// 6）提交 Job, 返回提交状态
status = submitClient.submitJob (jobId, submitJobDir.toString (), job.getCredentials ());

```

![image-20221106213339571](https://image.3001.net/images/20221106/16677416364405.png)

**2**）FileInputFormat 切片源码解析（input.getSplits (job)）

![image-20221106213352169](https://image.3001.net/images/20221106/16677416493309.png)

### 3.1.3 FileInputFormat 切片机制

![image-20221106213447314](https://image.3001.net/images/20221106/16677417044403.png)

![image-20221106213514220](https://image.3001.net/images/20221106/16677417312383.png)

### 3.1.4 TextInputFormat

**1**）FileInputFormat 实现类

​思考：在运行 MapReduce 程序时，输入的文件格式包括：基于行的日志文件、二进制格式文件、数据库表等。那么，针对不同的数据类型，MapReduce 是如何读取这些数据的呢？

​FileInputFormat 常见的接口实现类包括：**TextInputFormat**、KeyValueTextInputFormat、NLineInputFormat、**CombineTextInputFormat**和自定义 InputFormat 等。

**2**）TextInputFormat

​TextInputFormat 是默认的 FileInputFormat 实现类。按行读取每条记录。键是存储该行在整个文件中的起始字节偏移量， LongWritable 类型。值是这行的内容，不包括任何行终止符（换行符和回车符），Text 类型。

​以下是一个示例，比如，一个分片包含了如下 4 条文本记录。

```
Rich learning form
Intelligent learning engine
Learning more convenient
From the real demand for more close to the enterprise
```

每条记录表示为以下键 / 值对：

```
(0,Rich learning form)
(20,Intelligent learning engine)
(49,Learning more convenient)
(74,From the real demand for more close to the enterprise)
```
### 3.1.5 CombineTextInputFormat 切片机制

框架默认的 TextInputFormat 切片机制是对任务按文件规划切片，不管文件多小，都会是一个单独的切片，都会交给一个 MapTask，这样如果有大量小文件，就会产生大量的 MapTask，处理效率极其低下。

**1**）应用场景：

​`CombineTextInputFormat` 用于小文件过多的场景，它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个 MapTask 处理。

**2**）虚拟存储切片最大值设置

`​CombineTextInputFormat.setMaxInputSplitSize (job, 4194304);// 4m`

注意：虚拟存储切片最大值设置最好根据实际的小文件大小情况来设置具体的值。

**3**）切片机制

生成切片过程包括：虚拟存储过程和切片过程二部分。

![image-20221110125959434](https://image.3001.net/images/20221110/1668056403686.png)

（1）虚拟存储过程：

​ 将输入目录下所有文件大小，依次和设置的 setMaxInputSplitSize 值比较，如果不大于设置的最大值，逻辑上划分一个块。如果输入文件大于设置的最大值且大于两倍，那么以最大值切割一块；**当剩余数据大小超过设置的最大值且不大于最大值 2 倍，此时将文件均分成 2 个虚拟存储块（防止出现太小切片）。**

​ 例如 setMaxInputSplitSize 值为 4M，输入文件大小为 8.02M，则先逻辑上分成一个 4M。剩余的大小为 4.02M，如果按照 4M 逻辑划分，就会出现 0.02M 的小的虚拟存储文件，所以将剩余的 4.02M 文件切分成（2.01M 和 2.01M）两个文件。

（2）切片过程：

（a）判断虚拟存储的文件大小是否大于 setMaxInputSplitSize 值，大于等于则单独形成一个切片。

（b）如果不大于则跟下一个虚拟存储文件进行合并，共同形成一个切片。

（c）测试举例：有 4 个小文件大小分别为 1.7M、5.1M、3.4M 以及 6.8M 这四个小文件，则虚拟存储之后形成 6 个文件块，大小分别为：

1.7M，（2.55M、2.55M），3.4M 以及（3.4M、3.4M）

最终会形成 3 个切片，大小分别为：

（1.7+2.55）M，（2.55+3.4）M，（3.4+3.4）M

### 3.1.6 CombineTextInputFormat 案例实操

**1**）需求

将输入的大量小文件合并成一个切片统一处理。

（1）输入数据

准备 4 个小文件

（2）期望

期望一个切片处理 4 个文件

**2**）实现过程

（1）不做任何处理，运行 1.8 节的 WordCount 案例程序，观察切片个数为 4。

```
number of splits:4
```

（2）在 WordcountDriver 中增加如下代码，运行程序，并观察运行的切片个数为 3。

（a）驱动类中添加代码如下：

```java
// 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
job.setInputFormatClass (CombineTextInputFormat.class);

// 虚拟存储切片最大值设置 4m
CombineTextInputFormat.setMaxInputSplitSize (job, 4194304);
```

（b）运行如果为 3 个切片。

```
number of splits:3
```

（3）在 WordcountDriver 中增加如下代码，运行程序，并观察运行的切片个数为 1。

（a）驱动中添加代码如下：

```java
// 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
job.setInputFormatClass (CombineTextInputFormat.class);

// 虚拟存储切片最大值设置 20m
CombineTextInputFormat.setMaxInputSplitSize (job, 20971520);
```

（b）运行如果为 1 个切片

```
number of splits:1
```

## 3.2 MapReduce 工作流程

![image-20221110130426443](https://image.3001.net/images/20221110/16680566693739.png)

![image-20221110130436072](https://image.3001.net/images/20221110/16680566799674.png)

​上面的流程是整个 MapReduce 最全工作流程，但是 Shuffle 过程只是从第 7 步开始到第 16 步结束，具体 Shuffle 过程详解，如下：

（1）MapTask 收集我们的 map () 方法输出的 kv 对，放到内存缓冲区中

（2）从内存缓冲区不断溢出本地磁盘文件，可能会溢出多个文件

（3）多个溢出文件会被合并成大的溢出文件

（4）在溢出过程及合并的过程中，都要调用 Partitioner 进行分区和针对 key 进行排序

（5）ReduceTask 根据自己的分区号，去各个 MapTask 机器上取相应的结果分区数据

（6）ReduceTask 会抓取到同一个分区的来自不同 MapTask 的结果文件，ReduceTask 会将这些文件再进行合并（归并排序）

（7）合并成大文件后，Shuffle 的过程也就结束了，后面进入 ReduceTask 的逻辑运算过程（从文件中取出一个一个的键值对 Group，调用用户自定义的 reduce () 方法）

**注意：**

（1）Shuffle 中的缓冲区大小会影响到 MapReduce 程序的执行效率，原则上说，缓冲区越大，磁盘 IO 的次数越少，执行速度就越快。

（2）缓冲区的大小可以通过参数调整，参数：mapreduce.task.io.sort.mb 默认 100M。

## 3.3 Shuffle 机制

### 3.3.1 Shuffle 机制

Map 方法之后，Reduce 方法之前的数据处理过程称之为 Shuffle。

![image-20221110130507683](https://image.3001.net/images/20221110/1668056711326.png)

对 key 的索引按照字典顺序快速排序

分组格式：{key，（value1， value2， ……）}
### 3.3.2 Partition 分区

![image-20221110130520540](https://image.3001.net/images/20221110/16680567231755.png)

在 Driver 中添加 `job.setNumReduceTasks (2);`，然后 DeBug 逐步查找下面的代码：

```java
public void write (K key, V value) throws IOException, InterruptedException {  
    this.collector.collect (key, value, this.partitioner.getPartition (key, value, this.partitions));  
}
```

```java
//  
// Source code recreated from a .class file by IntelliJ IDEA  
// (powered by FernFlower decompiler)  
//  
  
package org.apache.hadoop.mapreduce.lib.partition;  
  
import org.apache.hadoop.classification.InterfaceAudience.Public;  
import org.apache.hadoop.classification.InterfaceStability.Stable;  
import org.apache.hadoop.mapreduce.Partitioner;  
  
@Public  
@Stable  
public class HashPartitioner<K, V> extends Partitioner<K, V> {  
    public HashPartitioner () {  
    }  
  
    public int getPartition (K key, V value, int numReduceTasks) {  
        return (key.hashCode () & Integer.MAX_VALUE) % numReduceTasks;  
    }  
}
```

在 Driver 中不设置 `job.setNumReduceTasks (2);`，然后 DeBug 逐步查找下面的代码：

```java
NewOutputCollector (JobContext jobContext, JobConf job, TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter) throws IOException, ClassNotFoundException {  
    this.collector = MapTask.this.createSortingCollector (job, reporter);  
    this.partitions = jobContext.getNumReduceTasks ();  
    if (this.partitions > 1) {  
        this.partitioner = (Partitioner) ReflectionUtils.newInstance (jobContext.getPartitionerClass (), job);  
    } else {  
        this.partitioner = new Partitioner<K, V>() {  
            public int getPartition (K key, V value, int numPartitions) {  
                return NewOutputCollector.this.partitions - 1;  
            }  
        };  
    }  
  
}  
  
public void write (K key, V value) throws IOException, InterruptedException {  
    this.collector.collect (key, value, this.partitioner.getPartition (key, value, this.partitions));  
}
```

`new Partitioner<K, V>(){}` 匿名内部类，`job.setNumReduceTasks` 默认是 1，只生成一个 0 号分区

![image-20221110130528974](https://image.3001.net/images/20221110/16680567322654.png)

![image-20221110130542610](https://image.3001.net/images/20221110/16680567452392.png)

### 3.3.3 Partition 分区案例实操

**1**）需求

将统计结果按照手机归属地不同省份输出到不同文件中（分区）

（1）输入数据

​`phone_data.txt`

```
1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
2	13846544121	192.196.100.2			264	0	200
3 	13956435636	192.196.100.3			132	1512	200
4 	13966251146	192.168.100.1			240	0	404
5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
7 	13590439668	192.168.100.4			1116	954	200
8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
9 	13729199489	192.168.100.6			240	0	200
10 	13630577991	192.168.100.7	www.shouhu.com	6960	690	200
11 	15043685818	192.168.100.8	www.baidu.com	3659	3538	200
12 	15959002129	192.168.100.9	www.atguigu.com	1938	180	500
13 	13560439638	192.168.100.10			918	4938	200
14 	13470253144	192.168.100.11			180	180	200
15 	13682846555	192.168.100.12	www.qq.com	1938	2910	200
16 	13992314666	192.168.100.13	www.gaga.com	3008	3720	200
17 	13509468723	192.168.100.14	www.qinghua.com	7335	110349	404
18 	18390173782	192.168.100.15	www.sogou.com	9531	2412	200
19 	13975057813	192.168.100.16	www.baidu.com	11058	48243	200
20 	13768778790	192.168.100.17			120	120	200
21 	13568436656	192.168.100.18	www.alibaba.com	2481	24681	200
22 	13568436656	192.168.100.19			1116	954	200
```

（2）期望输出数据

​ 手机号 136、137、138、139 开头都分别放到一个独立的 4 个文件中，其他开头的放到一个文件中。

**2**）需求分析

![image-20221110130647783](https://image.3001.net/images/20221110/1668056811486.png)

**3**）在案例**2.3**的基础上，增加一个分区类

```java
package com.TianHan.mapreduce.partitioner;  
  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Partitioner;  
  
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {  
  
    @Override  
    public int getPartition (Text text, FlowBean flowBean, int numPartitions) {  
        // 获取手机号前三位 prePhone  
        String phone = text.toString ();  
        String prePhone = phone.substring (0, 3);  
  
        // 定义一个分区号变量 partition, 根据 prePhone 设置分区号  
        int partition;  
  
        if ("136".equals (prePhone)){  
            partition = 0;  
        } else if ("137".equals (prePhone)){  
            partition = 1;  
        } else if ("138".equals (prePhone)){  
            partition = 2;  
        } else if ("139".equals (prePhone)){  
            partition = 3;  
        } else {  
            partition = 4;  
        }  
  
        // 最后返回分区号 partition  
        return partition;  
    }  
}
```

改进版写法：

```java
package com.TianHan.mapreduce.partitioner2;  
  
import com.TianHan.mapreduce.partitioner2.FlowBean;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Partitioner;  
  
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {  
  
    @Override  
    public int getPartition (Text text, FlowBean flowBean, int numPartitions) {  
        // 获取手机号前三位 prePhone  
        String phone = text.toString ();  
        String prePhone = phone.substring (0, 3);  
  
        // 定义一个分区号变量 partition, 根据 prePhone 设置分区号  
  
        // 最后返回分区号 partition  
        return switch (prePhone) {  
            case "136" -> 0;  
            case "137" -> 1;  
            case "138" -> 2;  
            case "139" -> 3;  
            default -> 4;  
        };  
    }  
}
```

**4**）在驱动函数中增加自定义数据分区设置和 ReduceTask 设置

```java
package com.TianHan.mapreduce.partitioner2;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import java.io.IOException;  
  
public class FlowDriver {  
  
    public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException {  
  
        //1 获取 job 对象    
		Configuration conf = new Configuration ();  
        Job job = Job.getInstance (conf);  
  
        //2 关联本 Driver 类    
		job.setJarByClass (FlowDriver.class);  
  
        //3 关联 Mapper 和 Reducer    
		job.setMapperClass (FlowMapper.class);  
        job.setReducerClass (FlowReducer.class);  
  
        //4 设置 Map 端输出数据的 KV 类型    
		job.setMapOutputKeyClass (Text.class);  
        job.setMapOutputValueClass (FlowBean.class);  
  
        //5 设置程序最终输出的 KV 类型    
		job.setOutputKeyClass (Text.class);  
        job.setOutputValueClass (FlowBean.class);  
  
        //8 指定自定义分区器    
		job.setPartitionerClass (ProvincePartitioner.class);  
  
        //9 同时指定相应数量的 ReduceTask    
		job.setNumReduceTasks (5);  
  
        //6 设置输入输出路径    
		FileInputFormat.setInputPaths (job, new Path ("E:\\BigData\\hadoop\\input"));  
        FileOutputFormat.setOutputPath (job, new Path ("E:\\BigData\\hadoop\\output2"));  
  
        //7 提交 Job    
		boolean b = job.waitForCompletion (true);  
        System.exit (b ? 0 : 1);  
    }  
}
```

指定自定义分区器 `job.setPartitionerClass (ProvincePartitioner.class);`

同时指定相应数量的 ReduceTask`job.setNumReduceTasks (5);`

自定义分区器和 ReduceTask 数量要相等，否则可能报错 `Illegal partition ..`。

设置为 1 即 `job.setNumReduceTasks (1);` 走默认 partioner 方法，不会报错，但是无法达到想要的效果。

```java
this.partitioner = new Partitioner<K, V>() {  
    public int getPartition (K key, V value, int numPartitions) {  
        return NewOutputCollector.this.partitions - 1;  
    }  
};
```

设置为 2 或 3 或 4 可能报错 `Illegal partition ..`。

设置为 5 刚刚好。

设置为 6 等大于 5 的数时不报错，但是多余文件中内容为空。
### 3.3.4 WritableComparable 排序

![image-20221110130827331](https://image.3001.net/images/20221110/1668056910753.png)

![image-20221110130839547](https://image.3001.net/images/20221110/1668056922989.png)

![image-20221110130850481](https://image.3001.net/images/20221110/16680569339822.png)

自定义排序**WritableComparable**原理分析

​bean 对象做为 key 传输，需要实现 WritableComparable 接口重写 compareTo 方法，就可以实现排序。

```java
@Override  
public int compareTo (FlowBean bean) {  
  
	int result;  
		  
	// 按照总流量大小，倒序排列  
	if (this.sumFlow > bean.getSumFlow ()) {  
		result = -1;  
	} else if (this.sumFlow < bean.getSumFlow ()) {  
		result = 1;  
	} else {  
		result = 0;  
	}  
  
	return result;  
}
```
### 3.3.5 WritableComparable 排序案例实操（全排序）

**1**）需求

根据案例 2.3 序列化案例产生的结果再次对总流量进行倒序排序。

（1）输入数据

​`phone_data.txt`

```
1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
2	13846544121	192.196.100.2			264	0	200
3 	13956435636	192.196.100.3			132	1512	200
4 	13966251146	192.168.100.1			240	0	404
5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
7 	13590439668	192.168.100.4			1116	954	200
8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
9 	13729199489	192.168.100.6			240	0	200
10 	13630577991	192.168.100.7	www.shouhu.com	6960	690	200
11 	15043685818	192.168.100.8	www.baidu.com	3659	3538	200
12 	15959002129	192.168.100.9	www.atguigu.com	1938	180	500
13 	13560439638	192.168.100.10			918	4938	200
14 	13470253144	192.168.100.11			180	180	200
15 	13682846555	192.168.100.12	www.qq.com	1938	2910	200
16 	13992314666	192.168.100.13	www.gaga.com	3008	3720	200
17 	13509468723	192.168.100.14	www.qinghua.com	7335	110349	404
18 	18390173782	192.168.100.15	www.sogou.com	9531	2412	200
19 	13975057813	192.168.100.16	www.baidu.com	11058	48243	200
20 	13768778790	192.168.100.17			120	120	200
21 	13568436656	192.168.100.18	www.alibaba.com	2481	24681	200
22 	13568436656	192.168.100.19			1116	954	200
```

第一次处理后的数据中只保留下面这个文件：

```
part-r-00000
```

（2）期望输出数据

```
13509468723 7335 110349 117684
13736230513 2481 24681 27162
13956435636 132 1512 1644
13846544121 264 0 264
。。。 。。。
```

**2**）需求分析

![image-20221110131050892](https://image.3001.net/images/20221110/1668057054897.png)

**3**）代码实现

（1）FlowBean 对象在在需求 1 基础上增加了比较功能

```java
import org.apache.hadoop.io.WritableComparable;  
import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  
  
public class FlowBean implements WritableComparable<FlowBean> {  
  
    private long upFlow; // 上行流量  
    private long downFlow; // 下行流量  
    private long sumFlow; // 总流量  
  
    // 提供无参构造  
    public FlowBean () {  
    }  
  
    // 生成三个属性的 getter 和 setter 方法  
    public long getUpFlow () {  
        return upFlow;  
    }  
  
    public void setUpFlow (long upFlow) {  
        this.upFlow = upFlow;  
    }  
  
    public long getDownFlow () {  
        return downFlow;  
    }  
  
    public void setDownFlow (long downFlow) {  
        this.downFlow = downFlow;  
    }  
  
    public long getSumFlow () {  
        return sumFlow;  
    }  
  
    public void setSumFlow (long sumFlow) {  
        this.sumFlow = sumFlow;  
    }  
  
    public void setSumFlow () {  
        this.sumFlow = this.upFlow + this.downFlow;  
    }  
  
    // 实现序列化和反序列化方法，注意顺序一定要一致  
    @Override  
    public void write (DataOutput out) throws IOException {  
        out.writeLong (this.upFlow);  
        out.writeLong (this.downFlow);  
        out.writeLong (this.sumFlow);  
  
    }  
  
    @Override  
    public void readFields (DataInput in) throws IOException {  
        this.upFlow = in.readLong ();  
        this.downFlow = in.readLong ();  
        this.sumFlow = in.readLong ();  
    }  
  
    // 重写 ToString, 最后要输出 FlowBean  
    @Override  
    public String toString () {  
        return upFlow + "\t" + downFlow + "\t" + sumFlow;  
    }  
  
    @Override  
    public int compareTo (FlowBean o) {  
  
        // 按照总流量比较，倒序排列  
        if (this.sumFlow > o.sumFlow){  
            return -1;  
        } else if (this.sumFlow < o.sumFlow){  
            return 1;  
        } else {  
            return 0;  
        }  
    }  
}
```

（2）编写 Mapper 类

```java
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;  
import java.io.IOException;  
  
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {  
    private FlowBean outK = new FlowBean ();  
    private Text outV = new Text ();  
  
    @Override  
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
  
        //1 获取一行数据  
        String line = value.toString ();  
  
        //2 按照 "\t", 切割数据  
        String [] split = line.split ("\t");  
  
        //3 封装 outK outV  
        outK.setUpFlow (Long.parseLong (split [1]));  
        outK.setDownFlow (Long.parseLong (split [2]));  
        outK.setSumFlow ();  
        outV.set (split [0]);  
  
        //4 写出 outK outV  
        context.write (outK,outV);  
    }  
}
```

（3）编写 Reducer 类

```java
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;  
import java.io.IOException;  
  
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {  
    @Override  
    protected void reduce (FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
  
        // 遍历 values 集合，循环写出，避免总流量相同的情况  
        for (Text value : values) {  
            // 调换 KV 位置，反向写出  
            context.write (value,key);  
        }  
    }  
}
```

总流量相同（key 相同）的键值对同时进入 reduce 方法。

（4）编写 Driver 类

```java
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import java.io.IOException;  
  
public class FlowDriver {  
  
    public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException {  
  
        //1 获取 job 对象  
        Configuration conf = new Configuration ();  
        Job job = Job.getInstance (conf);  
  
        //2 关联本 Driver 类  
        job.setJarByClass (FlowDriver.class);  
  
        //3 关联 Mapper 和 Reducer  
        job.setMapperClass (FlowMapper.class);  
        job.setReducerClass (FlowReducer.class);  
  
        //4 设置 Map 端输出数据的 KV 类型  
        job.setMapOutputKeyClass (FlowBean.class);  
        job.setMapOutputValueClass (Text.class);  
  
        //5 设置程序最终输出的 KV 类型  
        job.setOutputKeyClass (Text.class);  
        job.setOutputValueClass (FlowBean.class);  
  
        //6 设置输入输出路径  
        FileInputFormat.setInputPaths (job, new Path ("D:\\inputflow2"));  
        FileOutputFormat.setOutputPath (job, new Path ("D:\\comparout"));  
  
        //7 提交 Job  
        boolean b = job.waitForCompletion (true);  
        System.exit (b ? 0 : 1);  
    }  
}
```

输出结果如下所示：

```
13509468723	7335	110349	117684
13975057813	11058	48243	59301
13568436656	3597	25635	29232
13736230513	2481	24681	27162
18390173782	9531	2412	11943
13630577991	6960	690	7650
15043685818	3659	3538	7197
13992314666	3008	3720	6728
15910133277	3156	2936	6092
13560439638	918	4938	5856
84188413	4116	1432	5548
13682846555	1938	2910	4848
18271575951	1527	2106	3633
15959002129	1938	180	2118
13590439668	1116	954	2070
13956435636	132	1512	1644
13470253144	180	180	360
13846544121	264	0	264
13729199489	240	0	240
13768778790	120	120	240
13966251146	240	0	240
```

二次排序案例（总流量相同则按照上行流量升序排列）

修改 FlowBean.java 文件中的比较方式：

```java
@Override  
public int compareTo (FlowBean o) {  
    if (this.sumFlow > o.sumFlow) {  
        return -1;  
    } else if (this.sumFlow < o.sumFlow) {  
        return 1;  
    } else {  
        if (this.upFlow > o.upFlow){  
            return 1;  
        } else if (this.upFlow < o.upFlow){  
            return -1;  
        } else {  
            return 0;  
        }  
    }  
}
```

简单写法：

```java
@Override  
public int compareTo (FlowBean o) {  
    if (this.sumFlow > o.sumFlow) {  
        return -1;  
    } else if (this.sumFlow < o.sumFlow) {  
        return 1;  
    } else {  
        return this.upFlow.compareTo (o.upFlow);  
    }  
}
```

输出效果如下所示：

```
13509468723	7335	110349	117684
13975057813	11058	48243	59301
13568436656	3597	25635	29232
13736230513	2481	24681	27162
18390173782	9531	2412	11943
13630577991	6960	690	7650
15043685818	3659	3538	7197
13992314666	3008	3720	6728
15910133277	3156	2936	6092
13560439638	918	4938	5856
84188413	4116	1432	5548
13682846555	1938	2910	4848
18271575951	1527	2106	3633
15959002129	1938	180	2118
13590439668	1116	954	2070
13956435636	132	1512	1644
13470253144	180	180	360
13846544121	264	0	264
13768778790	120	120	240
13729199489	240	0	240
13966251146	240	0	240
```
### 3.3.6 WritableComparable 排序案例实操（区内排序）

**1**）需求

要求每个省份手机号输出的文件中按照总流量内部排序。

**2**）需求分析

​ 基于前一个需求，增加自定义分区类，分区按照省份手机号设置。

![image-20221110131236743](https://image.3001.net/images/20221110/16680571604100.png)

**3**）案例实操

（1）增加自定义分区类

```java
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Partitioner;  
  
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {  
  
    @Override  
    public int getPartition (FlowBean flowBean, Text text, int numPartitions) {  
        // 获取手机号前三位  
        String phone = text.toString ();  
        String prePhone = phone.substring (0, 3);  
  
        // 定义一个分区号变量 partition, 根据 prePhone 设置分区号  
        int partition;  
        if ("136".equals (prePhone)){  
            partition = 0;  
        } else if ("137".equals (prePhone)){  
            partition = 1;  
        } else if ("138".equals (prePhone)){  
            partition = 2;  
        } else if ("139".equals (prePhone)){  
            partition = 3;  
        } else {  
            partition = 4;  
        }  
  
        // 最后返回分区号 partition  
        return partition;  
    }  
}
```

（2）在驱动类中添加分区类

```java
package com.TianHan.mapreduce.writableComparablePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main (String [] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);

        job.setJarByClass (FlowDriver.class);

        job.setMapperClass (FlowMapper.class);
        job.setReducerClass (FlowReducer.class);

        job.setMapOutputKeyClass (FlowBean.class);
        job.setMapOutputValueClass (Text.class);

        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (FlowBean.class);

        // 设置自定义分区器
        job.setPartitionerClass (ProvincePartitioner.class);
        // 设置对应的 ReduceTask 的个数
        job.setNumReduceTasks (5);

        FileInputFormat.setInputPaths (job, new Path ("E:\\BigData\\hadoop\\output"));
        FileOutputFormat.setOutputPath (job, new Path ("E:\\BigData\\hadoop\\output5"));

        boolean res = job.waitForCompletion (true);
        System.exit (res ? 0 : 1);
    }
}

```
### 3.3.7 Combiner 合并

![image-20221110131319588](https://image.3001.net/images/20221110/16680572024854.png)

（6）自定义 Combiner 实现步骤

（a）自定义一个 Combiner 继承 Reducer，重写 Reduce 方法

```java
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {  
  
    private IntWritable outV = new IntWritable ();  
  
    @Override  
    protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  
  
        int sum = 0;  
        for (IntWritable value : values) {  
            sum += value.get ();  
        }  
       
        outV.set (sum);  
       
        context.write (key,outV);  
    }  
}
```

（b）在 Job 驱动类中设置：

```java
job.setCombinerClass (WordCountCombiner.class);
```
### 3.3.8 Combiner 合并案例实操

**1**）需求

​ 统计过程中对每一个 MapTask 的输出进行局部汇总，以减小网络传输量即采用 Combiner 功能。

（1）数据输入

`hello.txt`

```
banzhang ni hao  
xihuan hadoop banzhang  
banzhang ni hao  
xihuan hadoop banzhang
```

（2）期望输出数据

期望：Combine 输入数据多，输出时经过合并，输出数据减少。

**2**）需求分析

![image-20221110131417534](https://image.3001.net/images/20221110/16680572608397.png)

**3**）案例实操 - 方案一

（1）增加一个 WordCountCombiner 类继承 Reducer

```java
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;  
import java.io.IOException;  
  
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {  
  
private IntWritable outV = new IntWritable ();  
  
    @Override  
    protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  
  
        int sum = 0;  
        for (IntWritable value : values) {  
            sum += value.get ();  
        }  
  
        // 封装 outKV  
        outV.set (sum);  
  
        // 写出 outKV  
        context.write (key,outV);  
    }  
}
```

（2）在 WordcountDriver 驱动类中指定 Combiner

```java
// 指定需要使用 combiner，以及用哪个类作为 combiner 的逻辑  
job.setCombinerClass (WordCountCombiner.class);
```

**4**）案例实操 - 方案二（推荐）

（1）由于 Reducer 类中已经实现了相同的方法，下面将在 WordcountDriver 驱动类中指定 WordcountReducer 作为 Combiner

```java
// 指定需要使用 Combiner，以及用哪个类作为 Combiner 的逻辑  
job.setCombinerClass (WordCountReducer.class);
```

运行程序，如下图所示

![image-20221110131634660](https://image.3001.net/images/20221110/1668057397598.png)

注意：没有 Reduce 阶段就没有 Shuffle 阶段。
## 3.4 OutputFormat 数据输出

### 3.4.1 OutputFormat 接口实现类

![image-20221110131703273](https://image.3001.net/images/20221110/16680574261360.png)

### 3.4.2 自定义 OutputFormat 案例实操

**1**）需求

过滤输入的 `log` 日志，包含 `atguigu` 的网站输出到 `e:/atguigu.log`，不包含 `atguigu` 的网站输出到 `e:/other.log`。

（1）输入数据

`log.txt`

```
http://www.baidu.com  
http://www.google.com  
http://cn.bing.com  
http://www.atguigu.com  
http://www.sohu.com  
http://www.sina.com  
http://www.sin2a.com  
http://www.sin2desa.com  
http://www.sindsafa.com
```

（2）期望输出数据

​ atguigu.log

```
http://www.atguigu.com
```

other.log

```
http://cn.bing.com  
http://www.baidu.com  
http://www.google.com  
http://www.sin2a.com  
http://www.sin2desa.com  
http://www.sina.com  
http://www.sindsafa.com  
http://www.sohu.com
```

**2**）需求分析

![image-20221110132146534](https://image.3001.net/images/20221110/16680577096922.png)

**3**）案例实操

（1）编写 LogMapper 类

```java
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;  
  
import java.io.IOException;  
  
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {  
    @Override  
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
        // 不做任何处理，直接写出一行 log 数据  
        context.write (value,NullWritable.get ());  
    }  
}
```

（2）编写 LogReducer 类

```java
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;  
  
import java.io.IOException;  
  
public class LogReducer extends Reducer<Text, NullWritable,Text, NullWritable> {  
    @Override  
    protected void reduce (Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {  
        // 防止有相同的数据，迭代写出  
        for (NullWritable value : values) {  
            context.write (key,NullWritable.get ());  
        }  
    }  
}
```

（3）自定义一个 LogOutputFormat 类

```java
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
import java.io.IOException;  
  
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {  
    @Override  
    public RecordWriter<Text, NullWritable> getRecordWriter (TaskAttemptContext job) throws IOException, InterruptedException {  
        // 创建一个自定义的 RecordWriter 返回  
        LogRecordWriter logRecordWriter = new LogRecordWriter (job);  
        return logRecordWriter;  
    }  
}
```

（4）编写 LogRecordWriter 类

```java
package com.atguigu.mapreduce.outputformat;  
  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
  
import java.io.IOException;  
  
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {  
  
    private FSDataOutputStream atguiguOut;  
    private FSDataOutputStream otherOut;  
  
    public LogRecordWriter (TaskAttemptContext job) {  
        try {  
            // 获取文件系统对象  
            FileSystem fs = FileSystem.get (job.getConfiguration ());  
            // 用文件系统对象创建两个输出流对应不同的目录  
            atguiguOut = fs.create (new Path ("d:/hadoop/atguigu.log"));  
            otherOut = fs.create (new Path ("d:/hadoop/other.log"));  
        } catch (IOException e) {  
            e.printStackTrace ();  
        }  
    }  
  
    @Override  
    public void write (Text key, NullWritable value) throws IOException, InterruptedException {  
        String log = key.toString ();  
        // 根据一行的 log 数据是否包含 atguigu, 判断两条输出流输出的内容  
        if (log.contains ("atguigu")) {  
            atguiguOut.writeBytes (log + "\n");  
        } else {  
            otherOut.writeBytes (log + "\n");  
        }  
    }  
  
    @Override  
    public void close (TaskAttemptContext context) throws IOException, InterruptedException {  
        // 关流  
        IOUtils.closeStream (atguiguOut);  
        IOUtils.closeStream (otherOut);  
    }  
}
```

（5）编写 LogDriver 类

```java
package com.atguigu.mapreduce.outputformat;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
import java.io.IOException;  
  
public class LogDriver {  
    public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException {  
  
        Configuration conf = new Configuration ();  
        Job job = Job.getInstance (conf);  
  
        job.setJarByClass (LogDriver.class);  
        job.setMapperClass (LogMapper.class);  
        job.setReducerClass (LogReducer.class);  
  
        job.setMapOutputKeyClass (Text.class);  
        job.setMapOutputValueClass (NullWritable.class);  
  
        job.setOutputKeyClass (Text.class);  
        job.setOutputValueClass (NullWritable.class);  
  
        // 设置自定义的 outputformat  
        job.setOutputFormatClass (LogOutputFormat.class);  
  
        FileInputFormat.setInputPaths (job, new Path ("D:\\input"));  
        // 虽然我们自定义了 outputformat，但是因为我们的 outputformat 继承自 fileoutputformat  
        // 而 fileoutputformat 要输出一个_SUCCESS 文件，所以在这还得指定一个输出目录  
        FileOutputFormat.setOutputPath (job, new Path ("D:\\logoutput"));  
  
        boolean b = job.waitForCompletion (true);  
        System.exit (b ? 0 : 1);  
    }  
}
```


## 3.5 MapReduce 内核源码解析

### 3.5.1 MapTask 工作机制

![image-20221110132710797](https://image.3001.net/images/20221110/16680580349014.png)

​ （1）Read 阶段：MapTask 通过 InputFormat 获得的 RecordReader，从输入 InputSplit 中解析出一个个 key/value。

​ （2）Map 阶段：该节点主要是将解析出的 key/value 交给用户编写 map () 函数处理，并产生一系列新的 key/value。

​ （3）Collect 收集阶段：在用户编写 map () 函数中，当数据处理完成后，一般会调用 OutputCollector.collect () 输出结果。在该函数内部，它会将生成的 key/value 分区（调用 Partitioner），并写入一个环形内存缓冲区中。

​ （4）Spill 阶段：即 “溢写”，当环形缓冲区满后，MapReduce 会将数据写到本地磁盘上，生成一个临时文件。需要注意的是，将数据写入本地磁盘之前，先要对数据进行一次本地排序，并在必要时对数据进行合并、压缩等操作。

​ 溢写阶段详情：

​ 步骤 1：利用快速排序算法对缓存区内的数据进行排序，排序方式是，先按照分区编号 Partition 进行排序，然后按照 key 进行排序。这样，经过排序后，数据以分区为单位聚集在一起，且同一分区内所有数据按照 key 有序。

​ 步骤 2：按照分区编号由小到大依次将每个分区中的数据写入任务工作目录下的临时文件 output/spillN.out（N 表示当前溢写次数）中。如果用户设置了 Combiner，则写入文件之前，对每个分区中的数据进行一次聚集操作。

​ 步骤 3：将分区数据的元信息写到内存索引数据结构 SpillRecord 中，其中每个分区的元信息包括在临时文件中的偏移量、压缩前数据大小和压缩后数据大小。如果当前内存索引大小超过 1MB，则将内存索引写到文件 output/spillN.out.index 中。

​ （5）Merge 阶段：当所有数据处理完成后，MapTask 对所有临时文件进行一次合并，以确保最终只会生成一个数据文件。

​ 当所有数据处理完后，MapTask 会将所有临时文件合并成一个大文件，并保存到文件 output/file.out 中，同时生成相应的索引文件 output/file.out.index。

​ 在进行文件合并过程中，MapTask 以分区为单位进行合并。对于某个分区，它将采用多轮递归合并的方式。每轮合并 mapreduce.task.io.sort.factor（默认 10）个文件，并将产生的文件重新加入待合并列表中，对文件排序后，重复以上过程，直到最终得到一个大文件。

​ 让每个 MapTask 最终只生成一个数据文件，可避免同时打开大量文件和同时读取大量小文件产生的随机读取带来的开销。

### 3.5.2 ReduceTask 工作机制

![image-20221110132754693](https://image.3001.net/images/20221110/16680580777443.png)

​ （1）Copy 阶段：ReduceTask 从各个 MapTask 上远程拷贝一片数据，并针对某一片数据，如果其大小超过一定阈值，则写到磁盘上，否则直接放到内存中。

​ （2）Sort 阶段：在远程拷贝数据的同时，ReduceTask 启动了两个后台线程对内存和磁盘上的文件进行合并，以防止内存使用过多或磁盘上文件过多。按照 MapReduce 语义，用户编写 reduce () 函数输入数据是按 key 进行聚集的一组数据。为了将 key 相同的数据聚在一起，Hadoop 采用了基于排序的策略。由于各个 MapTask 已经实现对自己的处理结果进行了局部排序，因此，ReduceTask 只需对所有数据进行一次归并排序即可。

​ （3）Reduce 阶段：reduce () 函数将计算结果写到 HDFS 上。

### 3.5.3 ReduceTask 并行度决定机制

**回顾：** MapTask 并行度由切片个数决定，切片个数由输入文件和切片规则决定。

**思考：** ReduceTask 并行度由谁决定？

**1**）设置 ReduceTask 并行度（个数）

​ ReduceTask 的并行度同样影响整个 Job 的执行并发度和执行效率，但与 MapTask 的并发数由切片数决定不同，ReduceTask 数量的决定是可以直接手动设置：

```java
// 默认值是 1，手动设置为 4  
job.setNumReduceTasks (4);
```

**2**）实验：测试 ReduceTask 多少合适

（1）实验环境：1 个 Master 节点，16 个 Slave 节点：CPU:8GHZ，内存: 2G

（2）实验结论：

表 改变 ReduceTask（数据量为 1GB）

MapTask =16

| ReduceTask | 1   | 5   | 10  | 15  | 16  | 20  | 25  | 30  | 45  | 60  |
| ---------- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 总时间        | 892 | 146 | 110 | 92  | 88  | 100 | 128 | 101 | 145 | 104 |

**3**）注意事项

![image-20221110132857785](https://image.3001.net/images/20221110/16680581412729.png)

### 3.5.4 MapTask & ReduceTask 源码解析

**1**）MapTask 源码解析流程

![image-20221110132920449](https://image.3001.net/images/20221110/16680581638498.png)

**2**）ReduceTask 源码解析流程

![image-20221110133003542](https://image.3001.net/images/20221110/16680582064495.png)

## 3.6 Join 应用

### 3.6.1 Reduce Join

​Map 端的主要工作：为来自不同表或文件的 key/value 对，打标签以区别不同来源的记录。然后用连接字段作为 key，其余部分和新加的标志作为 value，最后进行输出。

Reduce 端的主要工作：在 Reduce 端以连接字段作为 key 的分组已经完成，我们只需要在每一个分组当中将那些来源于不同文件的记录（在 Map 阶段已经打标志）分开，最后进行合并就 ok 了。

### 3.6.2 Reduce Join 案例实操

**1**）需求

order.txt

```
1001	01	1
1002	02	2
1003	03	3
1004	01	4
1005	02	5
1006	03	6
```

pd.txt

```
01	小米
02	华为
03	格力
```

表 4-4 订单数据表 t_order

|id|pid|amount|
|---|---|---|
|1001|01|1|
|1002|02|2|
|1003|03|3|
|1004|01|4|
|1005|02|5|
|1006|03|6|

表 4-5 商品信息表 t_product

|pid|pname|
|---|---|
|01 | 小米 |
|02 | 华为 |
|03 | 格力 |

​将商品信息表中数据根据商品 pid 合并到订单数据表中。

表 4-6 最终数据形式

|id|pname|amount|
|---|---|---|
|1001 | 小米 | 1|
|1004 | 小米 | 4|
|1002 | 华为 | 2|
|1005 | 华为 | 5|
|1003 | 格力 | 3|
|1006 | 格力 | 6|

**2**）需求分析

​ 通过将关联条件作为 Map 输出的 key，将两表满足 Join 条件的数据并携带数据所来源的文件信息，发往同一个 ReduceTask，在 Reduce 中进行数据的串联。

![image-20221110133359824](https://image.3001.net/images/20221110/16680584431906.png)

**3**）代码实现

（1）创建商品和订单合并后的 TableBean 类

```java
package com.atguigu.mapreduce.reducejoin;  
  
import org.apache.hadoop.io.Writable;  
  
import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  
  
public class TableBean implements Writable {  
  
    private String id; // 订单 id  
    private String pid; // 产品 id  
    private int amount; // 产品数量  
    private String pname; // 产品名称  
    private String flag; // 判断是 order 表还是 pd 表的标志字段  
  
    public TableBean () {  
    }  
  
    public String getId () {  
        return id;  
    }  
  
    public void setId (String id) {  
        this.id = id;  
    }  
  
    public String getPid () {  
        return pid;  
    }  
  
    public void setPid (String pid) {  
        this.pid = pid;  
    }  
  
    public int getAmount () {  
        return amount;  
    }  
  
    public void setAmount (int amount) {  
        this.amount = amount;  
    }  
  
    public String getPname () {  
        return pname;  
    }  
  
    public void setPname (String pname) {  
        this.pname = pname;  
    }  
  
    public String getFlag () {  
        return flag;  
    }  
  
    public void setFlag (String flag) {  
        this.flag = flag;  
    }  
  
    @Override  
    public String toString () {  
        return id + "\t" + pname + "\t" + amount;  
    }  
  
    @Override  
    public void write (DataOutput out) throws IOException {  
        out.writeUTF (id);  
        out.writeUTF (pid);  
        out.writeInt (amount);  
        out.writeUTF (pname);  
        out.writeUTF (flag);  
    }  
  
    @Override  
    public void readFields (DataInput in) throws IOException {  
        this.id = in.readUTF ();  
        this.pid = in.readUTF ();  
        this.amount = in.readInt ();  
        this.pname = in.readUTF ();  
        this.flag = in.readUTF ();  
    }  
}
```

（2）编写 TableMapper 类

```java
package com.atguigu.mapreduce.reducejoin;  
  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.InputSplit;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
  
import java.io.IOException;  
  
public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {  
  
    private String filename;  
    private Text outK = new Text ();  
    private TableBean outV = new TableBean ();  
  
    @Override  
    protected void setup (Context context) throws IOException, InterruptedException {  
        // 获取对应文件名称  
        InputSplit split = context.getInputSplit ();  
        FileSplit fileSplit = (FileSplit) split;  
        filename = fileSplit.getPath ().getName ();  
    }  
  
    @Override  
    protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
  
        // 获取一行  
        String line = value.toString ();  
  
        // 判断是哪个文件，然后针对文件进行不同的操作  
        if (filename.contains ("order")){  // 订单表的处理  
            String [] split = line.split ("\t");  
            // 封装 outK  
            outK.set (split [1]);  
            // 封装 outV  
            outV.setId (split [0]);  
            outV.setPid (split [1]);  
            outV.setAmount (Integer.parseInt (split [2]));  
            outV.setPname ("");  
            outV.setFlag ("order");  
        } else {                             // 商品表的处理  
            String [] split = line.split ("\t");  
            // 封装 outK  
            outK.set (split [0]);  
            // 封装 outV  
            outV.setId ("");  
            outV.setPid (split [0]);  
            outV.setAmount (0);  
            outV.setPname (split [1]);  
            outV.setFlag ("pd");  
        }  
  
        // 写出 KV  
        context.write (outK,outV);  
    }  
}
```

（3）编写 TableReducer 类

```java
package com.atguigu.mapreduce.reducejoin;  
  
import org.apache.commons.beanutils.BeanUtils;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;  
  
import java.io.IOException;  
import java.lang.reflect.InvocationTargetException;  
import java.util.ArrayList;  
  
public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {  
  
    @Override  
    protected void reduce (Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {  
  
        ArrayList<TableBean> orderBeans = new ArrayList<>();  
        TableBean pdBean = new TableBean ();  
  
        for (TableBean value : values) {  
  
            // 判断数据来自哪个表  
            if ("order".equals (value.getFlag ())){   // 订单表  
  
			  // 创建一个临时 TableBean 对象接收 value，防止数据被覆盖
                TableBean tmpOrderBean = new TableBean ();  
  
                try {  
                    BeanUtils.copyProperties (tmpOrderBean,value);  
                } catch (IllegalAccessException e) {  
                    e.printStackTrace ();  
                } catch (InvocationTargetException e) {  
                    e.printStackTrace ();  
                }  
  
			  // 将临时 TableBean 对象添加到集合 orderBeans  
                orderBeans.add (tmpOrderBean);  
            } else {                                    // 商品表  
                try {  
                    BeanUtils.copyProperties (pdBean,value);  
                } catch (IllegalAccessException e) {  
                    e.printStackTrace ();  
                } catch (InvocationTargetException e) {  
                    e.printStackTrace ();  
                }  
            }  
        }  
  
        // 遍历集合 orderBeans, 替换掉每个 orderBean 的 pid 为 pname, 然后写出  
        for (TableBean orderBean : orderBeans) {  
  
            orderBean.setPname (pdBean.getPname ());  
  
		   // 写出修改后的 orderBean 对象  
            context.write (orderBean,NullWritable.get ());  
        }  
    }  
}
```

（4）编写 TableDriver 类

```java
package com.atguigu.mapreduce.reducejoin;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
import java.io.IOException;  
  
public class TableDriver {  
    public static void main (String [] args) throws IOException, ClassNotFoundException, InterruptedException {  
        Job job = Job.getInstance (new Configuration ());  
  
        job.setJarByClass (TableDriver.class);  
        job.setMapperClass (TableMapper.class);  
        job.setReducerClass (TableReducer.class);  
  
        job.setMapOutputKeyClass (Text.class);  
        job.setMapOutputValueClass (TableBean.class);  
  
        job.setOutputKeyClass (TableBean.class);  
        job.setOutputValueClass (NullWritable.class);  
  
        FileInputFormat.setInputPaths (job, new Path ("D:\\input"));  
        FileOutputFormat.setOutputPath (job, new Path ("D:\\output"));  
  
        boolean b = job.waitForCompletion (true);  
        System.exit (b ? 0 : 1);  
    }  
}
```

**4**）测试

运行程序查看结果

```
1004	小米	4
1001	小米	1
1005	华为	5
1002	华为	2
1006	格力	6
1003	格力	3
```

**5**）总结

缺点：这种方式中，合并的操作是在 Reduce 阶段完成，Reduce 端的处理压力太大，Map 节点的运算负载则很低，资源利用率不高，且在 Reduce 阶段极易产生数据倾斜。

**解决方案：Map 端实现数据合并。**

### 3.6.3 Map Join

**1**）使用场景

Map Join 适用于一张表十分小、一张表很大的场景。

**2**）优点

思考：在 Reduce 端处理过多的表，非常容易产生数据倾斜。怎么办？

在 Map 端缓存多张表，提前处理业务逻辑，这样增加 Map 端业务，减少 Reduce 端数据的压力，尽可能的减少数据倾斜。

**3**）具体办法：采用 DistributedCache

​ （1）在 Mapper 的 setup 阶段，将文件读取到缓存集合中。

​ （2）在 Driver 驱动类中加载缓存。

```java
// 缓存普通文件到 Task 运行节点。  
job.addCacheFile (new URI ("file:///e:/cache/pd.txt"));  
// 如果是集群运行，需要设置 HDFS 路径  
job.addCacheFile (new URI ("hdfs://hadoop102:8020/cache/pd.txt"));
```
### 3.6.4 Map Join 案例实操

**1**）需求

表 订单数据表 t_order

|id|pid|amount|
|---|---|---|
|1001|01|1|
|1002|02|2|
|1003|03|3|
|1004|01|4|
|1005|02|5|
|1006|03|6|

表 商品信息表 t_product

|pid|pname|
|---|---|
|01 | 小米 |
|02 | 华为 |
|03 | 格力 |

​ 将商品信息表中数据根据商品 pid 合并到订单数据表中。

表最终数据形式

|id|pname|amount|
|---|---|---|
|1001 | 小米 | 1|
|1004 | 小米 | 4|
|1002 | 华为 | 2|
|1005 | 华为 | 5|
|1003 | 格力 | 3|
|1006 | 格力 | 6|

**2**）需求分析

MapJoin 适用于关联表中有小表的情形。

![image-20221110133816471](https://image.3001.net/images/20221110/16680586993772.png)

**3**）实现代码

（1）先在 MapJoinDriver 驱动类中添加缓存文件

```java
package com.TianHan.mapreduce.mapJoin;  
    
import org.apache.hadoop.conf.Configuration;    
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.NullWritable;    
import org.apache.hadoop.io.Text;    
import org.apache.hadoop.mapreduce.Job;    
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;    
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;    
    
import java.io.IOException;    
import java.net.URI;    
import java.net.URISyntaxException;    
    
public class MapJoinDriver {    
    
    public static void main (String [] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {    
    
        // 1 获取 job 信息    
		Configuration conf = new Configuration ();    
        Job job = Job.getInstance (conf);    
        // 2 设置加载 jar 包路径    
		job.setJarByClass (MapJoinDriver.class);    
        // 3 关联 mapper    
		job.setMapperClass (MapJoinMapper.class);    
        // 4 设置 Map 输出 KV 类型    
		job.setMapOutputKeyClass (Text.class);    
        job.setMapOutputValueClass (NullWritable.class);    
        // 5 设置最终输出 KV 类型    
		job.setOutputKeyClass (Text.class);    
        job.setOutputValueClass (NullWritable.class);    
    
        // 加载缓存数据    
		job.addCacheFile (new URI ("file:///E:/BigData/hadoop/inputJoin/tablecache/pd.txt"));  
        // Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask 数量为 0    
		job.setNumReduceTasks (0);    
    
        // 6 设置输入输出路径    
		FileInputFormat.setInputPaths (job, new Path ("E:\\BigData\\hadoop\\inputJoin\\input"));  
        FileOutputFormat.setOutputPath (job, new Path ("E:\\BigData\\hadoop\\inputJoin\\output"));  
        // 7 提交    
		boolean b = job.waitForCompletion (true);    
        System.exit (b ? 0 : 1);    
    }    
}
```

（2）在 MapJoinMapper 类中的 setup 方法中读取缓存文件

```java
package com.TianHan.mapreduce.mapJoin;  
  
import org.apache.commons.lang3.StringUtils;  
import org.apache.hadoop.fs.FSDataInputStream;    
import org.apache.hadoop.fs.FileSystem;    
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.IOUtils;    
import org.apache.hadoop.io.LongWritable;    
import org.apache.hadoop.io.NullWritable;    
import org.apache.hadoop.io.Text;    
import org.apache.hadoop.mapreduce.Mapper;    
    
import java.io.BufferedReader;    
import java.io.IOException;    
import java.io.InputStreamReader;    
import java.net.URI;    
import java.util.HashMap;    
import java.util.Map;    
    
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {    
    
    private Map<String, String> pdMap = new HashMap<>();    
    private Text text = new Text ();    
    
    // 任务开始前将 pd 数据缓存进 pdMap    
@Override    
protected void setup (Context context) throws IOException, InterruptedException {    
    
        // 通过缓存文件得到小表数据 pd.txt    
		URI [] cacheFiles = context.getCacheFiles ();    
        Path path = new Path (cacheFiles [0]);    
    
        // 获取文件系统对象，并开流    
		FileSystem fs = FileSystem.get (context.getConfiguration ());    
        FSDataInputStream fis = fs.open (path);    
    
        // 通过包装流转换为 reader, 方便按行读取    
		BufferedReader reader = new BufferedReader (new InputStreamReader (fis, "UTF-8"));    
    
        // 逐行读取，按行处理    
		String line;    
        while (StringUtils.isNotEmpty (line = reader.readLine ())) {    
            // 切割一行        
			//01    小米  
            String [] split = line.split ("\t");    
            pdMap.put (split [0], split [1]);    
        }    
    
        // 关流    
		IOUtils.closeStream (reader);    
    }    
    
    @Override    
protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
    
        // 读取大表 order.txt 数据  
        //1001  01 1  
        String [] fields = value.toString ().split ("\t");    
    
        // 通过大表每行数据的 pid, 去 pdMap 里面取出 pname    
		String pname = pdMap.get (fields [1]);    
    
        // 将大表每行数据的 pid 替换为 pname    
		text.set (fields [0] + "\t" + pname + "\t" + fields [2]);    
    
        // 写出    
		context.write (text,NullWritable.get ());    
    }    
}
```

## 程序运行结果

part-m-00000

```
1001	小米	1
1002	华为	2
1003	格力	3
1004	小米	4
1005	华为	5
1006	格力	6
```

## 3.7 数据清洗（ETL）

ETL，是英文 Extract-Transform-Load 的缩写，用来描述将数据从来源端经过抽取（Extract）、转换（Transform）、加载（Load）至目的端的过程。ETL 一词较常用在数据仓库，但其对象并不限于数据仓库

在运行核心业务 MapReduce 程序之前，往往要先对数据进行清洗，清理掉不符合用户要求的数据。清理的过程往往只需要运行 Mapper 程序，不需要运行 Reduce 程序。

**1**）需求

去除日志中字段个数小于等于 11 的日志。

（1）输入数据

web.log

在项目中找

（2）期望输出数据

每行字段长度都大于 11。

**2**）需求分析

需要在 Map 阶段对输入的数据根据规则进行过滤清洗。

**3**）实现代码

（1）编写 WebLogMapper 类

```java
package com.atguigu.mapreduce.weblog;  
import java.io.IOException;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;  
  
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{  
	  
	@Override  
	protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
		  
		// 1 获取 1 行数据  
		String line = value.toString ();  
		  
		// 2 解析日志  
		boolean result = parseLog (line,context);  
		  
		// 3 日志不合法退出  
		if (!result) {  
			return;  
		}  
		  
		// 4 日志合法就直接写出  
		context.write (value, NullWritable.get ());  
	}  
  
	// 2 封装解析日志的方法  
	private boolean parseLog (String line, Context context) {  
  
		// 1 截取  
		String [] fields = line.split (" ");  
		  
		// 2 日志长度大于 11 的为合法  
		if (fields.length > 11) {  
			return true;  
		} else {  
			return false;  
		}  
	}  
}
```

（2）编写 WebLogDriver 类

```java
package com.atguigu.mapreduce.weblog;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
public class WebLogDriver {  
	public static void main (String [] args) throws Exception {  
  
// 输入输出路径需要根据自己电脑上实际的输入输出路径设置  
        args = new String [] { "D:/input/inputlog", "D:/output1" };  
  
		// 1 获取 job 信息  
		Configuration conf = new Configuration ();  
		Job job = Job.getInstance (conf);  
  
		// 2 加载 jar 包  
		job.setJarByClass (WebLogDriver.class);  
  
		// 3 关联 map  
		job.setMapperClass (WebLogMapper.class);  
  
		// 4 设置最终输出类型  
		job.setOutputKeyClass (Text.class);  
		job.setOutputValueClass (NullWritable.class);  
  
		// 设置 reducetask 个数为 0  
		job.setNumReduceTasks (0);  
  
		// 5 设置输入和输出路径  
		FileInputFormat.setInputPaths (job, new Path (args [0]));  
		FileOutputFormat.setOutputPath (job, new Path (args [1]));  
  
		// 6 提交  
         boolean b = job.waitForCompletion (true);  
         System.exit (b ? 0 : 1);  
	}  
}
```

## 附录：ETL 清洗规则

https://developer.aliyun.com/article/1319420

好，干货开始。数据清洗的目的可以从两个角度上看一是为了解决数据质量问题，二是让数据更适合做挖掘。不同的目的下分不同的情况，也都有相应的解决方式和方法。

### 解决数据质量问题

这部分主要是规范数据，满足业务的使用，解决数据质量的各种问题，其目的包括但不限于：

1. 数据的完整性 ---- 例如人的属性中缺少性别、籍贯、年龄等  
2. 数据的唯一性 ---- 例如不同来源的数据出现重复的情况  
3. 数据的权威性 ---- 例如同一个指标出现多个来源的数据，且数值不一样  
4. 数据的合法性 ---- 例如获取的数据与常识不符，年龄大于 150 岁  
5. 数据的一致性 ---- 例如不同来源的不同指标，实际内涵是一样的，或是同一指标内涵不一致  

  

数据清洗的结果是对各种脏数据进行对应方式的处理，得到标准的、干净的、连续的数据，提供给数据统计、数据挖掘等使用。

那么为了解决以上的各种问题，我们需要不同的手段和方法来一一处理。

每种问题都有各种情况，每种情况适用不同的处理方法，具体如下：

1：解决数据的完整性问题：  

> 解题思路：数据缺失，那么补上就好了。补数据有什么方法？

- 通过其他信息补全，例如使用身份证件号码推算性别、籍贯、出生日期、年龄等  
- 通过前后数据补全，例如时间序列缺数据了，可以使用前后的均值，缺的多了，可以使用平滑等处理，记得 Matlab 还是什么工具可以自动补全  
- 实在补不全的，虽然很可惜，但也必须要剔除。但是不要删掉，没准以后可以用得上  

2：解决数据的唯一性问题  

> 解题思路：去除重复记录，只保留一条。去重的方法有：

- 按主键去重，用 sql 或者 excel “去除重复记录” 即可，  
- 按规则去重，编写一系列的规则，对重复情况复杂的数据进行去重。例如不同渠道来的客户数据，可以通过相同的关键信息进行匹配，合并去重。  

3：解决数据的权威性问题  

> 解题思路：用最权威的那个渠道的数据方法：对不同渠道设定权威级别，例如：在家里，首先得相信媳妇说的。

4：解决数据的合法性问题  

> 解题思路：设定判定规则

1. 设定强制合法规则，凡是不在此规则范围内的，强制设为最大值，或者判为无效，剔除  

- 字段类型合法规则：日期字段格式为 “2010-10-10”  
- 字段内容合法规则：性别 in （男、女、未知）；出生日期 <= 今天  

2. 设定警告规则，凡是不在此规则范围内的，进行警告，然后人工处理  

- 警告规则：年龄 > 110  

3. 离群值人工特殊处理，使用分箱、聚类、回归、等方式发现离群值  


5：解决数据的一致性问题  

> 解题思路：建立元数据体系，包含但不限于：

1. 指标体系（度量）  
2. 维度（分组、统计口径）  
3. 单位  
4. 频度  
5. 数据  

tips：

如果数据质量问题比较严重，建议跟技术团队好好聊聊。

如果需要控制的范围越来越大，这就不是 ETL 工程师的工作了，得升级为数据治理了，下次有空再分享。

### 供应算法原料

1. 这部分主要是让数据更适合数据挖掘，作为算法训练的原料。其目标包括但不限于：

1. 高维度 ---- 不适合挖掘  
2. 维度太低 ---- 不适合挖掘  
3. 无关信息 ---- 减少存储  
4. 字段冗余 ---- 一个字段是其他字段计算出来的，会造成相关系数为 1 或者主成因分析异常）  
5. 多指标数值、单位不同 ---- 如 GDP 与城镇居民人均收入数值相差过大  

1：解决高维度问题  


> 解题思路：降维，方法包括但不限于：

1. 主成分分析  
2. 随机森林  

2：解决维度低或缺少维度问题  

> 解题思路：抽象，方法包括但不限于：

1. 各种汇总，平均、加总、最大、最小等  
2. 各种离散化，聚类、自定义分组等  

3：解决无关信息和字段冗余  

> 解决方法：剔除字段

4：解决多指标数值、单位不同问题  

> 解决方法：归一化，方法包括但不限于：

1. 最小 - 最大  
2. 零 - 均值  
3. 小数定标  

其实 ETL 工程师有非常好的数据功底，无论是转那个岗都方便，你缺少的是系统的学习和迈出去的勇气。
## 附录常用正则表达式

[Java 常用正则表达式大全 (史上最全的正则表达式 - 匹配中英文、字母和数字)](https://blog.csdn.net/ws54ws54/article/details/110220049)

在做项目的过程中，使用正则表达式来匹配一段文本中的特定种类字符，是比较常用的一种方式，下面是对常用的正则匹配做了一个归纳整理。

### 一、校验数字的表达式

```
1 数字：^[0-9]*$
2 n 位的数字：^\d {n}$
3 至少 n 位的数字：^\d {n,}$
4 m-n 位的数字：^\d {m,n}$
5 零和非零开头的数字：^(0|[1-9][0-9]*)$
6 非零开头的最多带两位小数的数字：^([1-9][0-9]*)(.[0-9]{1,2})?$
7 带 1-2 位小数的正数或负数：^(\-)?\d+(\.\d {1,2})?$
8 正数、负数、和小数：^(\-|\+)?\d+(\.\d+)?$
9 有两位小数的正实数：^[0-9]+(.[0-9]{2})?$
10 有 1~3 位小数的正实数：^[0-9]+(.[0-9]{1,3})?$
11 非零的正整数：^[1-9]\d*$ 或 ^([1-9][0-9]*){1,3}$ 或 ^\+?[1-9][0-9]*$
12 非零的负整数：^\-[1-9][] 0-9"*$ 或 ^-[1-9]\d*$
13 非负整数：^\d+$ 或 ^[1-9]\d*|0$
14 非正整数：^-[1-9]\d*|0$ 或 ^((-\d+)|(0+))$
15 非负浮点数：^\d+(\.\d+)?$ 或 ^[1-9]\d*\.\d*|0\.\d*[1-9]\d*|0?\.0+|0$
16 非正浮点数：^((-\d+(\.\d+)?)|(0+(\.0+)?))$ 或 ^(-([1-9]\d*\.\d*|0\.\d*[1-9]\d*))|0?\.0+|0$
17 正浮点数：^[1-9]\d*\.\d*|0\.\d*[1-9]\d*$ 或 ^(([0-9]+\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\.[0-9]+)|([0-9]*[1-9][0-9]*))$
18 负浮点数：^-([1-9]\d*\.\d*|0\.\d*[1-9]\d*)$ 或 ^(-(([0-9]+\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\.[0-9]+)|([0-9]*[1-9][0-9]*)))$
19 浮点数：^(-?\d+)(\.\d+)?$ 或 ^-?([1-9]\d*\.\d*|0\.\d*[1-9]\d*|0?\.0+|0)$
```

### 二、校验字符的表达式

```
1 汉字：^[\u4e00-\u9fa5]{0,}$
2 英文和数字：^[A-Za-z0-9]+$ 或 ^[A-Za-z0-9]{4,40}$
3 长度为 3-20 的所有字符：^.{3,20}$
4 由 26 个英文字母组成的字符串：^[A-Za-z]+$
5 由 26 个大写英文字母组成的字符串：^[A-Z]+$
6 由 26 个小写英文字母组成的字符串：^[a-z]+$
7 由数字和 26 个英文字母组成的字符串：^[A-Za-z0-9]+$
8 由数字、26 个英文字母或者下划线组成的字符串：^\w+$ 或 ^\w {3,20}$
9 中文、英文、数字包括下划线：^[\u4E00-\u9FA5A-Za-z0-9_]+$
10 中文、英文、数字但不包括下划线等符号：^[\u4E00-\u9FA5A-Za-z0-9]+$ 或 ^[\u4E00-\u9FA5A-Za-z0-9]{2,20}$
11 可以输入含有 ^%&’,;=?KaTeX parse error: Can't use function '\"' in math mode at position 1: \̲"̲等字符：`[^%&',;=?\x22]+12 禁止输入含有～的字符：[^~\x22]+ 三、特殊需求表达式 1 Email 地址：^\w+([-+.]\w+)@\w+([-.]\w+).\w+([-.]\w+) KaTeX parse error: Undefined control sequence: \s at position 108: …`[a-zA-z]+://[^\̲s̲]*`或`^http://…4 手机号码：^(13 [0-9]|14 [5|7]|15 [0|1|2|3|5|6|7|8|9]|18 [0|1|2|3|5|6|7|8|9])\d {8}$
5 电话号码 (“XXX-XXXXXXX”、“XXXX-XXXXXXXX”、“XXX-XXXXXXX”、“XXX-XXXXXXXX”、"XXXXXXX" 和 "XXXXXXXX)：^(\(\d {3,4}-)|\d {3.4}-)?\d {7,8}$
6 国内电话号码 (0511-4405222、021-87888822)：\d {3}-\d {8}|\d {4}-\d {7}
7 身份证号 (15 位、18 位数字)：^\d {15}|\d {18}$
8 短身份证号码 (数字、字母 x 结尾)：^([0-9]){7,18}(x|X)?$ 或 ^\d {8,18}|[0-9x]{8,18}|[0-9X]{8,18}?$
9 帐号是否合法 (字母开头，允许 5-16 字节，允许字母数字下划线)：^[a-zA-Z][a-zA-Z0-9_]{4,15}$
10 密码 (以字母开头，长度在 6~18 之间，只能包含字母、数字和下划线)：^[a-zA-Z]\w {5,17}$
11 强密码 (必须包含大小写字母和数字的组合，不能使用特殊字符，长度在 8-10 之间)：^(?=.*\d)(?=.*[a-z])(?=.*[A-Z]).{8,10}$
12 日期格式：^\d {4}-\d {1,2}-\d {1,2}
13 一年的 12 个月 (01～09 和 1～12)：^(0?[1-9]|1 [0-2])$
14 一个月的 31 天 (01～09 和 1～31)：^((0?[1-9])|((1|2)[0-9])|30|31)$
15 钱的输入格式：
16 1. 有四种钱的表示形式我们可以接受:“10000.00” 和 “10,000.00”, 和没有 “分” 的 “10000” 和 “10,000”：^[1-9][0-9]*$
17 2. 这表示任意一个不以 0 开头的数字，但是，这也意味着一个字符 "0" 不通过，所以我们采用下面的形式：^(0|[1-9][0-9]*)$
18 3. 一个 0 或者一个不以 0 开头的数字。我们还可以允许开头有一个负号：^(0|-?[1-9][0-9]*)$
19 4. 这表示一个 0 或者一个可能为负的开头不为 0 的数字。让用户以 0 开头好了。把负号的也去掉，因为钱总不能是负的吧。下面我们要加的是说明可能的小数部分：^[0-9]+(.[0-9]+)?$
20 5. 必须说明的是，小数点后面至少应该有 1 位数，所以 "10.“是不通过的，但是 “10” 和 “10.2” 是通过的：^[0-9]+(.[0-9]{2})?$
21 6. 这样我们规定小数点后面必须有两位，如果你认为太苛刻了，可以这样：^[0-9]+(.[0-9]{1,2})?$
22 7. 这样就允许用户只写一位小数。下面我们该考虑数字中的逗号了，我们可以这样：^[0-9]{1,3}(,[0-9]{3})*(.[0-9]{1,2})?$
23 8.1 到 3 个数字，后面跟着任意个 逗号 + 3 个数字，逗号成为可选，而不是必须：^([0-9]+|[0-9]{1,3}(,[0-9]{3})*)(.[0-9]{1,2})?$
24 备注：这就是最终结果了，别忘了”+“可以用”" 替代如果你觉得空字符串也可以接受的话 (奇怪，为什么？) 最后，别忘了在用函数时去掉去掉那个反斜杠，一般的错误都在这里
25 xml 文件：^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\.[x|X][m|M][l|L]$
26 中文字符的正则表达式：[\u4e00-\u9fa5]
27 双字节字符：[^\x00-\xff] (包括汉字在内，可以用来计算字符串的长度 (一个双字节字符长度计 2，ASCII 字符计 1))
28 空白行的正则表达式：\n\s*\r (可以用来删除空白行)
29 HTML 标记的正则表达式：<(\S*?)[^>]*>.*?</\1>|<.*? /> (网上流传的版本太糟糕，上面这个也仅仅能部分，对于复杂的嵌套标记依旧无能为力)
30 首尾空白字符的正则表达式：^\s*|\s*$ 或 (^\s*)|(\s*$) (可以用来删除行首行尾的空白字符 (包括空格、制表符、换页符等等)，非常有用的表达式)
31 腾讯 QQ 号：[1-9][0-9]{4,} (腾讯 QQ 号从 10000 开始)
32 中国邮政编码：[1-9]\d {5}(?!\d) (中国邮政编码为 6 位数字)
33 IP 地址：\d+\.\d+\.\d+\.\d+ (提取 IP 地址时有用)
```

## 3.8 MapReduce 开发总结

**1**）输入数据接口：InputFormat

（1）默认使用的实现类是：TextInputFormat

（2）TextInputFormat 的功能逻辑是：一次读一行文本，然后将该行的起始偏移量作为 key，行内容作为 value 返回。

（3）CombineTextInputFormat 可以把多个小文件合并成一个切片处理，提高处理效率。

**2**）逻辑处理接口：Mapper

用户根据业务需求实现其中三个方法：map ()、setup () 和 cleanup ()

**3**）Partitioner 分区

（1）有默认实现 HashPartitioner，逻辑是根据 key 的哈希值和 numReduces 来返回一个分区号；`key.hashCode ()&Integer.MAXVALUE % numReduces`

（2）如果业务上有特别的需求，可以自定义分区。

**4**）Comparable 排序

（1）当我们用自定义的对象作为 key 来输出时，就必须要实现 `WritableComparable` 接口，重写其中的 `compareTo ()` 方法。

（2）部分排序：对最终输出的每一个文件进行内部排序。

（3）全排序：对所有数据进行排序，通常只有一个 Reduce。

（4）二次排序：排序的条件有两个。

**5**）Combiner 合并

Combiner 合并可以提高程序执行效率，减少 IO 传输。但是使用时必须不能影响原有的业务处理结果。

**6**）逻辑处理接口：Reducer

用户根据业务需求实现其中三个方法：reduce ()、setup () 和 cleanup ()

**7**）输出数据接口：OutputFormat

（1）默认实现类是 TextOutputFormat，功能逻辑是：将每一个 KV 对，向目标文本文件输出一行。

（2）用户还可以自定义 OutputFormat。
