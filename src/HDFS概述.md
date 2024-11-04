# HDFS 概述

<!-- toc -->

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241027142704.png)

## HDFS 产出背景及定义

### 1 ）HDFS 产生背景

随着数据量越来越大， 在一个操作系统存不下所有的数据， 那么就分配到更多的操作系统管理的磁盘中，但是不方便管理和维护，迫切需要一种系统来管理多台机器上的文件，这就是分布式文件管理系统。HDFS 只是分布式文件管理系统中的一种。

### 2 ）HDFS 定义

HDFS（Hadoop Distributed File System），它是一个文件系统，用于存储文件，通过目录树来定位文件；其次，它是分布式的，由很多服务器联合起来实现其功能，集群中的服务器有各自的角色。

HDFS 的使用场景：**适合一次写入，多次读出的场景。 一个文件经过创建、写入和关闭之后就不需要改变**。

## HDFS 优缺点

HDFS优点

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241027144347.png)

HDFS缺点

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241027144402.png)

## HDFS 组成架构

HDFS组成架构示意图

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241027145753.png)

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241027145821.png)

简单形容：NameNode是老板，DataNode是打工仔，Client是客户，Secondary NameNode是秘书

## HDFS 文件块大小 （面试重点）

HDFS 文件块大小

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241027145939.png)

思考：为什么块的大小不能设置太小，也不能设置太大？

（1）HDFS的块设置太小，会增加寻址时间，程序一直在找块的开始位置；
（2）如果块设置的太大，从磁盘传输数据的时间会明显大于定位这个块开始位置所需的时间。导致程序在处理这块数据时，会非常慢。

总结：**HDFS块的大小设置主要取决于磁盘传输速率**。

e.g.

普通的机械硬盘，传输速率100M/s →HDFS块设置为128M

固态硬盘，传输速率200-300M/s →HDFS块设置为256M

## NameNode 和 SecondaryNameNode

### NN 和 2NN 工作机制

思考：NameNode 中的元数据是存储在哪里的？

首先，我们做个假设，如果存储在 NameNode 节点的磁盘中，因为经常需要进行随机访问，还有响应客户请求，必然是效率过低。因此，元数据需要存放在内存中。但如果只存在内存中，一旦断电，元数据丢失，整个集群就无法工作了。因此产生在磁盘中备份元数据的FsImage。

这样又会带来新的问题，当在内存中的元数据更新时，如果同时更新 FsImage，就会导致效率过低，但如果不更新，就会发生一致性问题，一旦 NameNode 节点断电，就会产生数据丢失。因此，引入 Edits 文件（只进行追加操作，效率很高）。每当元数据有更新或者添加元数据时，修改内存中的元数据并追加到 Edits 中。这样，一旦 NameNode 节点断电，可以通过 FsImage 和 Edits 的合并，合成元数据。 但是，如果长时间添加数据到 Edits 中，会导致该文件数据过大，效率降低，而且一旦断电，恢复元数据需要的时间过长。因此，需要定期进行 FsImage 和 Edits 的合并，如果这个操作由NameNode节点完成， 又会效率过低。 因此， **引入一个新的节点SecondaryNamenode，专门用于 FsImage 和 Edits 的合并。**

![a](https://i-blog.csdnimg.cn/blog_migrate/d99676a51208185f6f4c380d6f1e9354.png)

### NameNode工作机制

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102132550.png)

1 ） 第一阶段：NameNode 启动

（1）第一次启动 NameNode 格式化后，创建 Fsimage 和 Edits 文件。如果不是第一次启动，直接加载编辑日志和镜像文件到内存。

（2）客户端对元数据进行增删改的请求。

（3）NameNode 记录操作日志，更新滚动日志。

（4）NameNode 在内存中对元数据进行增删改。

2 ） 第二阶段：Secondary NameNode 工作

（1）Secondary NameNode 询问 NameNode 是否需要 CheckPoint。直接带回 NameNode是否检查结果。

（2）Secondary NameNode 请求执行 CheckPoint。

（3）NameNode 滚动正在写的 Edits 日志。

（4）将滚动前的编辑日志和镜像文件拷贝到 Secondary NameNode。

（5）Secondary NameNode 加载编辑日志和镜像文件到内存，并合并。

（6）生成新的镜像文件 fsimage.chkpoint。

（7）拷贝 fsimage.chkpoint 到 NameNode。

（8）NameNode 将 fsimage.chkpoint 重新命名成 fsimage。

### Fsimage 和 Edits 解析

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102132707.png)

查看 oiv 和 oev 命令说明

```
oiv            apply the offline fsimage viewer to an fsimage 
oev            apply the offline edits viewer to an edits file 
```

1 ）oiv 查看 Fsimage 文件

（1）基本语法

```
hdfs oiv -p 文件类型 -i 镜像文件 -o 转换后文件输出路径
```

（2）案例实操

```
hdfs oiv -p XML -i fsimage_0000000000000000261 -o /opt/module/hadoop-3.1.3/fsimage.xml
```

思考：Fsimage 中没有记录块所对应 DataNode，为什么？

在集群启动后，要求 DataNode 上报数据块信息，并间隔一段时间后再次上报。

2 ）oev 查看 Edits 文件

（1）基本语法

```
hdfs oev -p 文件类型 -i 编辑日志 -o 转换后文件输出路径
```

（2）案例实操

```
hdfs oev -p XML -i edits_inprogress_0000000000000000262 -o/opt/module/hadoop-3.1.3/edits.xml
```

思考：NameNode 如何确定下次开机启动的时候合并哪些 Edits？

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102133225.png)

注意时间，看到每间隔1h进行一次合并

集群一停止、开关机也要合并一次

### CheckPoint 时间设置

1 ） 通常情况下，SecondaryNameNode 每隔一小时执行一次

参照：hdfs-default.xml

```xml
<property> 
  <name>dfs.namenode.checkpoint.period</name> 
  <value>3600s</value> 
</property> 
```

2 ） 一分钟检查一次操作次数，当操作次数达到 1 百万时，SecondaryNameNode执行一次

```xml
<property> 
  <name>dfs.namenode.checkpoint.txns</name> 
  <value>1000000</value> 
<description>操作动作次数</description> 
</property> 
 
<property> 
  <name>dfs.namenode.checkpoint.check.period</name> 
  <value>60s</value> 
<description> 1 分钟检查一次操作次数</description> 
</property> 
```

## DataNode

### DataNode 工作机制

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102130953.png)

（1）一个数据块在 DataNode 上以文件形式存储在磁盘上，包括两个文件：一个是数据本身，一个是元数据包括数据块的长度、块数据的校验和以及时间戳。

（2）DataNode 启动后向 NameNode 注册，通过后，周期性（6 小时）的向 NameNode 上报所有的块信息。

DN 向 NN 汇报当前解读信息的时间间隔，默认 6 小时。

相关配置参数如下：

```xml
<property> 
  <name>dfs.blockreport.intervalMsec</name> 
  <value>21600000</value> 
  <description>Determines block reporting interval in 
milliseconds.</description> 
</property> 
```

注意：这里value值单位是毫秒

DN 扫描自己节点块信息列表的时间，默认 6 小时

相关配置参数如下：

```xml
<property> 
  <name>dfs.datanode.directoryscan.interval</name> 
  <value>21600s</value> 
  <description>Interval in seconds for Datanode to scan data 
  directories and reconcile the difference between blocks in memory and on 
the disk. 
  Support multiple time unit suffix(case insensitive), as described 
  in dfs.heartbeat.interval. 
  </description> 
</property> 
```

注意：这里value值单位是秒

（3）心跳是每 3 秒一次，心跳返回结果带有 NameNode 给该 DataNode 的命令如复制块数据到另一台机器 或删除某个数据块。 如果超过 10 分钟+30 秒没有收到某个 DataNode 的心跳，则认为该节点不可用（挂了），不会再向其传输信息。

（4）集群运行中可以安全加入和删除一些机器。

### 数据完整性

思考：DataNode 节点上的数据损坏了，却没有发现，是否也很危险，那么如何解决呢？

如下是 DataNode 节点保证数据完整性的方法。

（1）当 DataNode 读取 Block 的时候，它会计算 CheckSum。

（2）如果计算后的 CheckSum，与 Block 创建时值不一样，说明 Block 已经损坏。

（3）Client 读取其他 DataNode 上的 Block。

（4）常见的校验算法 crc（32），md5（128），sha1（160）

（5）DataNode 在其文件创建后周期验证 CheckSum。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102131756.png)


### DataNode掉线时限参数设置

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102131954.png)


相关配置参数如下：

```xml
<property> 
    <name>dfs.namenode.heartbeat.recheck-interval</name> 
    <value>300000</value> 
</property> 
 
<property> 
    <name>dfs.heartbeat.interval</name> 
    <value>3</value> 
</property> 
```

需要注意的是 hdfs-site.xml 配置文件中的 heartbeat.recheck.interval 的单位为毫秒，dfs.heartbeat.interval 的单位为秒。

DataNode 被中止之后，可以执行`hdfs --daemon start datanode`重启

