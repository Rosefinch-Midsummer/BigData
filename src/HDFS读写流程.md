# HDFS读写流程

<!-- toc -->

## 写数据

### 文件写入

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102124812.png)

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102124951.png)

（1）客户端通过 Distributed FileSystem 模块向 NameNode 请求上传文件，NameNode 检查目标文件是否已存在，父目录是否存在。

（2）NameNode 返回是否可以上传。

（3）客户端请求第一个 Block 上传到哪几个 DataNode 服务器上。

（4）NameNode 返回 3 个 DataNode 节点，分别为 dn1、dn2、dn3。

（5） 客户端通过 FSDataOutputStream 模块请求 dn1 上传数据， dn1 收到请求会继续调用
dn2，然后 dn2 调用 dn3，将这个通信管道建立完成。

（6）dn1、dn2、dn3 逐级应答客户端。

（7） 客户端开始往 dn1 上传第一个 Block （先从磁盘读取数据放到一个本地内存缓存） ，
以 Packet 为单位，dn1 收到一个 Packet 就会传给 dn2，dn2 传给 dn3；dn1 每传一个 packet
会放入一个应答队列等待应答。

（8）当一个 Block 传输完成之后， 客户端再次请求 NameNode 上传第二个 Block 的服务
器。（重复执行 3-7 步）。

### 网络拓扑-节点距离计算

在 HDFS 写数据的过程中，NameNode 会选择距离待上传数据最近距离的 DataNode 接
收数据。那么这个最近距离怎么计算呢？

**节点距离：两个节点到达最近的共同祖先的距离总和。**

例如，假设有数据中心 d1 机架 r1 中的节点 n1。该节点可以表示为/d1/r1/n1。利用这种
标记，这里给出四种距离描述。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102125235.png)


### 机架感知（副本存储节点选择）

1 ）机架感知说明

（1）官方说明

http://hadoop.apache.org/docs/r3.1.3/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Data_Replication

>For the common case, when the replication factor is three, HDFS’s placement policy is to put one replica on the local machine if the writer is on a datanode, otherwise on a random datanode, another replica on a node in a different (remote) rack, and the last on a different node in the same remote rack. This policy cuts the inter-rack write traffic which generally improves write performance. The chance of rack failure is far less than that of node failure; this policy does not impact data reliability and availability guarantees. However, it does reduce the aggregate network bandwidth used when reading data since a block is placed in only two unique racks rather than three. With this policy, the replicas of a file do not evenly distribute across the racks. One third of replicas are on one node, two thirds of replicas are on one rack, and the other third are evenly distributed across the remaining racks. This policy improves write performance without compromising data reliability or read performance.

（2）源码说明

Crtl + n 查找 BlockPlacementPolicyDefault，在该类中查找 chooseTargetInOrder 方法。

2 ）Hadoop3.1.3 副本节点选择

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102125546.png)

第一个选择最近的节点

第二个节点跨机架保证副本的可靠性

第三个节点还是兼顾效率

## 读数据流程

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102125848.png)

（1）客户端通过 DistributedFileSystem 向 NameNode 请求下载文件，NameNode 通过查询元数据，找到文件块所在的 DataNode 地址。

（2）挑选一台 DataNode服务器，请求读取数据。挑选过程遵循就近原则，然后随机（会考虑当前节点的负载能力）。

（3）DataNode 开始传输数据给客户端，从磁盘里面读取数据输入流，以 Packet 为单位来做校验。

（4）客户端以 Packet 为单位接收，先在本地缓存，然后写入目标文件。