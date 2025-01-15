# HDFS的Shell操作

<!-- toc -->

## 2.1 基本语法

`hadoop fs 具体命令` 和 `hdfs dfs 具体命令`两者等价。

## 2.2 命令大全

执行`bin/hadoop fs`或`hdfs dfs`查看所有相关命令。

## 2.3 常用命令实操

### 2.3.1 准备工作

1）启动 Hadoop 集群（方便后续的测试）

其实如果看了之前的Hadoop入门(十三)——集群常用知识(面试题)与技巧总结里面写了一个快速启动集群的脚本，只需要一个命令即可启动集群

```
[leokadia@hadoop102 bin]$ myhadoop.sh start
```

或者也可以像之前一样分别在102，103上使用以下两个命令：

```
[leokadia@hadoop102 hadoop-3.1.3]$ sbin/start-dfs.sh
[leokadia@hadoop103 hadoop-3.1.3]$ sbin/start-yarn.sh
```

2）-help：输出这个命令参数

如果对哪一个命令的用法不是特别清楚，用以下指令查看该命令如何使用的：

`hadoop fs -help rm`

3）创建/Marvel 文件夹

`hadoop fs -mkdir /Marvel`

打开之后就有了一个漫威的文件夹

后续的命令就在漫威的文件夹里面开启我们的漫威之旅

一共分三大类命令：上传、下载、HDFS直接操作

### 2.3.2 上传

注意以下命令执行目录都是`hadoop-3.1.3`，（这里其实可以是任何路径）

1）`-moveFromLocal`：从本地移动到 HDFS

在虚拟机中执行`vim Avengers.txt`创建一个txt文件

输入：The Avengers

执行`hadoop fs -moveFromLocal ./Avengers.txt /Marvel`将其上传到HDFS

源文件：Avengers.txt

目的目录：Marvel

2）`-copyFromLocal`：从本地文件系统中拷贝文件到 HDFS 路径去

`hadoop fs -copyFromLocal X-Men.txt /Marvel`

3）`-put`：等同于 -`copyFromLocal`，生产环境更习惯用 `put`

`hadoop fs -put ./Fantastic_Four.txt /Marvel`

4）`-appendToFile`：追加一个文件到已经存在的文件末尾

HDFS只能追加，不允许随机修改，而且只能在文件的末尾进行追加

`hadoop fs -appendToFile Iron_Man.txt /Marvel/Avengers.txt`

### 2.3.3 下载

注意以下命令执行目录都是`hadoop-3.1.3`

1）`-copyToLocal`：从 HDFS 拷贝到本地

将Marvel中的Averagers.txt拷贝到当前文件夹

`hadoop fs -copyToLocal /Marvel/Avengers.txt ./`


2）`-get`：等同于 `-copyToLocal`，生产环境更习惯用 `get`

将Marvel中的Averagers.txt拷贝到当前文件夹，并将拷贝的文件名更改为The_Avengers.txt

`hadoop fs -get /Marvel/Avengers.txt ./The_Avengers.txt`

### 2.3.4 HDFS 直接操作

注意以下命令执行目录都是`hadoop-3.1.3`（任何路径都可以）

1）-ls: 显示目录信息

查询根目录

`hadoop fs -ls /`

查询Marvel目录

`hadoop fs -ls /Marvel`

2）-cat：显示文件内容

`hadoop fs -cat /Marvel/Avengers.txt`

3）-chgrp、-chmod、-chown：Linux 文件系统中的用法一样，修改文件所属权限

`hadoop fs -chmod 666 /Marvel/Avengers.txt`

`hadoop fs -chown leokadia:leokadia /Marvel/Avengers.txt`

4）-mkdir：创建路径

`hadoop fs -mkdir /DC`

再建一个Disney文件夹（因为后来漫威被迪士尼收购了）

`hadoop fs -mkdir /Disney`

5）-cp：从 HDFS 的一个路径拷贝到 HDFS 的另一个路径

`hadoop fs -cp /Marvel/Avengers.txt /Disney`

这个命令是拷贝，也就是说复仇者联盟在漫威中还有一份

6）-mv：在 HDFS 目录中移动文件

`hadoop fs -mv /Marvel/Fantastic_Four.txt /Disney`

`hadoop fs -mv /Marvel/X-Men.txt /Disney`

成功移动，注意，这个是移动，所以原来的文件夹里这两个文件没有了。

7）-tail：显示一个文件的末尾 1kb 的数据

`hadoop fs -tail /Marvel/Avengers.txt`

8）-rm：删除文件或文件夹

`hadoop fs -rm /Marvel/Avengers.txt`

9）-rm -r：递归删除目录及目录里面内容

删除文件夹及里面的内容

`hadoop fs -rm -r /Marvel`

没有Marvel文件夹了，呜呜呜。。。

注意：用 rm-r 之类的删除命令一定要慎重，慎重，再慎重！！！

10）-du 统计文件夹的大小信息

`hadoop fs -du -s -h /Disney`

说明：43 表示文件大小；129 表示 43\*3 个副本；/Disney 表示查看的目录

`hadoop fs -du -h /Disney`

22+15+6=43 所以Disney文件总大小为43

11）-setrep：设置 HDFS 中文件的副本数量

给Avengers.txt设置10个副本：

`hadoop fs -setrep 10 /Disney/Avengers.txt`

这里设置的副本数只是记录在 NameNode 的元数据中， 是否真的会有这么多副本， 还得看DataNode 的数量。因为目前只有 3 台设备，最多也就 3 个副本，只有节点数的增加到 10台时，副本数才能达到 10。

