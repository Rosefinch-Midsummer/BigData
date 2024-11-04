# HDFS的API操作

<!-- toc -->

刚刚（二）讲的是用Shell/Hadoop fs/HDFS/dfs的一些相关操作，相当于是在集群内部，跟集群的一些客户端打交道，这章讲的是：我们希望在Windows环境（办公环境）对远程的集群进行一个客户端访问，于是现在就在Windows环境上写代码，写HDFS客户端代码，远程连接上集群，对它们进行增删改查相关操作。

## 3.1 客户端环境准备

想让我们的windows能够连接上远程的Hadoop集群，windows里面也得有相关的环境变量

1） 下载 hadoop-3.1.0 （windows版）到非中文路径 （比如E:\Sofware）

2 ） 配置 HADOOP_HOME 环境 变量

3 ） 配置 Path 环境 变量。

将HADOOP_HOME目录添加到对应的PATH目录

注意： 如果环境变量不起作用，可以 重启电脑 试试。

验证 Hadoop 环境变量是否正常。双击 winutils.exe，如果报如下错误。

说明缺少微软运行库 （正版系统往往有这个问题） 。 下载微软运行库安装包双击安装即可。

4 ） 在 IDEA 中 创建一个 Maven 工程 HdfsClientDemo ，并导入相应的依赖坐标+ 日志

创建Maven工程以及进行相关配置

5 ） 创建包名 ：com.leokadia.hdfs

6 ） 创建 HdfsClient 类

创建好了客户端类，接下来写代码操作远程的服务器集群集群

7 ） 执行 程序

客户端去操作 HDFS 时，是有一个用户身份的。默认情况下，HDFS 客户端 API 会从采用 Windows 默认用户访问 HDFS，会报权限异常错误。所以在访问 HDFS 时，一定要配置用户。


## 3.2 HDFS 的 API 案例实操

### 3.2.1 用客户端远程创建目录

```java
package com.leokadia.hdfs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author sa
 * @create 2021-05-04 16:37
 *
 *
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS zookeeper
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {

        //连接集群的nn地址
        URI uri = new URI("hdfs://hadoop102:8020");

        //创建一个配置文件
        Configuration configuration = new Configuration();

        //用户
        String user = "leokadia";

        // 1 获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testmkdir() throws URISyntaxException,IOException,InterruptedException {
        // 2 创建一个文件夹
        fs.mkdirs(new Path("/Marvel/Avengers"));

    }
}
```

运行@Test

成功创建文件夹

### 3.2.2 HDFS 用客户端上传文件（测试 参数优先级 ）

1）上传文件

先在D盘根目录下创建一个待上传的文件

在刚刚的代码中加入如下代码

```java
 // 上传
    @Test
    public void testPut() throws IOException {
        //参数解读：参数一：表示删除原数据；参数二：是否允许覆盖；参数三：原数据路径；参数四：目的地路径
        fs.copyFromLocalFile(false,false,new Path("D:\\Iron_Man.txt"),new Path("hdfs://hadoop102/Marvel/Avengers"));
    }

```

点击运行

2 ） 将 hdfs-site.xml 拷贝到项目的 resources 资源目录下

已知服务器的默认配置 （xxx-default.xml） 中的副本数是3，现在resources下新建一个file——hdfs-site.xml修改副本数，测试二者的优先级

在resources下新建一个file——hdfs-site.xml

将下面代码粘贴到里面：（修改副本数为1）

```xml
<?xml version="1.0" encoding="UTF-8"?> 
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?> 
 
<configuration> 
  <property> 
   <name>dfs.replication</name>       
    <value>1</value> 
  </property> 
</configuration> 
```
 
如果再上传一个文件，它的副本数为1，说明 resources 资源目录下的hdfs-site.xml 优先级高
再创建一个测试文件

执行上传，发现副本数为1

说明在项目资源目录下用户自定义的配置文件高

再测试客户端代码中配置副本的值的优先级：

在源代码中加上：

```java
configuration.set("dfs.replication","2");
```

再运行一遍刚刚的代码

发现Spider_Man.txt副本数变为2了

说明客户端代码中设置的值 >ClassPath 下的用户自定义配置文件

3 ）总结： 参数 优先级

参数优先级排序：

（1）客户端代码中设置的值 >
（2）ClassPath 下的用户自定义配置文件 >
（3） 然后是服务器的自定义配置 （xxx-site.xml） >
（4） 服务器的默认配置 （xxx-default.xml）

### 3.2.3 HDFS 文件下载

```java
// 文件下载
    @Test
    public void testGet() throws IOException {
        //参数解读：参数一： boolean delSrc 指是否将原文件删除；参数二：Path src 指要下载的原文件路径
        // 参数三：Path dst 指将文件下载到的目标地址路径；参数四：boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/Marvel/Avengers/Iron_Man.txt"), new Path("D:\\Robert.txt"), false);
    }

```

### 3.2.4 HDFS 删除文件和目录

```java
 // 删除
    @Test
    public void testRm() throws IOException {

        // 参数解读：参数1：要删除的路径； 参数2 ： 是否递归删除
        // 删除文件(不再演示了)
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"),false);

        // 删除空目录
        fs.delete(new Path("/delete_test_empty"), false);

        // 删除非空目录
        fs.delete(new Path("/Marvel"), true);
    }

```

### 3.2.5 文件的更名和移动

新建一个测试文件夹和相应的测试文件

```java
// 文件的更名和移动
    @Test
    public void testmv() throws IOException {
        // 参数解读：参数1 ：原文件路径； 参数2 ：目标文件路径
        // 对文件名称的修改
        fs.rename(new Path("/move/from.txt"), new Path("/move/new.txt"));

        // 文件的移动和更名
        fs.rename(new Path("/move/new.txt"),new Path("/to.txt"));

        // 目录更名
        fs.rename(new Path("/move"), new Path("/shift"));

    }

```


### 3.2.6 通过客户端的方式获取 HDFS 文件 详情信息

查看文件名称、权限、长度、块信息

```java
// 获取文件详细信息
    @Test
    public void fileDetail() throws IOException {

        // 获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("==========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));

        }
    }

```




### 3.2.7 HDFS 文件和文件夹判断

```java
// 判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {

            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            } else {
                System.out.println("目录：" + status.getPath().getName());
            }
        }
    }

```

