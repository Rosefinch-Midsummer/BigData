# MapReduce

<!-- toc -->

## 概览

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img/20241102144251.png)

[完整博文来源](https://yangmour.github.io/2022/11/02/%E5%A4%A7%E6%95%B0%E6%8D%AE/%E5%A4%A7%E6%95%B0%E6%8D%AE%E6%90%AD%E5%BB%BA%E7%8E%AF%E5%A2%83/hadoop-3.1.3/04_%E5%B0%9A%E7%A1%85%E8%B0%B7%E5%A4%A7%E6%95%B0%E6%8D%AE%E6%8A%80%E6%9C%AF%E4%B9%8BHadoop%EF%BC%88MapReduce%EF%BC%89V3.3/)

## MapReduce 开发总结

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

## 常见错误及解决方案

1）导包容易出错。尤其 Text 和 CombineTextInputFormat。

2）Mapper 中第一个输入的参数必须是 LongWritable 或者 NullWritable，不可以是 IntWritable. 报的错误是类型转换异常。

3）java.lang.Exception: java.io.IOException: Illegal partition for 13926435656 (4)，说明 Partition 和 ReduceTask 个数没对上，调整 ReduceTask 个数。

4）如果分区数不是 1，但是 reducetask 为 1，是否执行分区过程。答案是：不执行分区过程。因为在 MapTask 的源码中，执行分区的前提是先判断 ReduceNum 个数是否大于 1。不大于 1 肯定不执行。

5）在 Windows 环境编译的 jar 包导入到 Linux 环境中运行，

```
hadoop jar wc.jar com.atguigu.mapreduce.wordcount.WordCountDriver/user/atguigu//user/atguigu/output
```

报如下错误：

```
Exception in thread “main” java.lang.UnsupportedClassVersionError: com/atguigu/mapreduce/wordcount/WordCountDriver : Unsupported major.minor version 52.0
```

原因是 Windows 环境用的 jdk1.7，Linux 环境用的 jdk1.8。

解决方案：统一 jdk 版本。

6）缓存 `pd.txt` 小文件案例中，报找不到 `pd.txt` 文件

原因：大部分为路径书写错误。还有就是要检查 `pd.txt.txt` 的问题。还有个别电脑写相对路径找不到 `pd.txt`，可以修改为绝对路径。

7）报类型转换异常。

通常都是在驱动函数中设置 `Map` 输出和最终输出时编写错误。

Map 输出的 key 如果没有排序，也会报类型转换异常。

8）集群中运行 `wc.jar` 时出现了无法获得输入文件。

原因：`WordCount` 案例的输入文件不能放用 `HDFS` 集群的根目录。

9）出现了如下相关异常

```
Exception in thread "main" java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0 (Ljava/lang/String;I) Z  
	at org.apache.hadoop.io.nativeio.NativeIO$Windows.access0 (Native Method)  
	at org.apache.hadoop.io.nativeio.NativeIO$Windows.access (NativeIO.java:609)  
	at org.apache.hadoop.fs.FileUtil.canRead (FileUtil.java:977)  
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.  
	at org.apache.hadoop.util.Shell.getQualifiedBinPath (Shell.java:356)  
	at org.apache.hadoop.util.Shell.getWinUtilsPath (Shell.java:371)  
	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:364)
```

解决方案：拷贝 `hadoop.dll` 文件到 `Windows` 目录 `C:\Windows\System32`。个别同学电脑还需要修改 `Hadoop` 源码。

方案二：创建包 `org.apache.hadoop.io.nativeio`，并将 NativeIO.java 拷贝到该包名下

[详情参考 org.apache.hadoop.io.nativeio.NativeIO$Windows.access0 (Ljava/lang/String;I) Z 的解决办法](https://blog.csdn.net/syl_ccc/article/details/105946007)

10）自定义 Outputformat 时，注意在 RecordWirter 中的 close 方法必须关闭流资源。否则输出的文件内容中数据为空。

```java
@Override  
public void close (TaskAttemptContext context) throws IOException, InterruptedException {  
		if (atguigufos != null) {  
			atguigufos.close ();  
		}  
		if (otherfos != null) {  
			otherfos.close ();  
		}  
}
```



