# Spark Core实战

思路

开发原则

1. 多什么，删什么（减小数据规模）
2. 缺什么，补什么
3. 功能实现中尽可能少用Shuffle


```java
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.JavaPairRDD;  
import org.apache.spark.api.java.JavaRDD;  
import org.apache.spark.api.java.JavaSparkContext;  
import org.jetbrains.annotations.NotNull;  
import scala.Tuple2;  
  
import java.io.Serializable;  
import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.List;  
  
public class SparkHandsOn {  
    public static void main(String[] args) {  
        // 1.创建配置对象  
        SparkConf conf = new SparkConf();  
        conf.setAppName("Spark01_Env");  
        conf.setMaster("local[*]");  
  
        // 2. 创建sparkContext  
        JavaSparkContext jsc = new JavaSparkContext(conf);  
  
        // 3. 编写代码  
        JavaRDD<String> rdd = jsc.textFile("D:\\Documents\\items\\BigData\\BigDataCode\\spark\\src\\main\\resources\\data\\user_visit_action.txt");  
        // 过滤掉搜索数据，减小数据规模  
        JavaRDD<String> filterRDD = rdd.filter(line -> {  
            String[] items = line.split("_");  
            return "null".equals(items[5]);  
        });  
  
        // 分组统计  
        JavaRDD<CategoryCountInfo> categoryCountInfoJavaRDD = filterRDD.flatMap(line -> {  
            String[] items = line.split("_");  
            if (!"-1".equals(items[6])) {  
                return Arrays.asList(new CategoryCountInfo(items[6], 1L, 0L, 0L)).iterator();  
            } else if (!"null".equals(items[8])) {  
                List<CategoryCountInfo> categoryCountInfoList = new ArrayList<>();  
                String[] ids = items[8].split(",");  
                for (String categoryId : ids) {  
                    categoryCountInfoList.add(new CategoryCountInfo(categoryId, 0L, 1L, 0L));  
                }  
                return categoryCountInfoList.iterator();  
            } else if (!"null".equals(items[10])) {  
                List<CategoryCountInfo> categoryCountInfoList = new ArrayList<>();  
                String[] ids = items[10].split(",");  
                for (String categoryId : ids) {  
                    categoryCountInfoList.add(new CategoryCountInfo(categoryId, 0L, 0L, 1L));  
                }  
                return categoryCountInfoList.iterator();  
            } else {  
                System.out.println("数据格式错误：" + line);  
                return null;  
            }  
        });  
  
        JavaPairRDD<String, CategoryCountInfo> stringCategoryCountInfoJavaPairRDD = categoryCountInfoJavaRDD.  
                mapToPair(pair -> new Tuple2<>(pair.getCategoryId(), pair))  
                .reduceByKey((a, b) -> {  
                    a.setClickCount(a.getClickCount() + b.getClickCount());  
                    a.setOrderCount(a.getOrderCount() + b.getOrderCount());  
                    a.setPayCount(a.getPayCount() + b.getPayCount());  
                    return a;  
                });  
  
        JavaRDD<CategoryCountInfo> mapRDD = stringCategoryCountInfoJavaPairRDD.map(pair -> pair._2);  
        JavaRDD<CategoryCountInfo> sortRDD = mapRDD.sortBy(obj -> obj, false, 2);  
        List<CategoryCountInfo> takeSort = sortRDD.take(10);  
        for (CategoryCountInfo categoryCountInfo : takeSort) {  
            System.out.println(categoryCountInfo);  
        }  
  
        // 4. 关闭sc  
        jsc.stop();  
    }  
}  
  
class CategoryCountInfo implements Comparable<CategoryCountInfo>, Serializable {  
    private String categoryId;  
    private Long clickCount;  
    private Long orderCount;  
    private Long payCount;  
  
    public CategoryCountInfo() {  
    }  
  
    public CategoryCountInfo(String categoryId, Long clickCount, Long orderCount, Long payCount) {  
        this.categoryId = categoryId;  
        this.clickCount = clickCount;  
        this.orderCount = orderCount;  
        this.payCount = payCount;  
    }  
  
    public String getCategoryId() {  
        return categoryId;  
    }  
  
    public void setCategoryId(String categoryId) {  
        this.categoryId = categoryId;  
    }  
  
    public Long getClickCount() {  
        return clickCount;  
    }  
  
    public void setClickCount(Long clickCount) {  
        this.clickCount = clickCount;  
    }  
  
    public Long getOrderCount() {  
        return orderCount;  
    }  
  
    public void setOrderCount(Long orderCount) {  
        this.orderCount = orderCount;  
    }  
  
    public Long getPayCount() {  
        return payCount;  
    }  
  
    public void setPayCount(Long payCount) {  
        this.payCount = payCount;  
    }  
  
    @Override  
    public String toString() {  
        return "CategoryCountInfo{" +  
                "categoryId='" + categoryId + '\'' +  
                ", clickCount=" + clickCount +  
                ", orderCount=" + orderCount +  
                ", payCount=" + payCount +  
                '}';  
    }  
  
    @Override  
    public int compareTo(@NotNull CategoryCountInfo other) {  
        if (this.clickCount > other.clickCount) {  
            return 1;  
        } else if (this.clickCount < other.clickCount) {  
            return -1;  
        } else {  
            if (this.orderCount > other.orderCount) {  
                return 1;  
            } else if (this.orderCount < other.orderCount) {  
                return -1;  
            } else {  
                return this.payCount.compareTo(other.payCount);  
            }  
        }  
    }  
}
```

输出结果如下所示：

```
CategoryCountInfo{categoryId='15', clickCount=6120, orderCount=1672, payCount=1259}
CategoryCountInfo{categoryId='2', clickCount=6119, orderCount=1767, payCount=1196}
CategoryCountInfo{categoryId='20', clickCount=6098, orderCount=1776, payCount=1244}
CategoryCountInfo{categoryId='12', clickCount=6095, orderCount=1740, payCount=1218}
CategoryCountInfo{categoryId='11', clickCount=6093, orderCount=1781, payCount=1202}
CategoryCountInfo{categoryId='17', clickCount=6079, orderCount=1752, payCount=1231}
CategoryCountInfo{categoryId='7', clickCount=6074, orderCount=1796, payCount=1252}
CategoryCountInfo{categoryId='9', clickCount=6045, orderCount=1736, payCount=1230}
CategoryCountInfo{categoryId='19', clickCount=6044, orderCount=1722, payCount=1158}
CategoryCountInfo{categoryId='13', clickCount=6036, orderCount=1781, payCount=1161}
```



