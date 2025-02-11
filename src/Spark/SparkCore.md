# Spark内核

<!-- toc -->

## 运行流程

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116141936.png)

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116150729.png)


`YarnClusterApplication.class`

```java
package org.apache.spark.deploy.yarn;  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.deploy.SparkApplication;  
import org.apache.spark.internal.config.package.;  
import org.apache.spark.rpc.RpcEnv;  
import scala.reflect.ScalaSignature;  
  
@ScalaSignature(  
    bytes = "\u0006\u0001e2Qa\u0001\u0003\u0001\u00119AQ!\u0007\u0001\u0005\u0002mAQA\b\u0001\u0005B}\u0011a#W1s]\u000ecWo\u001d;fe\u0006\u0003\b\u000f\\5dCRLwN\u001c\u0006\u0003\u000b\u0019\tA!_1s]*\u0011q\u0001C\u0001\u0007I\u0016\u0004Hn\\=\u000b\u0005%Q\u0011!B:qCJ\\'BA\u0006\r\u0003\u0019\t\u0007/Y2iK*\tQ\"A\u0002pe\u001e\u001c2\u0001A\b\u0016!\t\u00012#D\u0001\u0012\u0015\u0005\u0011\u0012!B:dC2\f\u0017B\u0001\u000b\u0012\u0005\u0019\te.\u001f*fMB\u0011acF\u0007\u0002\r%\u0011\u0001D\u0002\u0002\u0011'B\f'o[!qa2L7-\u0019;j_:\fa\u0001P5oSRt4\u0001\u0001\u000b\u00029A\u0011Q\u0004A\u0007\u0002\t\u0005)1\u000f^1siR\u0019\u0001eI\u001a\u0011\u0005A\t\u0013B\u0001\u0012\u0012\u0005\u0011)f.\u001b;\t\u000b\u0011\u0012\u0001\u0019A\u0013\u0002\t\u0005\u0014xm\u001d\t\u0004!\u0019B\u0013BA\u0014\u0012\u0005\u0015\t%O]1z!\tI\u0003G\u0004\u0002+]A\u00111&E\u0007\u0002Y)\u0011QFG\u0001\u0007yI|w\u000e\u001e \n\u0005=\n\u0012A\u0002)sK\u0012,g-\u0003\u00022e\t11\u000b\u001e:j]\u001eT!aL\t\t\u000bQ\u0012\u0001\u0019A\u001b\u0002\t\r|gN\u001a\t\u0003m]j\u0011\u0001C\u0005\u0003q!\u0011\u0011b\u00159be.\u001cuN\u001c4"  
)  
public class YarnClusterApplication implements SparkApplication {  
    public void start(final String[] args, final SparkConf conf) {  
        conf.remove(.MODULE$.JARS());  
        conf.remove(.MODULE$.FILES());  
        conf.remove(.MODULE$.ARCHIVES());  
        (new Client(new ClientArguments(args), conf, (RpcEnv)null)).run();  
    }  
  
    public YarnClusterApplication() {  
    }  
}
```

## 核心对象

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116153352.png)

## 核心对象通信流程——Netty

Java通信方式

Java io=> BIO（阻塞式IO）
- FileInputStream
- FileOutputStream
- Socket

Java nio => NIO + (Linux Epoll)（非阻塞式IO+模拟异步）
- Kafka 
	- FileChannel 
	- Selector

Java AIO => AIO（异步）

Netty使用的就是NIO + (Linux Epoll)方式。


![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116153308.png)


```java
public class Inbox implements Logging {  
    private final String endpointName;  
    private final RpcEndpoint endpoint;  
    @GuardedBy("this")  
    private final LinkedList<InboxMessage> messages;  
    @GuardedBy("this")  
    private boolean stopped;  
    @GuardedBy("this")  
    private boolean enableConcurrent;  
    @GuardedBy("this")  
    private int numActiveThreads;  
    private transient Logger org$apache$spark$internal$Logging$$log_;
}
```

```java
public class Outbox {  
    public final NettyRpcEnv org$apache$spark$rpc$netty$Outbox$$nettyEnv;  
    private final RpcAddress address;  
    @GuardedBy("this")  
    private final LinkedList<OutboxMessage> messages;  
    @GuardedBy("this")  
    private TransportClient org$apache$spark$rpc$netty$Outbox$$client;  
    @GuardedBy("this")  
    private Future<BoxedUnit> org$apache$spark$rpc$netty$Outbox$$connectFuture;  
    @GuardedBy("this")  
    private boolean org$apache$spark$rpc$netty$Outbox$$stopped;  
    @GuardedBy("this")  
    private boolean draining;
}
```

## Task任务的调度执行

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116155452.png)

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116160035.png)

DAGSheduler.java

```java
private ResultStage createResultStage(final RDD<?> rdd, final Function2<TaskContext, Iterator<?>, ?> func, final int[] partitions, final int jobId, final CallSite callSite) {
        Tuple2 var8 = this.getShuffleDependenciesAndResourceProfiles(rdd);
        if (var8 != null) {
            HashSet shuffleDeps = (HashSet)var8._1();
            HashSet resourceProfiles = (HashSet)var8._2();
            Tuple2 var7 = new Tuple2(shuffleDeps, resourceProfiles);
            HashSet shuffleDeps = (HashSet)var7._1();
            HashSet resourceProfiles = (HashSet)var7._2();
            ResourceProfile resourceProfile = this.mergeResourceProfilesForStage(resourceProfiles);
            this.checkBarrierStageWithDynamicAllocation(rdd);
            this.checkBarrierStageWithNumSlots(rdd, resourceProfile);
            this.checkBarrierStageWithRDDChainPattern(rdd, (new ArrayOps.ofInt(scala.Predef..MODULE$.intArrayOps(partitions))).toSet().size());
            List parents = this.getOrCreateParentStages(shuffleDeps, jobId);
            int id = this.nextStageId().getAndIncrement();
            ResultStage stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite, resourceProfile.id());
            this.stageIdToStage().update(BoxesRunTime.boxToInteger(id), stage);
            this.updateJobIdStageIdMaps(jobId, stage);
            return stage;
        } else {
            throw new MatchError(var8);
        }
    }



public void handleJobSubmitted(final int jobId, final RDD<?> finalRDD, final Function2<TaskContext, Iterator<?>, ?> func, final int[] partitions, final CallSite callSite, final JobListener listener, final JobArtifactSet artifacts, final Properties properties) {
        ObjectRef finalStage = ObjectRef.create((Object)null);

        try {
            finalStage.elem = this.createResultStage(finalRDD, func, partitions, jobId, callSite);
        } catch (BarrierJobSlotsNumberCheckFailed var18) {
            int numCheckFailures = BoxesRunTime.unboxToInt(this.barrierJobIdToNumTasksCheckFailures().compute(BoxesRunTime.boxToInteger(jobId), (x$22, value) -> BoxesRunTime.boxToInteger($anonfun$handleJobSubmitted$1(BoxesRunTime.unboxToInt(x$22), BoxesRunTime.unboxToInt(value)))));
            this.logWarning(() -> (new StringBuilder(92)).append("Barrier stage in job ").append(jobId).append(" requires ").append(var18.requiredConcurrentTasks()).append(" slots, ").append("but only ").append(var18.maxConcurrentTasks()).append(" are available. ").append("Will retry up to ").append(this.maxFailureNumTasksCheck() - numCheckFailures + 1).append(" more times").toString());
            if (numCheckFailures <= this.maxFailureNumTasksCheck()) {
                this.messageScheduler().schedule(new Runnable(jobId, finalRDD, func, partitions, callSite, listener, artifacts, properties) {
                    private final int jobId$3;
                    private final RDD finalRDD$1;
                    private final Function2 func$1;
                    private final int[] partitions$1;
                    private final CallSite callSite$2;
                    private final JobListener listener$1;
                    private final JobArtifactSet artifacts$1;
                    private final Properties properties$1;

                    public void run() {
                        this.$outer.eventProcessLoop().post(new JobSubmitted(this.jobId$3, this.finalRDD$1, this.func$1, this.partitions$1, this.callSite$2, this.listener$1, this.artifacts$1, this.properties$1));
                    }

                    public {
                        if (DAGScheduler.this == null) {
                            throw null;
                        } else {
                            this.$outer = DAGScheduler.this;
                            this.jobId$3 = jobId$3;
                            this.finalRDD$1 = finalRDD$1;
                            this.func$1 = func$1;
                            this.partitions$1 = partitions$1;
                            this.callSite$2 = callSite$2;
                            this.listener$1 = listener$1;
                            this.artifacts$1 = artifacts$1;
                            this.properties$1 = properties$1;
                        }
                    }
                }, this.timeIntervalNumTasksCheck(), TimeUnit.SECONDS);
                return;
            }

            this.barrierJobIdToNumTasksCheckFailures().remove(BoxesRunTime.boxToInteger(jobId));
            listener.jobFailed(var18);
            return;
        } catch (Exception var19) {
            this.logWarning(() -> (new StringBuilder(50)).append("Creating new stage failed due to exception - job: ").append(jobId).toString(), var19);
            listener.jobFailed(var19);
            return;
        }

        this.barrierJobIdToNumTasksCheckFailures().remove(BoxesRunTime.boxToInteger(jobId));
        ActiveJob job = new ActiveJob(jobId, (ResultStage)finalStage.elem, callSite, listener, artifacts, properties);
        this.clearCacheLocs();
        this.logInfo(() -> (new StringOps(scala.Predef..MODULE$.augmentString("Got job %s (%s) with %d output partitions"))).format(scala.Predef..MODULE$.genericWrapArray(new Object[]{BoxesRunTime.boxToInteger(job.jobId()), callSite.shortForm(), BoxesRunTime.boxToInteger(partitions.length)})));
        this.logInfo(() -> (new StringBuilder(16)).append("Final stage: ").append((ResultStage)finalStage.elem).append(" (").append(((ResultStage)finalStage.elem).name()).append(")").toString());
        this.logInfo(() -> (new StringBuilder(24)).append("Parents of final stage: ").append(((ResultStage)finalStage.elem).parents()).toString());
        this.logInfo(() -> (new StringBuilder(17)).append("Missing parents: ").append(this.getMissingParentStages((ResultStage)finalStage.elem)).toString());
        long jobSubmissionTime = this.clock.getTimeMillis();
        this.jobIdToActiveJob().update(BoxesRunTime.boxToInteger(jobId), job);
        this.activeJobs().$plus$eq(job);
        ((ResultStage)finalStage.elem).setActiveJob(job);
        int[] stageIds = (int[])((TraversableOnce)this.jobIdToStageIds().apply(BoxesRunTime.boxToInteger(jobId))).toArray(scala.reflect.ClassTag..MODULE$.Int());
        StageInfo[] stageInfos = (StageInfo[])(new ArrayOps.ofInt(scala.Predef..MODULE$.intArrayOps(stageIds))).flatMap((id) -> $anonfun$handleJobSubmitted$8(this, BoxesRunTime.unboxToInt(id)), scala.Array..MODULE$.canBuildFrom(scala.reflect.ClassTag..MODULE$.apply(StageInfo.class)));
        this.listenerBus.post(new SparkListenerJobStart(job.jobId(), jobSubmissionTime, scala.Predef..MODULE$.wrapRefArray((Object[])stageInfos), org.apache.spark.util.Utils..MODULE$.cloneProperties(properties)));
        this.submitStage((ResultStage)finalStage.elem);
    }
```

`Tuple2 var8 = this.getShuffleDependenciesAndResourceProfiles(rdd);`

Pool.class

```java
public Pool(final String poolName, final Enumeration.Value schedulingMode, final int initMinShare, final int initWeight) {
        label30: {
            Object var11;
            label29: {
                label32: {
                    this.poolName = poolName;
                    this.schedulingMode = schedulingMode;
                    super();
                    Logging.$init$(this);
                    this.schedulableQueue = new ConcurrentLinkedQueue();
                    this.schedulableNameToSchedulable = new ConcurrentHashMap();
                    this.weight = initWeight;
                    this.minShare = initMinShare;
                    this.runningTasks = 0;
                    this.priority = 0;
                    this.stageId = -1;
                    this.name = poolName;
                    this.parent = null;
                    Enumeration.Value var10001 = org.apache.spark.scheduler.SchedulingMode..MODULE$.FAIR();
                    if (var10001 == null) {
                        if (schedulingMode == null) {
                            break label32;
                        }
                    } else if (var10001.equals(schedulingMode)) {
                        break label32;
                    }

                    var10001 = org.apache.spark.scheduler.SchedulingMode..MODULE$.FIFO();
                    if (var10001 == null) {
                        if (schedulingMode != null) {
                            break label30;
                        }
                    } else if (!var10001.equals(schedulingMode)) {
                        break label30;
                    }

                    var11 = new FIFOSchedulingAlgorithm();
                    break label29;
                }

                var11 = new FairSchedulingAlgorithm();
            }

            this.taskSetSchedulingAlgorithm = (SchedulingAlgorithm)var11;
            return;
        }
```

## Shuffle底层的实现原理

以Task为单位可能存在数据安全问题、小文件过多问题。

以CPU核为单位也存在小文件过多问题。

实际：以Task为单位，但尽量避免可能存在的数据安全问题。

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116164756.png)

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116164729.png)


![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116163109.png)

SortShuffleManager.class

```java
public <K, V, C> ShuffleHandle registerShuffle(final int shuffleId, final ShuffleDependency<K, V, C> dependency) {
        if (.MODULE$.shouldBypassMergeSort(this.conf, dependency)) {
            return new BypassMergeSortShuffleHandle(shuffleId, dependency);
        } else {
            return (ShuffleHandle)(SortShuffleManager$.MODULE$.canUseSerializedShuffle(dependency) ? new SerializedShuffleHandle(shuffleId, dependency) : new BaseShuffleHandle(shuffleId, dependency));
        }
    }
```


![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116164652.png)


![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116164841.png)

`reduceByKey()`有预聚合

`groupByKey()`没有预聚合

## 内存管理


![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116165936.png)

![](https://cdn.jsdelivr.net/gh/Rosefinch-Midsummer/MyImagesHost04/img20250116181019.png)

堆外内存：JVM管理不了（操作系统）的内存，称之为堆外内存(Unsafe)

堆内内存：JVM管理的内存，称之为堆内内存

Spark封装了堆外内存和堆内内存。

非堆内存：栈内存，方法区内存