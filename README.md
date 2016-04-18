# spark-alluxio-blockstorage
off-heap alluxio spark-blockstorage

# Usage

```
val conf = new SparkConf()
withDebug(conf)
conf.setAppName("simple Alluxio OFF-HEAP")
	.set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.AlluxioBlockManager")
	.set("spark.externalBlockStore.subDirectories", "8") 
	.set("spark.externalBlockStore.url", "alluxio://hadoop-master2:19998") 
	.set("spark.externalBlockStore.baseDir", "/spark-helloworld") 

val sc = new SparkContext(conf)
val wordcount = sc.textFile(source)
	.flatMap(_.split(" "))
	.map((_, 1))
	.reduceByKey(_ + _, 1)
	.persist(StorageLevel.OFF_HEAP)

val sortedWordcount = wordcount.map(kv => (kv._2, kv._1)).sortByKey(false).map(kv => (kv._2, kv._1))
sortedWordcount.saveAsTextFile(s"$target/sorted")

wordcount.saveAsTextFile(s"$target/simple")
```


提交任务：

```
export SPARK_CLASSPATH=~/alluxio-1.1.0-SNAPSHOT/core/client/target/alluxio-core-client-1.1.0-SNAPSHOT-jar-with-dependencies.jar 

~/spark-1.6.0-bin-2.6.3/bin/spark-submit \
--class HelloWorld \
--master spark://hadoop-master2:7077 \
--jars spark-alluxio-blockstorage-0.1.jar spark-alluxio-blockstorage-0.1-tests.jar
```

# More

Windows本地测试可以通过 conf.setJars(Array) 和 spark.executor.extraClassPath 环境变量指定。更多查看 [HelloWorld.scala](src/test/scala/HelloWorld.scala)


