import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkConf, SparkContext}

object HelloWorld extends Logging {

  val hadoopConf = new org.apache.hadoop.conf.Configuration
  val hdfs = hadoopConf.get("fs.defaultFS")

  val ide = false
  val debug = true

  val source = "hdfs:///simple-alluxion/in"
  val target = "hdfs:///simple-alluxion/out"

  def withDebug(conf: SparkConf): Unit = {
    if (debug) {
      System.setProperty("HADOOP_USER_NAME", "hadoop")

      import org.apache.hadoop.fs.{FileSystem, Path}
      FileSystem.get(hadoopConf).delete(new Path(s"$target"), true)

      conf.set("spark.eventLog.enabled", "true")
          .set("spark.eventLog.dir", s"$hdfs/spark-eventlogs")
          .set("spark.testing", "true")
          .set("spark.executor.memory", "256m") // @UnifiedMemoryManager#getMaxMemory
          .set("spark.driver.memory", "256m")
          .set("spark.driver.host", "192.168.191.1") // 主机多IP的情况下，最好指定和集群同一个网络的IP

      if (ide) {
        conf.setMaster("local")
      } else {
        // first build jar
        //        import scala.sys.process._
        //        "cmd /C mvn package jar:test-jar -DskipTests".!

        conf.setMaster("spark://hadoop-master2:7077")
            .setJars(Array("target/spark-alluxio-blockstorage-0.1.jar", "target/spark-alluxio-blockstorage-0.1-tests.jar"))
            // 绝对路径！！
            .set("spark.executor.extraClassPath", "/home/hadoop/alluxio-1.1.0-SNAPSHOT/core/client/target/alluxio-core-client-1.1.0-SNAPSHOT-jar-with-dependencies.jar")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    withDebug(conf)
    conf.setAppName("simple Alluxio OFF-HEAP")
        .set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.AlluxioBlockManager")
        .set("spark.externalBlockStore.subDirectories", "8") // default 64
        .set("spark.externalBlockStore.url", "alluxio://hadoop-master2:19998") // default alluxio://localhost:19998
        .set("spark.externalBlockStore.baseDir", "/spark-helloworld") // default /tmp_spark_alluxio
    val sc = new SparkContext(conf)

    import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
    val config = sc.hadoopConfiguration
    config.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)

    val wordcount = sc.textFile(source)
        .flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _, 1)
        .persist(StorageLevel.OFF_HEAP)

    val sortedWordcount = wordcount.map(kv => (kv._2, kv._1)).sortByKey(false).map(kv => (kv._2, kv._1))
    sortedWordcount.saveAsTextFile(s"$target/sorted")

    wordcount.saveAsTextFile(s"$target/simple")

  }

}
