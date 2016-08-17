package com.leezy.sparkdemo

/**
  * Created by lizhen on 2016/8/14.
  */
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object StreamTest {
  def main(args: Array[String]): Unit = {


    //  val conf = new SparkConf().setMaster("spark://bigdata6:7077")
    //.setAppName("test")
    //.setJars(List("/home/workspace/spark/test/out/artifacts/sparkProject_jar/sparkProject.jar"))
    //.set("spark.executor.memory","20g")
    //.set("spark.eventLog.enabled","true")
    //.set("spark.history.fs.logDirectory","/tmp/spark-events")
    //.set("spark.history.fs.logDirectory","hdfs://bigdata6:8020/user/spark/history")
    //.set("spark.eventLog.dir","hdfs://bigdata6:8020/user/spark/history")
    val session = SparkSession.builder.appName("structWordCount").master("local[3]")
        .config("spark.sql.warehouse.dir","E:\\idea\\spark\\warehouse").getOrCreate()
    import session.implicits._
    val lines = session.readStream.format("socket").option("host", "localhost")
      .option("port", 9999).load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }
}