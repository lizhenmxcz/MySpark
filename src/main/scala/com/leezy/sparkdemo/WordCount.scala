package com.leezy.sparkdemo

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 16-7-6.
  */
  object WordCount{
    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("spark://bigdata6:7077")
                                .setAppName("test")
                                .setJars(List("/home/workspace/spark/test/out/artifacts/sparkProject_jar/sparkProject.jar"))
                                .set("spark.executor.memory","20g")
                                .set("spark.eventLog.enabled","true")
        //.set("spark.history.fs.logDirectory","/tmp/spark-events")
        //.set("spark.history.fs.logDirectory","hdfs://bigdata6:8020/user/spark/history")
        .set("spark.eventLog.dir","hdfs://bigdata6:8020/user/spark/history")
      val sc = new SparkContext(conf)

      val lines = sc.textFile("hdfs://bigdata6:8020/user/lizhen/abc")
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map((_, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      wordCounts.collect().foreach(println(_))
  }
  }
