package com.stdatalabs.SparkSecondarySort

/*#############################################################################################
# Description: WordCount using Spark
##
# Input: 
#   1. /user/cloudera/person.tsv
#
# To Run this code use the command:    
# spark-submit --class com.stdatalabs.SparkSecondarySort.Driver \
#							 --master yarn-client \
#							 --num-executors 5 \
#							 --driver-memory 4g \
#							 --executor-memory 4g \
#							 --executor-cores 1 
#							 SparkSecondarySort-0.0.1-SNAPSHOT.jar \
#							 /user/cloudera/person.tsv \
#							 /user/cloudera/SparkSecondarySort_op
#############################################################################################*/

// Scala Imports
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.hive.HiveContext

object Driver {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark - Secondary Sort")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile(args(0))
    val pairsRDD = personRDD.map(_.split(",")).map { k => (k(0), k(1)) }

    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://quickstart.cloudera:8020"), hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    } catch {
      case _: Throwable => {}
    }

    resultRDD.saveAsTextFile(args(1))

  }
}