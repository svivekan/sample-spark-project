package com.github.mrpowers.spark.pika.core

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("sample-spark-project")
      .getOrCreate()
  }


}
