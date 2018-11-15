package com.github.mrpowers.spark.pika.core

import org.apache.spark.sql.SparkSession

trait BaseSparkTestSuite {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName(getClass.getName)
      .getOrCreate()
  }


}
