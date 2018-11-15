package com.github.mrpowers.spark.pika.core

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    LogManager.getRootLogger.setLevel(Level.ERROR)
    SparkSession
      .builder()
      .master("local")
      .appName("spark pika")
      .getOrCreate()
  }


}
