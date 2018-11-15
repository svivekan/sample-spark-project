package com.github.mrpowers.spark.pika.chaining

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object Chaining {

  def withGreeting(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello"))
  }

  def withFarewell(df: DataFrame): DataFrame = {
    df.withColumn("farewell", lit("goodbye"))
  }

}
