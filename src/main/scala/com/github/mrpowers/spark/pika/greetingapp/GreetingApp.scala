package com.github.mrpowers.spark.pika.greetingapp

import com.github.mrpowers.spark.pika.chaining.Chaining
import com.github.mrpowers.spark.pika.core.BaseJob
import com.github.mrpowers.spark.pika.types.Greeting
import org.apache.spark.sql.SparkSession

object GreetingApp extends BaseJob {

  val OUTPUT_PATH = "output.path"

  override def run(spark: SparkSession, args: Map[String, String]): Unit = {
    import spark.implicits._

    val df = Seq("man", "woman").toDF("subject")

    val actualDF = df
      .transform(Chaining.withGreeting)
      .transform(Chaining.withFarewell)

    val actualDS = actualDF.as[Greeting]
    actualDS.write.format("csv").save(args(OUTPUT_PATH))

  }

}
