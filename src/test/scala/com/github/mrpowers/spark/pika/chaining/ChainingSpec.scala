package com.github.mrpowers.spark.pika.chaining

import com.github.mrpowers.spark.pika.core.SparkSessionWrapper
import com.github.mrpowers.spark.pika.types.Greeting
import org.scalatest.{FunSuite, Matchers}


class ChainingSpec
  extends FunSuite
    with SparkSessionWrapper
    with Matchers {

  test("withGreeting and withFarewell") {
    import spark.implicits._

    val df = Seq("dog", "cat").toDF("subject")

    val actualDF = df
      .transform(Chaining.withGreeting)
      .transform(Chaining.withFarewell)
    val actualDS = actualDF.as[Greeting]

    val expectedDS = Seq(Greeting("dog", "hello", "goodbye"), Greeting("cat", "hello", "goodbye")).toDS()

    actualDS.except(expectedDS).count() should be(0)
  }

}

