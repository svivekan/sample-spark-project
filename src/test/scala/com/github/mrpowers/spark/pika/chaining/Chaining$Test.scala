package com.github.mrpowers.spark.pika.chaining

import com.github.mrpowers.spark.pika.core.BaseSparkTestSuite
import com.github.mrpowers.spark.pika.types.Greeting
import org.scalatest.{FunSuite, Matchers}


class Chaining$Test
  extends FunSuite
    with BaseSparkTestSuite
    with Matchers {

  test("withGreeting and withFarewell") {
    import spark.implicits._

    val df = Seq("man", "woman").toDF("subject")

    val actualDF = df
      .transform(Chaining.withGreeting)
      .transform(Chaining.withFarewell)

    val actualDS = actualDF.as[Greeting]
    val expectedDS = Seq(Greeting("man", "hello", "goodbye"), Greeting("woman", "hello", "goodbye")).toDS()

    actualDS.except(expectedDS).count() should be(0)
  }

}

