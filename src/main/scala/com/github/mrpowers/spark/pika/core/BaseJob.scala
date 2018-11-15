package com.github.mrpowers.spark.pika.core

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try


trait BaseJob {


  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  def sparkSession: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName(getClass.getName)
      .getOrCreate()
  }

  /**
    * Override this to include any initialization tasks that should be run before run method.
    *
    * @param spark SparkSession
    * @param args  Map of arguments
    */
  def init(spark: SparkSession, args: Map[String, String]) {
    logger.info("Initializing...")
  }

  /**
    * Override this to include any clean up tasks
    *
    * @param spark SparkSession
    * @param args  Map of arguments
    */
  def cleanUp(spark: SparkSession, args: Map[String, String]) {
    logger.info("Cleaning up...")
  }


  /**
    * This is where the main logic of the spark job should be implemented.
    *
    * @param spark SparkSession
    * @param args  arguments for the job
    */
  def run(spark: SparkSession, args: Map[String, String])


  /**
    * Entry point for this spark job.<br/>
    * This cannot be overridden.
    *
    * @param cmdArgs arguments
    */
  final def main(cmdArgs: Array[String]) {
    // handle arguments in terms of name-value pairs
    val args = getNamedArgs(cmdArgs)
    val spark = sparkSession

    try {
      // initialize
      init(spark, args)

      // invoke the actual logic
      run(spark, args)

    } catch {
      case ex: Throwable =>
        throw ex
    } finally {

      // clean up
      Try(cleanUp(spark, args))
      Try(spark.stop())
    }
    logger.info(getClass.getName + " is complete.")
  }


  private def getNamedArgs(args: Array[String]): Map[String, String] = {
    args.filter(line => line.contains("="))
      .map(x => (x.substring(0, x.indexOf("=")), x.substring(x.indexOf("=") + 1))).toMap
  }

}

