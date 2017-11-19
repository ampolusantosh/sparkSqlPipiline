package org.anish.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

/**
  *
  *
  * Created by anish on 19/11/17.
  */
trait SparkTestCase extends FlatSpec with BeforeAndAfterAll {

  // Lazy val is used to cache
  lazy val sparkSession = SparkSession
    .builder()
    .appName("SQL Pipeline Unit Tests")
    .master("local[8]")
    .config("spark.ui.enabled", false)
    .getOrCreate()

  override def afterAll(): Unit =
    sparkSession.stop()

}
