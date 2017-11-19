package org.anish.spark.sqlpipelines

import org.apache.spark.sql.SparkSession

/**
  * Properties common to all our pipelines are here.
  *
  * Created by anish on 19/11/17.
  */
abstract class Pipeline {

  protected lazy val sparkSession = SparkSession
    .builder()
    .appName("SQL Pipeline")
    .master("local[8]")
    .getOrCreate()

}
