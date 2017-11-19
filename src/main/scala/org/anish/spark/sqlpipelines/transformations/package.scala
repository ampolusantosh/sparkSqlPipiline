package org.anish.spark.sqlpipelines

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * We keep our transformations. We may wish to apply these to multiple pipelines
  *
  * Created by anish on 19/11/17.
  */
package object transformations {

  implicit class dfTransformations(dataFrame: DataFrame) {

    import dataFrame.sparkSession.implicits._

    /**
      * Remove records which are already present in the other data frame
      *
      * @param other
      * @param thisKey  join key to identify the records in the current data frame
      * @param otherKey join key to identify the records in the other data frame
      * @return
      */
    def removeDuplicatesFrom(other: DataFrame, thisKey: String, otherKey: String): DataFrame = {
      dataFrame.join(other, dataFrame(thisKey) === other(otherKey), "leftouter")
        .filter(other(otherKey) isNull)
        .select(dataFrame.columns.map(dataFrame.apply): _*) // Select cols from only the first df
    }

    /**
      * Remove extra spaces from column names. Usually CSV files have spaces after col headers
      *
      * @return
      */
    def trimColumnNames(): DataFrame = {
      dataFrame.columns.foldLeft(dataFrame) {
        (df, c) => df.withColumnRenamed(c, c.trim)
      }
    }

    /**
      * Remove outliers for a particular column(s)
      *
      * @param column
      * @return
      */
    def removeOutliers(column: String): DataFrame = {
      removeOutliers(col(column))
    }

    def removeOutliers(c: Column): DataFrame = {
      val std_dev = dataFrame.agg(stddev(c)).first().get(0) // should have only 1 row and column
      val meanValue = dataFrame.agg(mean(c)).first().get(0)
      dataFrame.filter(abs(c - meanValue) < lit(std_dev) * 3)
    }

    def removeOutliers(columns: Column*): DataFrame = {
      columns.foldLeft(dataFrame) {
        (df, c) => df.removeOutliers(c)
      }
    }

  }

}
