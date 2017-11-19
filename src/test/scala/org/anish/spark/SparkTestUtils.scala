package org.anish.spark

import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.Matchers

/**
  * Created by anish on 24/01/17.
  */
object SparkTestUtils extends Matchers {

  /**
    * Gets absolute file path of a resource.
    *
    * @param pathInResource
    * @return actual path of file
    */
  def getResourcePath(pathInResource: String): String = {
    getClass.getResource(pathInResource).getPath
  }

  /**
    * Compares two dataframes and ensures that they have the same schema (ignore nullable) and the same values
    * This collects both data frames in the driver, thus not suitable for very large test data. Good for unit testing.
    *
    * @param actualDF   The DF we want to check for correctness
    * @param expectedDF The correct DF we use for comparison
    * @param onlySchema only compare the schemas of the dataframes
    */
  def dfEquals(actualDF: DataFrame, expectedDF: DataFrame, onlySchema: Boolean = false): Unit = {
    actualDF.schema.map(f => (f.name, f.dataType)).toSet shouldBe expectedDF.schema.map(f => (f.name, f.dataType)).toSet
    if (!onlySchema) {
      actualDF.collect.map(_.toSeq.toSet).toSet shouldBe expectedDF.collect.map(_.toSeq.toSet).toSet
    }
  }

  /**
    * Compares two datasets and ensures that they have the same schema (ignore nullable) and the same values
    * This collects both data sets in the driver, thus not suitable for very large test data. Good for unit testing.
    *
    * @param actualDs   The DS we want to check for correctness
    * @param expectedDs The correct DS we use for comparison
    * @param onlySchema only compare the schemas of the datasets
    */
  def dsEquals[T](actualDs: Dataset[T], expectedDs: Dataset[T], onlySchema: Boolean = false): Unit = {
    actualDs.schema.map(f => (f.name, f.dataType)).toSet shouldBe expectedDs.schema.map(f => (f.name, f.dataType)).toSet
    if (!onlySchema) {
      actualDs.collect.toSet shouldBe expectedDs.collect.toSet
    }
  }
}
