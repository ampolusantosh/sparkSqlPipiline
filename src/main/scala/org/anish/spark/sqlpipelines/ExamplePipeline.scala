package org.anish.spark.sqlpipelines

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * This is a sample SQL pipeline which attaches different transformations
  *
  * Created by anish on 19/11/17.
  */
object ExamplePipeline extends Pipeline {

  import transformations._
  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {
    // We can parse parameters here, and use them to set configurations.

    // Run the pipeline from here
    val pipelineOutput = pipeline(dummyDataSource, someOtherOldData)
    saveOutput(pipelineOutput)
  }

  // Building the data sources - Some small examples
  val dummyDataSource: DataFrame = {
    (1 to 1000).map(i => (i, i * 3, "A")).toDF("series", "series*3", "name")
      .withColumn("random", rand(1) * 100 cast IntegerType)
  }

  val someOtherOldData: DataFrame = {
    (10 to 20).map(i => (i, i * 3, "A")).toDF("series", "series*3", "name")
      .withColumn("random", rand(1) * 100 cast IntegerType)
  }

  // Write the data to disk / Output Strategy
  def saveOutput(dataFrame: DataFrame) {
    /*dataFrame
      .write
      .save("some path")*/

    dataFrame.show()
  }

  // Multiple transformations clubbed together in one pipeline
  // The actual nice, clean and readable pipelines.
  def pipeline(dummyDataSource: DataFrame, someOtherOldData: DataFrame): DataFrame = {
    dummyDataSource
      .select('series, 'random)
      .removeOutliers('series, 'random)
      .removeDuplicatesFrom(someOtherOldData, "series", "series")
    // Add other transformations here
  }
}
