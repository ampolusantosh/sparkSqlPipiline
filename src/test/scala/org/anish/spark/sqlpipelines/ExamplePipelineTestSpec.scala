package org.anish.spark.sqlpipelines

import org.anish.spark.SparkTestCase
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by anish on 19/11/17.
  */
class ExamplePipelineTestSpec extends FlatSpec with SparkTestCase with Matchers {

  // Example of testing all transformations in the pipeline
  it should "run a series of transformations to the input" in {
    // This should use the dummy data stored in test resources or created here
    ExamplePipeline.pipeline(ExamplePipeline.dummyDataSource, ExamplePipeline.someOtherOldData)
      .count() shouldBe 989
  }
}