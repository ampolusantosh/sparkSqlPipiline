package org.anish.spark.sqlpipelines

import org.anish.spark.SparkTestCase
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by anish on 19/11/17.
  */
class TransformationTestSpec extends FlatSpec with SparkTestCase with Matchers {

  // Example of testing one transformation
  it should "remove outliers" in {
    import sparkSession.implicits._
    import transformations._

    (1 to 100).toDF("someValue")
      .union((500 to 525).toDF("someValue"))
      .removeOutliers('someValue)
      .count shouldBe 126
  }

}
