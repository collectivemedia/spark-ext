package org.apache.spark.ml.feature

import org.apache.spark.ml.param.{Param, Params}

/**
 * Trait for shared param categoryCol.
 */
private[ml] trait HasCategoryCol extends Params {

  /**
   * Param for category column name.
   * @group param
   */
  final val categoryCol: Param[String] = new Param[String](this, "categoryCol",
    "Column that holds value for category name")

  /** @group getParam */
  def getCategoryCol: String = $(categoryCol)
}

/**
 * Trait for shared param valueCol.
 */
private[ml] trait HasValueCol extends Params {

  /**
   * Param for value column name.
   * @group param
   */
  val valueCol: Param[String] = new Param[String](this, "valueCol",
    "Column that holds a value for category")


  /** @group getParam */
  def getValueCol: String = $(valueCol)

}
