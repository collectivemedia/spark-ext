package org.apache.spark.ml.feature

import org.apache.spark.ml.param.{Param, Params}

/**
 * Trait for shared param keyCol.
 */
private[ml] trait HasKeyCol extends Params {

  /**
   * Param for category column name.
   * @group param
   */
  final val keyCol: Param[String] = new Param[String](this, "keyCol",
    "Column that holds value for category name")

  /** @group getParam */
  def getCategoryCol: String = $(keyCol)
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
