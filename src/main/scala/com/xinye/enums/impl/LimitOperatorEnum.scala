package com.xinye.enums.impl

import com.xinye.enums.BaseEnum

/**
 * @author daiwei04@xinye.com
 * @since 2020/12/3 10:42
 */
object LimitOperatorEnum extends BaseEnum {

  val EQUAL: LimitOperatorEnum.Value = Value("=")
  val NOT_EQUAL: LimitOperatorEnum.Value = Value("!=")
  val GREATER_EQUAL: LimitOperatorEnum.Value = Value(">=")
  val LESS_EQUAL: LimitOperatorEnum.Value = Value("<=")
  val GREATER: LimitOperatorEnum.Value = Value(">")
  val LESS: LimitOperatorEnum.Value = Value("<")
  val IN: LimitOperatorEnum.Value = Value
  val NOTIN: LimitOperatorEnum.Value = Value
  val REGEX: LimitOperatorEnum.Value = Value
  val CHAIN_UP: LimitOperatorEnum.Value = Value
  val CHAIN_DOWN: LimitOperatorEnum.Value = Value
  val YOY_HOUR_UP: LimitOperatorEnum.Value = Value
  val YOY_HOUR_DOWN: LimitOperatorEnum.Value = Value
  val YOY_DAY_UP: LimitOperatorEnum.Value = Value
  val YOY_DAY_DOWN: LimitOperatorEnum.Value = Value

}
