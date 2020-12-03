package com.xinye.enums.impl

import com.xinye.enums.BaseEnum

/**
 * @author daiwei04@xinye.com
 * @date 2020/12/3 10:42
 * @desc
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
}
