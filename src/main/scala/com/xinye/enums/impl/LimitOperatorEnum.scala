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
  val CHAINUP: LimitOperatorEnum.Value = Value
  val CHAINDOWN: LimitOperatorEnum.Value = Value
  val HOURCHAINUP: LimitOperatorEnum.Value = Value
  val HOURCHAINDOWN: LimitOperatorEnum.Value = Value
  val YESTERDAYCHAINUP: LimitOperatorEnum.Value = Value
  val YESTERDAYCHAINDOWN: LimitOperatorEnum.Value = Value

}
