package com.xinye.enums

import com.xinye.enums.imp.BaseEnum

object AggregatorFunctionType extends BaseEnum {
  val SUM,
  AVG,
  MIN,
  MAX,
  COUNT,
  COUNTDISTINCT,
  COMPUTEBYSTEP
  = Value
}
