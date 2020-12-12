package com.xinye.enums.impl

import com.xinye.enums.BaseEnum

/**
 * @author daiwei04@xinye.com
 * @since 2020/12/3 10:42
 */
object ComputeEnum extends BaseEnum {
  val addition: ComputeEnum.Value = Value("+")
  val subtraction: ComputeEnum.Value = Value("-")
  val multiplication: ComputeEnum.Value = Value("*")
  val division: ComputeEnum.Value = Value("/")
}
