package com.xinye.enums

import scala.util.control.Breaks.{break, breakable}

/**
 * @author daiwei04@xinye.com
 * @since 2020/11/24 17:14
 */
trait BaseEnum extends Enumeration {

  /**
   * 遍历当前枚举类型中的所有值 , 查看是否有能与 输入字符串相匹配的类型
   *
   * @param str 输入的 enum 字符串
   * @return 对应的 enum 类型
   */
  def fromString(str: String): Value = {
    var r: Value = null
    breakable(
      for (e <- this.values) {
        if (e.toString.equalsIgnoreCase(str)) {
          r = e
          break()
        }
      }
    )
    r
  }

}
