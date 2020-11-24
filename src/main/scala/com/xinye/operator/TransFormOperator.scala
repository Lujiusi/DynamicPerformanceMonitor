package com.xinye.operator

import java.util.regex.Pattern

object TransFormOperator {

  def regex(target: String, pattern: String): Option[String] = {
    val r = pattern.r
    r.findFirstIn(target)
  }

  def regexp_replace(target: String, pattern: String): Unit = {
    val r = Pattern.compile(pattern)
    r.split(target)

  }

  def regexp_extract(target: String, pattern: String): Unit = {

  }

}
