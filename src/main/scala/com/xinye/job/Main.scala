package com.xinye.job

import java.io.InputStream
import java.util.Properties

import com.xinye.dispatcher.Dispatcher

/**
 * @author daiwei04@xinye.com
 * @since 2020/11/19 11:18
 */
object Main {

  private val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("job-ol.properties")
  private val prop = new Properties()


  def main(args: Array[String]): Unit = {

    prop.load(in)
    val dispatcher = Dispatcher(prop)
    dispatcher.run()

  }

}
