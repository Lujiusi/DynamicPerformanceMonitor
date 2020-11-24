package com.xinye.job

import java.io.InputStream
import java.util.Properties

import com.xinye.dispatcher.Dispatcher
import com.xinye.config.Parameters
import org.apache.flink.api.java.utils.ParameterTool

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 11:18
 * @desc
 */
object Main {

  private val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("job-test.properties")
  private val prop = new Properties()


  def main(args: Array[String]): Unit = {

    prop.load(in)
    val dispatcher = Dispatcher(prop)
    dispatcher.run()

  }

}
