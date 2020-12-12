
import com.xinye.enums.impl.LogicEnum

import java.util
import java.util.Map
import java.util.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 17:06
 * @desc
 */
object StrTest {

  def main(args: Array[String]): Unit = {

    /*
        val array = Array(Array[(String, String, String)](("group_sum", "{\"host\":\"10.114.6.25\"}", "7.0"), ("group_sum", "{\"host\":\"10.114.133.158\"}", "4.0")),
          Array[(String, String, String)](("all_sum", "{}", "19.0")))

        println(combination(array))
    */

    val map = new util.HashMap[String, String]

    println(map.get("ddd") != null)

    println(LogicEnum.fromString(null))


  }

  def combination(arrList: Array[Array[(String, String, String)]]): ArrayBuffer[Map[String, (String, String)]] = {
    if (arrList.length == 1) {
      val result = new ArrayBuffer[util.Map[String, (String, String)]]()
      val startMap = new util.HashMap[String, (String, String)]
      arrList(0).foreach(tuple => {
        startMap.put(tuple._1, (tuple._2, tuple._3))
      })
      result.append(startMap)
      result

    } else {
      combinerList(combination(arrList.drop(1)), arrList(0))
    }
  }

  def combinerList(arr1: ArrayBuffer[Map[String, (String, String)]], arr2: Array[(String, String, String)]): ArrayBuffer[Map[String, (String, String)]] = {
    val result = new ArrayBuffer[Map[String, (String, String)]]()
    arr2.foreach(tuple => {
      arr1.foreach(
        map => {
          val temp = new HashMap[String, (String, String)](map)
          temp.put(tuple._1, (tuple._2, tuple._3))
          result.append(temp)
        }
      )
      println(s"result $result")
    })
    result
  }
}
