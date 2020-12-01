import java.util

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 17:06
 * @desc
 */
object StrTest {

  def main(args: Array[String]): Unit = {

    var message =
      """agent.heartbeat.[tenant.ppdapi.com]-fat,cluster=default,env=fat,healthy=true,host=10.114.15.120 detail="{MemoryStatus=Result{isHealthy=true}, SystemLoad=Result{isHealthy=true}, ThreadDeadlock=Result{isHealthy=true}}",startTime=1606387656265,uptime=338304105,version="4.0" 1606725900000000000
        |""".stripMargin


    val result = new util.HashMap[String, String]()
    //    1605750180000
    result.put("timestamp", message.substring(message.lastIndexOf(' ') + 1).substring(0, 13))
    message = message.substring(0, message.lastIndexOf(' '))
    result.put("appName", message.substring(message.indexOf('[') + 1, message.indexOf(']')))
    result.put("datasource", message.substring(0, message.indexOf('[') - 1))
    result.put("env", message.substring(message.indexOf(']') + 2, message.indexOf(',')))
    val mesArr: Array[String] = message.split("]-.{1,5},")

    println(mesArr(1).substring(0, mesArr(1).lastIndexOf(' ')))
    println(mesArr(1).substring(mesArr(1).lastIndexOf(' ') + 1))

    addValue(mesArr(1).substring(0, mesArr(1).lastIndexOf(' ')), result)
    addValue(mesArr(1).substring(mesArr(1).lastIndexOf(' ') + 1), result)

    print(result)

  }

  def addValue(str: String, result: util.Map[String, String]): Unit = {
    val buffer = new StringBuffer().append(str.charAt(0))
    for (c <- 1 until str.length - 1) {
      val c1 = str.charAt(c)
      val c2 = str.charAt(c - 1)
      if (','.equals(c1) && !'\\'.equals(c2) && !'}'.equals(c2) || c == str.length - 1) {
        val equalIndex = buffer.indexOf("=")
        result.put(buffer.substring(0, equalIndex), buffer.substring(equalIndex + 1))
        buffer.delete(0, buffer.length())
      } else {
        buffer.append(c1)
      }
    }
  }

}
