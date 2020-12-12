package com.xinye.pojo

import com.alibaba.fastjson.JSONObject

/**
 * @author daiwei04@xinye.com
 * @since 2020/12/8 22:12
 */
case class AlarmMessage(ruleId: Int, appName: String, alarmRule: JSONObject)
