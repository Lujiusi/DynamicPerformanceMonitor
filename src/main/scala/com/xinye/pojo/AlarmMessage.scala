package com.xinye.pojo

import com.alibaba.fastjson.JSONObject

/**
 * @author daiwei04@xinye.com
 * @date 2020/12/8 22:12
 * @desc
 */
case class AlarmMessage(ruleId: Int, appName: String, alarmRule: JSONObject)
