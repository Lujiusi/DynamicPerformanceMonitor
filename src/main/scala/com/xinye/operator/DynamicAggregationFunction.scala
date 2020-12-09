package com.xinye.operator

import java.text.{DecimalFormat, SimpleDateFormat}
import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.pojo.{AlarmMessage, DynamicKey}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.List
import java.util.Map
import java.util.HashMap
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.xinye.enums.impl.{ComputeEnum, LimitOperatorEnum, LogicEnum, RuleSateEnum}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.mutable

class DynamicAggregationFunction extends KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DynamicAggregationFunction])

  lazy val detailState: MapState[String, Map[Long, ArrayBuffer[Map[String, String]]]] = getRuntimeContext.getMapState(StateDescriptor.detailState)

  lazy val aggState: MapState[String, Map[Long, Map[String, String]]] = getRuntimeContext.getMapState(StateDescriptor.aggState)

  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def processElement(value: (DynamicKey, Map[String, String]),
                              ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage]#ReadOnlyContext,
                              collector: Collector[AlarmMessage]): Unit = {
    val aggState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState)
    val ruleId: Int = value._1.id
    if (aggState.contains(ruleId)) {
      RuleSateEnum.fromString(aggState.get(ruleId).getRuleState) match {
        case RuleSateEnum.START =>
          //将数据加入到对应 datasource 和 时间 内
          val timeStamp: Long = value._2.get("timestamp").toLong / (1 * 60 * 1000) * 1 * 60 * 1000
          val datasource = value._2.get("datasource")
          if (detailState.get(datasource) == null) {
            detailState.put(datasource, new HashMap[Long, ArrayBuffer[Map[String, String]]])
          }
          if (detailState.get(datasource).get(timeStamp) == null) {
            detailState.get(datasource).put(timeStamp, new ArrayBuffer[Map[String, String]]())
          }
          //移除datasource 减小状态
          value._2.remove("datasource")
          detailState.get(datasource).get(timeStamp).append(value._2)
          ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() / (1 * 60 * 1000) * 1 * 60 * 1000 + 1 * 60 * 1000)
        case _ =>
      }
    }
  }

  override def processBroadcastElement(rule: Rule,
                                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage]#Context,
                                       collector: Collector[AlarmMessage]): Unit = {
    StateDescriptor.changeBroadcastState(rule, ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage]#OnTimerContext,
                       out: Collector[AlarmMessage]): Unit = {
    val time = timestamp - 60 * 1000

    val appName = JSON.parseObject(ctx.getCurrentKey.key).getString("appName")

    val rule = ctx.getBroadcastState(StateDescriptor.dynamicAggregateRuleMapState).get(ctx.getCurrentKey.id)

    val aggFuncList = rule.getAggregatorFun

    //datasource 最大保存的时间 (获取最大窗口)
    val dsToMaxWindow = aggFuncList.groupBy(_.getDatasource).mapValues(_.maxBy(_.getWindow)).values

    // 所有的用于统计指标的datasource都存在
    val aggAvailable = dsToMaxWindow.map(_.getDatasource)
      .forall(datasource => {
        detailState.contains(datasource)
      })

    if (ruleIsAvailable(rule) && aggFuncList.nonEmpty && aggAvailable) {

      // 清除已经不在datasource中的 key
      detailState.keys().filter(datasource => {
        !dsToMaxWindow.map(_.getDatasource).contains(datasource)
      }).foreach(detailState.remove)

      // 清楚 datasource 中已经不在最大窗口大小中的数据
      dsToMaxWindow.foreach(fun => {
        detailState.get(fun.getDatasource)
          .map(_._1)
          .filter(_ < time - fun.getWindow * 60 * 1000)
          .foreach(detailState.get(fun.getDatasource).remove)
      })

      // 保存 agg 和 postAgg 计算出的所有结果,
      val aggResultMap = new HashMap[String, Map[String, String]]()

      // 添加 agg 计算结果
      aggFuncList.foreach(fun => {
        val window = fun.getWindow
        val valueList = detailState.get(fun.getDatasource)
          .filter(entry => entry._1 > time - window * 60 * 1000 && entry._1 <= time)
          .flatMap(_._2)
          .toList
        if (valueList.nonEmpty) aggResultMap.put(fun.getAliasName, AggregationFunction.calculate(fun, valueList))
      })

      val postAggFunList = rule.getPostAggregatorFun

      val postAvailable = postAggFunList.flatMap(_.getFields)
        .map(_.getFieldName)
        .flatMap(_.split(","))
        .forall(fieldName => {
          aggResultMap.containsKey(fieldName)
        })

      // 如果 postAggFun 中所需要的数据 aggResultMap 全都有
      // 才能进行开始计算 postAgg 结果 并添加到 aggResultMap 中

      if (postAvailable) {

        postAggFunList.foreach(fun => {

          if (!aggResultMap.containsKey(fun.getAliasName)) {
            aggResultMap.put(fun.getAliasName, new HashMap[String, String])
          }

          val fieldsValueMap = fun.getFields.map(
            fields => {
              val result = new HashMap[String, String]
              fields.getFieldName.split(",").foreach(
                filedName => {
                  aggResultMap.get(filedName)
                    .entrySet()
                    .foreach(entry => {
                      if (!result.containsKey(entry.getKey)) {
                        result.put(entry.getKey, entry.getValue)
                      } else {
                        val value = result.get(entry.getKey)
                        result.put(entry.getKey, (value.toDouble + entry.getValue.toDouble).toString)
                      }
                    })
                }
              )
              result
            }
          )

          fieldsValueMap.get(1).foreach(
            entry => {
              fieldsValueMap.get(0)
                //遍历第一个fieldMap 得到和当钱的第二个数据相关的数据
                .filter(item => relationJSON(JSON.parseObject(entry._1), JSON.parseObject(item._1)))
                .foreach(item => {
                  aggResultMap.get(fun.getAliasName).put(item._1, postCalculate(item._2, entry._2, fun.getOperator))
                })
            }
          )

        })

        val alarmRule = rule.getAlarmRule

        // 获取alarm需要的所有的指标别名
        val selectors = getSelector(alarmRule)

        // 将aggState中不含的key进行清除
        aggState.keys().filter(key => {
          !selectors.map(_._1).contains(key)
        }).foreach(aggState.remove)

        // 确定别名所要对比的时间,或者要保存的 '单个周期时长' (预警需要保存七个周期时长)
        val selectorToMap = selectors.map(selector => {
          (selector._1, getRelationWindow(selector._1, selector._2, rule))
        }).toMap

        selectorToMap.foreach(item => {
          if (!aggState.contains(item._1)) {
            aggState.put(item._1, new HashMap[Long, Map[String, String]])
          }
          //如果当聚合的 中间结果中 含有当前 aliasName 则 将当前的数据加入状态中
          if (aggResultMap.containsKey(item._1)) {
            aggState.get(item._1).put(time, aggResultMap.get(item._1))
          }

          // 去除七个周期以外的数据
          aggState.get(item._1).map(_._1)
            .filter(_ < time - 6 * selectorToMap(item._1))
            .foreach(aggState.get(item._1).remove)
        })

        // 获取所有 selector 所对应的分组值
        val groupingNameList = selectors.map(selector =>
          getRelationGroupingName(selector._1, rule)
        )

        // 获取 selector 都有的 分组值
        val commonGroupingName = groupingNameList.flatMap(_.split(","))
          .groupBy(word => word)
          .mapValues(_.size)
          .filter(_._2 == groupingNameList.size)
          .keys.toList

        // 清楚 aggResult 中 selector 不需要的数据
        aggResultMap.keys.filter(key => {
          !selectors.map(_._1).contains(key)
        }).foreach(aggResultMap.remove)

        val alarmDataList = aggResultMap.flatMap(entry => {
          entry._2.map(item => {
            (entry._1, item._1, item._2)
          })
        })
          .groupBy(tuple => {
            getCommonJSON(commonGroupingName, JSON.parseObject(tuple._2)).toJSONString
          })
          .mapValues(iter => {
            val list = iter.groupBy(_._1).values.toArray.map(_.toArray)
            combination(list)
          })
          .flatMap(_._2)
          .map(map => {
            map.map(entry => {
              (entry._1, (entry._2._1, entry._2._2, selectorToMap(entry._1)))
            })
          })

        logger.info(s"alarmDataList ${alarmDataList.toList}")

        alarmDataList.filter(
          alarmFilter(time, _, alarmRule)
        )
          .foreach(map => {
            val alarmRule = JSON.parseObject(rule.getAlarmRule.toJSONString)
            reformAlarmRule(alarmRule, map, time)
            out.collect(AlarmMessage(rule.getRuleID, appName, alarmRule))
          })
      }

    } else {
      detailState.clear()
    }

  }

  /**
   * 判断规则的状态是否为 start (激活态)
   *
   * @param rule 当前规则
   * @return
   */
  def ruleIsAvailable(rule: Rule): Boolean = {
    var tag: Boolean = false
    if (rule != null) {
      tag = RuleSateEnum.fromString(rule.getRuleState) match {
        case RuleSateEnum.START => true
        case _ => false
      }
    }
    tag
  }

  /**
   * 用于查找为维度值有关联的数据
   *
   * @param first  第一个维度json
   * @param second 第二个维度json
   * @return 如果第二个json中包含第一个json中的key且对应值相同
   */
  def relationJSON(first: JSONObject, second: JSONObject): Boolean = {
    first.forall(item => {
      second.contains(item._1) || !second.get(item._1).equals(item._2)
    })
  }

  /**
   * 计算加减乘除的结果
   *
   * @param firstValue  多项式的第一个值
   * @param secondValue 多项式的第二个值
   * @param operator    计算规则
   * @return 计算结果
   */
  def postCalculate(firstValue: String, secondValue: String, operator: String): String = {
    val df = new DecimalFormat("#.0000")
    val result = ComputeEnum.fromString(operator) match {
      case ComputeEnum.addition => firstValue.toDouble + secondValue.toDouble
      case ComputeEnum.subtraction => firstValue.toDouble - secondValue.toDouble
      case ComputeEnum.division => df.format(firstValue.toDouble / secondValue.toDouble).toDouble
      case ComputeEnum.multiplication => firstValue.toDouble + secondValue.toDouble
    }
    result.toString
  }

  def getRelationAliasName(aliasName: String, rule: Rule): String = {
    val relationPostAggFun = rule.getPostAggregatorFun.filter(_.getAliasName.equals(aliasName))
    if (relationPostAggFun.nonEmpty) {
      relationPostAggFun.get(0).getAliasName
    } else {
      aliasName
    }
  }

  /**
   * 对于alarm中的一个别名,获取其对应的窗口的大小 ,用于确定需要保存的状态的时间范围
   *
   * @param aliasName 别名
   * @param rule      规则
   * @return window 的大小
   */
  def getRelationWindow(aliasName: String, operator: LimitOperatorEnum.Value, rule: Rule): Long = {
    operator match {
      case LimitOperatorEnum.YOY_HOUR_DOWN | LimitOperatorEnum.YOY_HOUR_GROWTH => 60 * 60 * 1000L
      case LimitOperatorEnum.YOY_DAY_DOWN | LimitOperatorEnum.YOY_DAY_GROWTH => 24 * 60 * 60 * 1000L
      case LimitOperatorEnum.CHAIN_DOWN | LimitOperatorEnum.CHAIN_GROWTH =>
        rule.getAggregatorFun.filter(_.getAliasName.equals(getRelationAliasName(aliasName, rule))).get(0).getWindow * 60 * 1000L
      case _ => 60 * 1000L
    }
  }

  /**
   * 获取当前别名所对应的分组字段
   *
   * @param aliasName alarm 对应分组字段
   * @param rule      规则
   * @return
   */
  def getRelationGroupingName(aliasName: String, rule: Rule): String = {
    rule.getAggregatorFun.filter(_.getAliasName.equals(getRelationAliasName(aliasName, rule))).get(0).getGroupingNames.mkString(",")
  }

  /**
   *
   * @param alarm alarmRule
   * @return
   */
  def getSelector(alarm: JSONObject): ArrayBuffer[(String, LimitOperatorEnum.Value)] = {
    val result = new ArrayBuffer[(String, LimitOperatorEnum.Value)]()
    LogicEnum.fromString(alarm.getString("type")) match {
      // 对selector类型的对象, 直接获取 (alarmColumn,compareOperator)
      case LogicEnum.SELECTOR =>
        result :+ (alarm.getString("alarmColumn"), LimitOperatorEnum.fromString(alarm.getString("compareOperator")))
      // 对于非selector类型的对象, 进入内部迭代遍历
      case _ =>
        result ++ alarm.getJSONArray("fields").flatMap(obj => getSelector(obj.asInstanceOf[JSONObject]))
    }
  }

  def getCommonJSON(commonGroupingName: List[String], json: JSONObject): JSONObject = {
    val result = new JSONObject()
    commonGroupingName.foreach(groupName => result.put(groupName, json.getString(groupName)))
    result
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
    })
    result
  }

  def alarmFilter(time: Long, alarmData: mutable.Map[String, (String, String, Long)], alarm: JSONObject): Boolean = {
    LogicEnum.fromString(alarm.getString("type")) match {
      // 遇到 判断连接条件为 and ,则 内部所有判断条件都为 true
      case LogicEnum.AND =>
        alarm.getJSONArray("fields").forall(bool => alarmFilter(time, alarmData, bool.asInstanceOf[JSONObject]))
      // 遇到 判断连接条件为or , 则 内部只要存在一个 true 就行
      case LogicEnum.OR =>
        alarm.getJSONArray("fields").exists(bool => alarmFilter(time, alarmData, bool.asInstanceOf[JSONObject]))

      case LogicEnum.SELECTOR =>
        // 告警字段 字段
        val alarmColumn = alarm.getString("alarmColumn")
        // 数值为第二个值
        val currentValue = alarmData(alarmColumn)._2.toDouble
        // 目标值为 target 对应的值
        val targetValue = alarm.getString("target").toDouble

        LimitOperatorEnum.fromString(alarm.getString("compareOperator")) match {
          case LimitOperatorEnum.LESS => currentValue < targetValue
          case LimitOperatorEnum.LESS_EQUAL => currentValue <= targetValue
          case LimitOperatorEnum.GREATER => currentValue > targetValue
          case LimitOperatorEnum.GREATER_EQUAL => currentValue >= targetValue
          case LimitOperatorEnum.CHAIN_DOWN | LimitOperatorEnum.YOY_DAY_DOWN | LimitOperatorEnum.YOY_HOUR_DOWN =>
            val window = alarmData(alarmColumn)._3
            val groupName = alarmData(alarmColumn)._1
            if (aggState.get(alarmColumn).containsKey(time - window) && aggState.get(alarmColumn).get(time - window).containsKey(groupName)) {
              val lastValue = aggState.get(alarmColumn).get(time - window).get(groupName).toDouble
              (lastValue - currentValue) / lastValue > targetValue
            } else {
              false
            }
          case LimitOperatorEnum.CHAIN_GROWTH | LimitOperatorEnum.YOY_HOUR_GROWTH | LimitOperatorEnum.YOY_DAY_GROWTH =>
            val window = alarmData(alarmColumn)._3
            val groupName = alarmData(alarmColumn)._1
            if (aggState.get(alarmColumn).containsKey(time - window) && aggState.get(alarmColumn).get(time - window).containsKey(groupName)) {
              val lastValue = aggState.get(alarmColumn).get(time - window).get(groupName).toDouble
              (currentValue - lastValue) / lastValue > targetValue
            } else {
              false
            }
        }

    }
  }

  def reformAlarmRule(alarm: JSONObject, alarmData: mutable.Map[String, (String, String, Long)], time: Long): Unit = {
    LogicEnum.fromString(alarm.getString("type")) match {
      case LogicEnum.AND | LogicEnum.OR =>
        alarm.getJSONArray("fields")
          .foreach(alarmChild => reformAlarmRule(alarmChild.asInstanceOf[JSONObject], alarmData, time))
      case LogicEnum.SELECTOR =>
        val alarmColumn = alarm.getString("alarmColumn")
        val alarmSelector = alarmData(alarmColumn)
        val window = alarmSelector._3
        alarm.put("value", alarmSelector._2)
        alarm.put("groupName", JSON.parseObject(alarmSelector._1))
        alarm.put("history", getHistoryData(alarmColumn, alarmSelector._1, window, time))
    }
  }

  def getHistoryData(alarmColumn: String, groupName: String, window: Long, time: Long): JSONObject = {
    val result = new JSONObject()
    val longs = (for (i <- 0 to 6) yield time - i * window)
    if (aggState.contains(alarmColumn)) {
      longs.foreach(timestamp => {
        if (aggState.get(alarmColumn).containsKey(timestamp) &&
          aggState.get(alarmColumn)
            .get(timestamp)
            .containsKey(groupName)) {
          val value = aggState.get(alarmColumn).get(timestamp).get(groupName)
          if (value != null) {
            result.put(format.format(timestamp), value.toDouble)
          }
        }
      })
    }
    result
  }

}