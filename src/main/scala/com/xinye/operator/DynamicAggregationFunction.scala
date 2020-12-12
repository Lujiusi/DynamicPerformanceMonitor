package com.xinye.operator

import java.text.{DecimalFormat, SimpleDateFormat}
import com.xinye.base.Rule
import com.xinye.pojo.{AlarmMessage, DynamicKey}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.{HashMap, List, Map}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.xinye.enums.impl.{ComputeEnum, LimitOperatorEnum, LogicEnum}
import com.xinye.state.StateDescriptor
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * 将数据
 *
 * @author daiwei04@xinye.com
 * @since 2020/12/11 22:18
 */
class DynamicAggregationFunction extends KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage] {

  private final val logger: Logger = LoggerFactory.getLogger(classOf[DynamicAggregationFunction])

  lazy val detailState: MapState[String, Map[Long, ArrayBuffer[Map[String, String]]]] = getRuntimeContext.getMapState(StateDescriptor.detailState)

  lazy val aggState: MapState[String, Map[Long, Map[String, String]]] = getRuntimeContext.getMapState(StateDescriptor.aggState)

  private final val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private final val allDsToMaxWindow: Map[Int, Iterable[Rule.AggregatorFun]] = new HashMap[Int, Iterable[Rule.AggregatorFun]]()

  override def processElement(value: (DynamicKey, Map[String, String]),
                              ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage]#ReadOnlyContext,
                              collector: Collector[AlarmMessage]): Unit = {
    val aggState: ReadOnlyBroadcastState[Integer, Rule] = ctx.getBroadcastState(StateDescriptor.ruleState)
    val ruleId: Int = value._1.id
    if (aggState.contains(ruleId) && CommonFunction.ruleIsAvailable(aggState.get(ruleId))) {
      //将数据加入到对应 datasource 和 时间 内
      val timestamp: Long = value._2.get("timestamp").toLong / (1 * 60 * 1000) * 1 * 60 * 1000
      val datasource = value._2.get("datasource")
      if (detailState.get(datasource) == null) {
        detailState.put(datasource, new HashMap[Long, ArrayBuffer[Map[String, String]]])
      }
      if (detailState.get(datasource).get(timestamp) == null) {
        detailState.get(datasource).put(timestamp, new ArrayBuffer[Map[String, String]]())
      }
      //移除datasource 减小状态
      value._2.remove("datasource")
      detailState.get(datasource).get(timestamp).append(value._2)
      ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() / (60 * 1000) * 60 * 1000 + 60 * 1000)
    }
  }

  override def processBroadcastElement(rule: Rule,
                                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage]#Context,
                                       collector: Collector[AlarmMessage]): Unit = {
    if (StateDescriptor.changeBroadcastState(rule, ctx.getBroadcastState(StateDescriptor.ruleState))) {
      allDsToMaxWindow.put(rule.getRuleID, rule.getAggregatorFun.groupBy(_.getDatasource).mapValues(_.maxBy(_.getWindow)).values)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, AlarmMessage]#OnTimerContext,
                       out: Collector[AlarmMessage]): Unit = {

    val rule = ctx.getBroadcastState(StateDescriptor.ruleState).get(ctx.getCurrentKey.id)

    val aggFuncList = rule.getAggregatorFun

    // 如果规则可行
    if (CommonFunction.ruleIsAvailable(rule) && aggFuncList.nonEmpty) {

      val dsToMaxWindow = allDsToMaxWindow.get(rule.getRuleID)

      // 所有的用于统计指标的datasource都存在
      val aggAvailable = dsToMaxWindow.map(_.getDatasource)
        .forall(datasource => {
          detailState.contains(datasource)
        })

      val time = timestamp - 60 * 1000

      if (aggAvailable) {

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
          if (valueList.nonEmpty) aggResultMap.put(fun.getAliasName, CommonFunction.calculate(fun, valueList))
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

            // 从每个 aggResultMap 中 找到并计算每个 field 随对应的值
            val fieldsValueMap = fun.getFields
              .map(fields => {
                val result = new HashMap[String, String]
                fields.getFieldName.split(",")
                  .foreach(filedName => {
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
                  })
                result
              })

            // 准备好 HashMap 用于接受每个当前聚合函数的结果
            val currentPostAggResult = new HashMap[String, String]

            fieldsValueMap.get(1).foreach(
              entry => {
                fieldsValueMap.get(0)
                  //遍历第一个fieldMap 得到和当钱的第二个数据相关的数据
                  .filter(item => relationJSON(JSON.parseObject(entry._1), JSON.parseObject(item._1)))
                  .foreach(item => {
                    currentPostAggResult.put(item._1, postCalculate(item._2, entry._2, fun.getOperator))
                  })
              }
            )

            aggResultMap.put(fun.getAliasName, currentPostAggResult)

          })

          logger.info("aggResultMap : {}", aggResultMap)

          val alarmRule = rule.getAlarmRule

          // 如果有预警条件
          if (alarmRule != null && alarmRule.size() != 0) {

            // 获取alarm需要的所有的指标别名 和对应比较方法
            val selectors = getSelector(alarmRule)

            // 如果预警所需要的指标 aggResultMap 全都有 则继续 , 否则报错
            if (selectors.map(_._1).forall(aggResultMap.containsKey)) {

              // 清除 aggResult 中 selector 不需要的数据
              aggResultMap.keys.filter(key => {
                !selectors.map(_._1).contains(key)
              }).foreach(aggResultMap.remove)

              val appName = JSON.parseObject(ctx.getCurrentKey.key).getString("appName")

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
                // 将当前的 聚合数据 加入状态中
                aggState.get(item._1).put(time, aggResultMap.get(item._1))

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

              logger.info("alarmDataList : {}", alarmDataList.toList)

              alarmDataList.filter(alarmFilter(time, _, alarmRule))
                .foreach(map => {
                  val alarmRule = JSON.parseObject(rule.getAlarmRule.toJSONString)
                  reformAlarmRule(alarmRule, map, time)
                  out.collect(AlarmMessage(rule.getRuleID, appName, alarmRule))
                })

            }
          }
        } else {
          logger.warn(s"Rule NO.${rule.getRuleID} 在 ${format.format(time)} 分钟 不满足postAggregate条件 !", rule.getRuleID)
        }
      } else {
        logger.warn(s"Rule NO.${rule.getRuleID} 在 ${format.format(time)} 分钟 不满足aggregate条件 !", rule.getRuleID)
      }
    } else {
      logger.warn("Rule NO.{} is not start || aggregationFunction is Empty !", rule.getRuleID)
      detailState.clear()
    }
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
      second.contains(item._1) && second.get(item._1).equals(item._2)
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
      relationPostAggFun.get(0).getAliasName.split(",")(0)
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
      case LimitOperatorEnum.YOY_HOUR_DOWN | LimitOperatorEnum.YOY_HOUR_UP => 60 * 60 * 1000L
      case LimitOperatorEnum.YOY_DAY_DOWN | LimitOperatorEnum.YOY_DAY_UP => 24 * 60 * 60 * 1000L
      case LimitOperatorEnum.CHAIN_DOWN | LimitOperatorEnum.CHAIN_UP =>
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
   * 获取alarm中的所有 selector 字段 和其对应的 计算条件
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
      val result = new ArrayBuffer[Map[String, (String, String)]]()
      val startMap = new HashMap[String, (String, String)]
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

  /**
   * 是否报警判断
   *
   * @param time      当前时间
   * @param alarmData 被用于判断的数据
   * @param alarm     报警条件
   * @return
   */
  def alarmFilter(time: Long, alarmData: mutable.Map[String, (String, String, Long)], alarm: JSONObject): Boolean = {
    LogicEnum.fromString(alarm.getString("type")) match {
      case LogicEnum.AND =>
        // 遇到 判断连接条件为 and ,则 内部所有判断条件都为 true
        alarm.getJSONArray("fields").forall(bool => alarmFilter(time, alarmData, bool.asInstanceOf[JSONObject]))
      case LogicEnum.OR =>
        // 遇到 判断连接条件为or , 则 内部只要存在一个 true 就行
        alarm.getJSONArray("fields").exists(bool => alarmFilter(time, alarmData, bool.asInstanceOf[JSONObject]))
      case LogicEnum.SELECTOR =>
        // 告警字段 字段
        val alarmColumn = alarm.getString("alarmColumn")
        // 数值为第二个值
        val currentValue = alarmData(alarmColumn)._2.toDouble
        // 目标值为 target 对应的值
        val targetValue = alarm.getString("target").toDouble
        // 获取比较符
        val limitOperatorEnum = LimitOperatorEnum.fromString(alarm.getString("compareOperator"))
        limitOperatorEnum match {
          case LimitOperatorEnum.LESS => currentValue < targetValue
          case LimitOperatorEnum.LESS_EQUAL => currentValue <= targetValue
          case LimitOperatorEnum.GREATER => currentValue > targetValue
          case LimitOperatorEnum.GREATER_EQUAL => currentValue >= targetValue
          case _ =>
            val lastTimeAggResult = aggState.get(alarmColumn).get(time - alarmData(alarmColumn)._3)
            // 上一时间有数据 统计出了指标数据
            if (lastTimeAggResult != null) {
              val lastValueString = lastTimeAggResult.get(alarmData(alarmColumn)._1)
              // 上一时间的指标数据中含有当前 名称的指标
              if (lastValueString != null) {
                val lastValue = lastValueString.toDouble
                if (Array(LimitOperatorEnum.CHAIN_DOWN, LimitOperatorEnum.YOY_DAY_DOWN, LimitOperatorEnum.YOY_HOUR_DOWN).contains(limitOperatorEnum)) {
                  (lastValue - currentValue) / lastValue > targetValue
                } else if (Array(LimitOperatorEnum.CHAIN_UP, LimitOperatorEnum.YOY_HOUR_UP, LimitOperatorEnum.YOY_DAY_UP).contains(limitOperatorEnum)) {
                  (currentValue - lastValue) / lastValue > targetValue
                } else {
                  false
                }
              } else {
                false
              }
            } else {
              false
            }
        }
    }
  }

  /**
   * 根据 alarmRule 生成报警数据
   *
   * @param alarm     alarmRule
   * @param alarmData 当前满足报警条件的数据
   * @param time      当前时间
   */
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

  /**
   * 用于获取历史数据
   *
   * @param alarmColumn 别名
   * @param groupName   分组名
   * @param window      step 大小
   * @param time        当前时间
   * @return
   */
  def getHistoryData(alarmColumn: String, groupName: String, window: Long, time: Long): JSONObject = {
    val result = new JSONObject()
    if (aggState.contains(alarmColumn)) {
      (for (i <- 0 to 6) yield time - i * window).foreach(timestamp => {
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