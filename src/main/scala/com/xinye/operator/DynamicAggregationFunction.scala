package com.xinye.operator

import java.text.{DecimalFormat, SimpleDateFormat}
import com.xinye.base.Rule
import com.xinye.pojo.DynamicKey
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.{HashMap, HashSet, List, Map}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.xinye.enums.impl.{ComputeEnum, LimitOperatorEnum, LogicEnum}
import com.xinye.state.StateDescriptor
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * 将数据
 *
 * @author daiwei04@xinye.com
 * @since 2020/12/11 22:18
 */
class DynamicAggregationFunction extends KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, JSONObject] {

  private final val logger: Logger = LoggerFactory.getLogger(classOf[DynamicAggregationFunction])

  lazy val detailState: MapState[String, Map[Long, ArrayBuffer[Map[String, String]]]] = getRuntimeContext.getMapState(StateDescriptor.detailState)

  lazy val aggState: MapState[String, Map[Long, Map[String, String]]] = getRuntimeContext.getMapState(StateDescriptor.aggState)

  //  lazy val hostNote = getRuntimeContext.getMapState(StateDescriptor.hostNote)
  //
  //  lazy val timeHostNote = getRuntimeContext.getMapState(StateDescriptor.timeHostNote)

  private final val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //  override def open(parameters: Configuration): Unit = {
  //    logger.info("开辟分区啦")
  //  }

  override def processElement(value: (DynamicKey, Map[String, String]),
                              ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, JSONObject]#ReadOnlyContext,
                              collector: Collector[JSONObject]): Unit = {
    val ruleId = value._1.id
    val ruleState: ReadOnlyBroadcastState[String, Rule] = ctx.getBroadcastState(StateDescriptor.ruleState)
    //    val ruleId: Int = value._1.id
    if (ruleState.contains(ruleId)
      && CommonFunction.ruleIsAvailable(ruleState.get(ruleId))
      && ruleState.get(ruleId).getAggregatorFun.map(_.getDatasource).contains(value._2.get("datasource"))) {
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

      //      if (timeHostNote.get(timestamp) == null) {
      //        timeHostNote.put(timestamp, new HashSet[String]())
      //      }
      //
      //      timeHostNote.get(timestamp).add(value._2.get("host"))

      detailState.get(datasource).get(timestamp).append(value._2)

      ctx.timerService().registerEventTimeTimer(timestamp / (60 * 1000) * 60 * 1000 + 60 * 1000)
    }
  }

  override def processBroadcastElement(rule: Rule,
                                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, JSONObject]#Context,
                                       collector: Collector[JSONObject]): Unit = {
    StateDescriptor.changeBroadcastState(rule, ctx.getBroadcastState(StateDescriptor.ruleState))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[DynamicKey, (DynamicKey, Map[String, String]), Rule, JSONObject]#OnTimerContext,
                       out: Collector[JSONObject]): Unit = {

    //    println(s"key ${ctx.getCurrentKey.key} ")
    //
    //    println(detailState)

    if (timestamp > 0) {

      val time = timestamp - 60 * 1000

      logger.info("定时器触发 {} , key : {} , ruleId :{}", format.format(time), ctx.getCurrentKey.key, ctx.getCurrentKey.id)

      val rule = ctx.getBroadcastState(StateDescriptor.ruleState).get(ctx.getCurrentKey.id)

      try {

        val mapKey = JSON.parseObject(ctx.getCurrentKey.key)

        val appName = mapKey.getString("appName")

        val env = mapKey.getString("env")

        val aggFuncList = rule.getAggregatorFun

        val alarmRule = rule.getAlarmRule

        // 如果规则可行
        if (CommonFunction.ruleIsAvailable(rule) && aggFuncList != null && alarmRule != null) {


          // 筛选出历史 host 中不在当前批次中出现的数据

          // 初始化 预警数据
          val result = new JSONObject()
          result.put("ruleId", rule.getRuleID)
          result.put("appName", appName)
          result.put("tags", rule.getTags)
          result.put("sink", rule.getSink)
          result.put("env", env)
          result.put("category", rule.getCategory)
          result.put("alertName", rule.getAlertName)

          //        if (hostNote.keys().nonEmpty) {
          //          if (timeHostNote.contains(time)) {
          //            hostNote.iterator()
          //              .filter(entry => entry.getValue > 0 && !timeHostNote.get(time).contains(entry.getKey))
          //              .map(_.getKey)
          //              .foreach(host => {
          //                //将 host 的对应的值减一
          //                hostNote.put(host, hostNote.get(host) - 1)
          //                val hostNotAppear = new JSONObject(result)
          //                hostNotAppear.put("host", host)
          //                hostNotAppear.put("type", "hostNoteAppear")
          //                out.collect(hostNotAppear)
          //              })
          //          } else if (!detailState.iterator().exists(entry => entry.getValue.containsKey(time))) {
          //            val appNameNotAppear = new JSONObject(result)
          //            appNameNotAppear.put("type", "appNameNotAppear")
          //            out.collect(appNameNotAppear)
          //          }
          //        }
          //
          //        result.remove("type")
          //        result.remove("host")
          //
          //        // 将当前时间的 host 放入历史 host 当中
          //        if (timeHostNote.contains(time)) {
          //          timeHostNote.get(time)
          //            .foreach(hostNote.put(_, 3))
          //          println("hostceshi " + hostNote.iterator().map(entry => (entry.getKey, entry.getValue)).toList + " : " + timeHostNote.get(time).toList)
          //          //将目前的的所存储的 host 清除掉
          //          timeHostNote.remove(time)
          //        }

          val dsToMaxWindow = rule.getAggregatorFun.groupBy(_.getDatasource).mapValues(_.maxBy(_.getWindow)).values

          //          logger.info("ds {}", dsToMaxWindow)

          cleanDetailState(time, dsToMaxWindow)

          //          logger.info("时间戳{}", detailState.get("system.cpu").keys.toList.map(format.format))


          // 保存 agg 和 postAgg 计算出的所有结果,
          val aggResultMap = new HashMap[String, Map[String, String]]()

          // 添加 agg 计算结果
          aggFuncList.foreach(fun => {
            // 如果 含有 对应的 datasource 的明细数据 则进行统计对应的聚合信息
            if (detailState.contains(fun.getDatasource)) {
              val valueList = detailState.get(fun.getDatasource)
                .filter(entry => entry._1 > (time - fun.getWindow * 60 * 1000) && entry._1 <= time)
                .flatMap(_._2)
                .toList
                .filter(_.containsKey(fun.getComputeColumn))

              //              println(format.format(time - fun.getWindow * 60 * 1000) + "::" + format.format(time))
              //
              //              println("shuju1", detailState.get(fun.getDatasource)
              //                .filter(entry => entry._1 > (time - fun.getWindow * 60 * 1000) && entry._1 <= time).map(entry => (format.format(entry._1), entry._2)))

              //              logger.info("valueList : {}", valueList)

              val aliasResult = CommonFunction.calculate(fun, valueList)

              //              logger.info(format.format(time) + " MapKey :" + mapKey + " ValueList : " + valueList)

              //              logger.info("aliasResult : {} ", aliasResult)

              if (aliasResult.nonEmpty) {
                aggResultMap.put(fun.getAliasName, CommonFunction.calculate(fun, valueList))
              }
            }
          })

          //          logger.info("aggResultMap : {}", aggResultMap)

          val postAggFunList = rule.getPostAggregatorFun

          if (postAggFunList != null) {

            postAggFunList.foreach(fun => {

              // 如果方法所需要的 前聚合数据 aggResultMap 全都有, 则进行当前 postAgg 的计算,并加入 aggResultMap\
              if (fun.getFields.flatMap(_.getFeildName.split(",")).forall(aggResultMap.containsKey)) {

                // 从每个 aggResultMap 中 找到并计算每个 field 随对应的值
                val fieldsValueMap = fun.getFields
                  .map(fields => {
                    val result = new HashMap[String, String]
                    fields.getFeildName.split(",")
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

              }
            })

          }

          logger.info("aggResultMap : {}", aggResultMap)

          //      logger.info(aggResultMap.toString)

          // 聚合操作完成完成 aggResultMap 装填完成

          val selectors = getSelector(alarmRule)

          // 确定别名所要对比的时间,或者要保存的 '单个周期时长' (预警需要保存七个周期时长)
          val selectorToMap = selectors.map(selector => {
            (selector._1, getRelationWindow(selector._1, selector._2, rule))
          }).toMap

          // 每次定时器的都要执行的操作 将能聚合出的指标加入状态中
          operatorAggState(time, selectorToMap, aggResultMap)

          // 到达此步 所有计算操作完成 , 准备进行 加载预警判断

          if (selectors.map(_._1).forall(aggResultMap.containsKey)) {

            // 清除 aggResultMap 中不被 alarmRule 需要的部分
            aggResultMap.map(_._1).filter(aliasName => {
              !selectors.map(_._1).contains(aliasName)
            }).foreach(aggResultMap.remove)

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

            //        val allGroupingName = groupingNameList.flatMap(_.split(",")).distinct

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

            val rules = ctx.getBroadcastState(StateDescriptor.ruleState)
              .immutableEntries()
              .map(_.getValue)
              .filter(rule => "start".equalsIgnoreCase(rule.getRuleState))

            val customs = getCustoms(rules)

            //            logger.info("customs : {}", customs)

            logger.info("alarmDataList : {}", alarmDataList)

            alarmDataList.filter(alarmFilter(time, _, alarmRule))
              .map(item => {
                val allGroupingName = new JSONObject()
                item.values.map(_._1)
                  .map(JSON.parseObject)
                  .foreach(json => json.foreach(obj => allGroupingName.put(obj._1, obj._2)))
                (allGroupingName, item)
              })
              .foreach(tuple => {
                val alarmResult = new JSONObject(result)
                alarmResult.put("filters", tuple._1)
                val alarmRule = JSON.parseObject(rule.getAlarmRule.toJSONString)
                reformAlarmRule(alarmRule, tuple._2, time)
                alarmResult.put("alarmRule", alarmRule)
                //                logger.info("alarmResult {}", alarmResult)
                if (screenAlarmData(alarmResult, customs)) {
                  out.collect(alarmResult)
                } else {
                  logger.info("result : {}", result)
                }
              })

          }

        } else {
          logger.warn("Rule NO.{} 不是 start 状态 ! 清空缓存的所有状态", rule.getRuleID)
          detailState.clear()
          aggState.clear()
          //        hostNote.clear()
        }

      } catch {
        case e: Exception =>
          logger.error(s"数据计算出错,是否规则 ${rule.getRuleID} 违法")
          e.printStackTrace()
      }


    }

  }

  /**
   * 对明细状态进行清除
   *
   * @param time          当前时间
   * @param dsToMaxWindow datasource 和 需要保存的最大时时间
   */
  def cleanDetailState(time: Long, dsToMaxWindow: Iterable[Rule.AggregatorFun]): Unit = {

    // 清除已经不在datasource中的 key
    detailState.keys().filter(datasource => {
      !dsToMaxWindow.map(_.getDatasource).contains(datasource)
    }).foreach(detailState.remove)

    // 清楚 datasource 中已经不在最大窗口大小中的数据
    dsToMaxWindow.foreach(fun => {
      if (detailState.contains(fun.getDatasource)) {
        detailState.get(fun.getDatasource)
          .map(_._1)
          .filter(_ < (time - fun.getWindow * 60 * 1000))
          .foreach(detailState.get(fun.getDatasource).remove)
      }
    })

  }

  /**
   * 清理 aggState 中过期的过期的数据 并将计算出来的数据 加入状态
   *
   * @param time
   * @param selectorToMap
   * @param aggResultMap
   */
  def operatorAggState(time: Long, selectorToMap: Map[String, Long], aggResultMap: Map[String, Map[String, String]]): Unit = {

    // 将aggState中不含的key进行清除
    aggState.keys().filter(key => {
      !selectorToMap.map(_._1).contains(key)
    }).foreach(aggState.remove)

    selectorToMap.foreach(item => {
      if (aggState.contains(item._1)) {
        // 去除七个周期以外的数据
        aggState.get(item._1).map(_._1)
          .filter(_ < (time - 6 * selectorToMap(item._1)))
          .foreach(aggState.get(item._1).remove)
      } else {
        aggState.put(item._1, new HashMap[Long, Map[String, String]])
      }
      // 将当前的 聚合数据 加入状态中
      if (aggResultMap.containsKey(item._1)) {
        aggState.get(item._1).put(time, aggResultMap.get(item._1))
      }

    })

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
      case ComputeEnum.division =>
        if (secondValue.toDouble == 0) 0 else df.format(firstValue.toDouble / secondValue.toDouble).toDouble
      case ComputeEnum.multiplication => firstValue.toDouble + secondValue.toDouble
    }
    result.toString
  }

  def getRelationAliasName(aliasName: String, rule: Rule): String = {
    val relationPostAggFun = rule.getPostAggregatorFun.filter(_.getAliasName.equals(aliasName))
    if (relationPostAggFun.nonEmpty) {
      relationPostAggFun.get(0).getFields.get(0).getFeildName.split(",")(0)
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
      case LimitOperatorEnum.HOURCHAINDOWN | LimitOperatorEnum.HOURCHAINUP => 60 * 60 * 1000L
      case LimitOperatorEnum.YESTERDAYCHAINUP | LimitOperatorEnum.YESTERDAYCHAINDOWN => 24 * 60 * 60 * 1000L
      case LimitOperatorEnum.CHAINDOWN | LimitOperatorEnum.CHAINUP =>
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
    rule.getAggregatorFun.filter(_.getAliasName.equals(getRelationAliasName(aliasName, rule))).get(0).getGroupingKeyNames.mkString(",")
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
        result ++ alarm.getJSONArray("feilds").flatMap(obj => getSelector(obj.asInstanceOf[JSONObject]))
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
        alarm.getJSONArray("feilds").forall(bool => alarmFilter(time, alarmData, bool.asInstanceOf[JSONObject]))
      case LogicEnum.OR =>
        // 遇到 判断连接条件为or , 则 内部只要存在一个 true 就行
        alarm.getJSONArray("feilds").exists(bool => alarmFilter(time, alarmData, bool.asInstanceOf[JSONObject]))
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
          case LimitOperatorEnum.LESS => //println(currentValue + "<" + targetValue)
            currentValue < targetValue
          case LimitOperatorEnum.LESS_EQUAL => //println(currentValue + "<=" + targetValue)
            currentValue <= targetValue
          case LimitOperatorEnum.GREATER => //println(currentValue + ">" + targetValue)
            currentValue > targetValue
          case LimitOperatorEnum.GREATER_EQUAL => //println(currentValue + ">=" + targetValue)
            currentValue >= targetValue
          case _ =>
            val lastTimeAggResult = aggState.get(alarmColumn).get(time - alarmData(alarmColumn)._3)
            // 上一时间有数据 统计出了指标数据
            if (lastTimeAggResult != null) {
              val lastValueString = lastTimeAggResult.get(alarmData(alarmColumn)._1)
              // 上一时间的指标数据中含有当前 名称的指标
              if (lastValueString != null) {
                val lastValue = lastValueString.toDouble
                if (Array(LimitOperatorEnum.CHAINDOWN, LimitOperatorEnum.YESTERDAYCHAINDOWN, LimitOperatorEnum.HOURCHAINDOWN).contains(limitOperatorEnum)) {
                  (lastValue - currentValue) / lastValue > targetValue
                } else if (Array(LimitOperatorEnum.CHAINUP, LimitOperatorEnum.YESTERDAYCHAINUP, LimitOperatorEnum.HOURCHAINUP).contains(limitOperatorEnum)) {
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
        alarm.getJSONArray("feilds")
          .foreach(alarmChild => reformAlarmRule(alarmChild.asInstanceOf[JSONObject], alarmData, time))
      case LogicEnum.SELECTOR =>
        val alarmColumn = alarm.getString("alarmColumn")
        val alarmSelector = alarmData(alarmColumn)
        val window = alarmSelector._3
        alarm.put("value", alarmSelector._2)
        //        alarm.put("groupName", JSON.parseObject(alarmSelector._1))
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
          aggState.get(alarmColumn).get(timestamp).containsKey(groupName)) {
          val value = aggState.get(alarmColumn).get(timestamp).get(groupName)
          if (value != null) {
            result.put(format.format(timestamp), value.toDouble)
          }
        }
      })
    }
    result
  }

  def getCustoms(rules: Iterable[Rule]): HashMap[String, Map[Map[String, String], String]] = {
    val customs = new HashMap[String, Map[Map[String, String], String]]
    rules.foreach(rule => {
      // 生成数据的拦截条件
      val ruleId = rule.getRuleID
      val category = rule.getCategory
      // 先对特定的 大类型 进行初始化
      if (!customs.contains(category)) {
        customs.put(category, new HashMap[Map[String, String], String]())
      }
      val filters = rule.getFilters
      rule.getAggregatorFun.map(_.getFilters)
        .filter(fil => fil != null && fil.nonEmpty)
        .flatMap(getHostSelector)
        .groupBy(_._1)
        .mapValues(iter => iter.map(_._2).distinct.mkString(","))
        .foreach(entry => filters.put(entry._1, entry._2))
      if (filters.size() != 0) {
        customs.get(category).put(filters, ruleId)
      }
    })
    customs
  }

  def getHostSelector(filter: JSONObject): ArrayBuffer[(String, String)] = {
    val result = new ArrayBuffer[(String, String)]()
    LogicEnum.fromString(filter.getString("type")) match {
      // 对selector类型的对象, 直接获取 (alarmColumn,compareOperator)
      case LogicEnum.SELECTOR =>
        if ("in".equals(filter.getString("operator"))) {
          result :+ (filter.getString("key"), filter.getString("value"))
        } else {
          result
        }
      // 对于非selector类型的对象, 进入内部迭代遍历
      case _ =>
        result ++ filter.getJSONArray("feilds").flatMap(obj => getHostSelector(obj.asInstanceOf[JSONObject]))
    }
  }

  /**
   *
   * @param value
   * @return
   */
  def screenAlarmData(value: JSONObject, customs: HashMap[String, Map[Map[String, String], String]]): Boolean = {
    val category = value.getString("category")
    //    println(category)
    if (customs.contains(category)) {
      val appName = value.getString("appName")
      val env = value.getString("env")

      // 是否有对当前 appName 的限制   如果有
      val appNameCustoms = customs.get(category)
        .filter(entry => entry._1.get("appName") != null && entry._1.get("appName").split(",").contains(appName) && entry._1.get("env").equals(env))

      //      println(appNameCustoms + " : " + value)

      if (appNameCustoms.nonEmpty) {

        val ruleId = value.getString("ruleId")
        val groupingName = value.getJSONObject("filters")

        // 是是否有 进一层次的限制
        val target = appNameCustoms.filter(_._1.size() > 2)
          .filter(entry => {
            entry._1.exists(item => {
              groupingName.containsKey(item._1) && item._2.split(",").contains(groupingName.get(item._1))
            })
          })
          .values

        // 如果有 则包含当前 ruleId
        if (target.nonEmpty) {
          target.contains(ruleId)
        } else {
          val currentAppNameFilter = appNameCustoms.filter(_._1.size() == 2).filter(item => appName.equals(item._1.get("appName")))
          if (currentAppNameFilter.nonEmpty) {
            //如果有 appName限制 则和当前 appName 限制一致
            currentAppNameFilter.values.contains(ruleId)
          } else {
            true
          }

        }

      } else {
        true
      }

    } else {
      false
    }
  }

}