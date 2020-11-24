package com.xinye.operator

import java.text.DecimalFormat

import com.alibaba.fastjson.JSONObject
import com.xinye.base.Rule
import com.xinye.config.state.StateDescriptor
import com.xinye.enums.{AggregatorFunctionType, ComputeEnum, LimitOperatorEnum, LogicEnum, RuleSateEnum}
import com.xinye.operator.pojo.DynamicKey
import org.apache.flink.api.common.state.{ListState, MapState, MapStateDescriptor, StateTtlConfig, ValueState}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

class DynamicPostAggregateFun extends
  KeyedBroadcastProcessFunction[Int, (Long, DynamicKey, Map[String, Any]), Rule,
    ((Long, Int), ArrayBuffer[mutable.Map[String, Any]])] {
  var postState: ValueState[Int] = _
  private val LOG = LoggerFactory.getLogger(classOf[DynamicPostAggregateFun])
  var postMapState: MapState[Long, ArrayBuffer[mutable.Map[String, Any]]] = _
  var currentRule: Rule = _

  override def open(parameters: Configuration): Unit = {
    postState = getRuntimeContext.getState(StateDescriptor.postState)
    val ttlConfig = StateTtlConfig.newBuilder(Time.hours(3))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .neverReturnExpired()
      .build()
    val dynamicPost = new MapStateDescriptor[Long, ArrayBuffer[mutable.Map[String, Any]]]("DynamicPost", classOf[Long], classOf[ArrayBuffer[mutable.Map[String, Any]]])
    dynamicPost.enableTimeToLive(ttlConfig)
    postMapState = getRuntimeContext.getMapState(dynamicPost)

  }

  override def processElement(value: (Long, DynamicKey, Map[String, Any]),
                              ctx: KeyedBroadcastProcessFunction[Int, (Long, DynamicKey, Map[String, Any]), Rule, ((Long, Int), ArrayBuffer[mutable.Map[String, Any]])]#ReadOnlyContext,
                              out: Collector[((Long, Int), ArrayBuffer[mutable.Map[String, Any]])]): Unit = {

    handleMetrics(value, ctx.currentWatermark())
    if (postState.value() == 0) {
      val fature = value._1 + 60 * 1 * 1000
      ctx.timerService().registerEventTimeTimer(fature)
    }

  }

  override def processBroadcastElement(value: Rule,
                                       ctx: KeyedBroadcastProcessFunction[Int, (Long, DynamicKey, Map[String, Any]), Rule,
                                         ((Long, Int), ArrayBuffer[mutable.Map[String, Any]])]#Context,
                                       out: Collector[((Long, Int), ArrayBuffer[mutable.Map[String, Any]])]): Unit = {
    StateDescriptor.changeBroadcastState(value, ctx.getBroadcastState(StateDescriptor.dynamicPostAggregateRuleMapState))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedBroadcastProcessFunction[Int, (Long, DynamicKey, Map[String, Any]), Rule, ((Long, Int), ArrayBuffer[mutable.Map[String, Any]])]#OnTimerContext,
                       out: Collector[((Long, Int), ArrayBuffer[mutable.Map[String, Any]])]): Unit = {
    LOG.info("postAgg触发！")
    val postRuleState = ctx.getBroadcastState(StateDescriptor.dynamicPostAggregateRuleMapState)
    val ruleID = ctx.getCurrentKey
    val rule = postRuleState.get(ruleID)
    RuleSateEnum.fromString(rule.getRuleState) match {
      case RuleSateEnum.ACTIVE =>
        val postaggregatorFun = rule.getPostAggregatorFun.asScala
        val mapsateList = postMapState.iterator().asScala.toList
        if (mapsateList.nonEmpty) {
          //获取上一批数据进行计算
          val metircState = mapsateList.head
          val longs = metircState.getKey
          val metricsList = metircState.getValue.toList
          val resultBuffer = new ArrayBuffer[mutable.Map[String, Any]]()
          if (postaggregatorFun.nonEmpty) {
            //进行判断如果规则发生改变时，后聚合所涉及的字段必须是前聚合统计过的才可以进入后聚合
            for (post <- postaggregatorFun) {
              if (post.getFields.size() == 2) {
                val fields = post.getFields
                val field_first: Rule.Field = fields.get(0)
                val firstAgg = compute(field_first, metricsList)
                val field_second = fields.get(1)
                val firstName = field_first.getFieldName
                val secondName = field_second.getFieldName
                val secondAgg = compute(field_second, metricsList)
                val operator = post.getOperator
                val aliasName = post.getAliasName
                val resultList = DynamicPostAggregateFun.handleRate(firstAgg, secondAgg, firstName, secondName, operator, aliasName)
                resultBuffer.addAll(resultList.map(postmap => postmap))

              } else if (post.getFields.size() == 1) {
                val field = post.getFields.get(0)
                val aliasName = post.getAliasName
                val feildName: String = field.getFieldName
                val map = compute(field, metricsList)
                val result = map.mapValues(values => {
                  val value = values.getOrDefault(feildName, -1)
                  values.put(aliasName, value)
                  values.remove(feildName)
                  values
                })
                resultBuffer.addAll(result.values)
              }
            }

          } else {
            val aggRule = rule.getAggregatorFun
            val names = aggRule.map(agg => agg.getAliasName)
            val b = names.map(aliasName => {
              val m = metricsList.filter(metric => metric.keySet.contains(aliasName)).toList
              m.groupBy(map => {
                val hashMap = new HashMap[String, Any]()
                hashMap.putAll(map)
                hashMap.remove(aliasName)
                hashMap.entrySet().toList
              })
            })
            val result = names.map(ali => b.map(i => {
              val filterList = i.filter(entry => {
                val listvalue: List[mutable.Map[String, Any]] = entry._2
                val maps = listvalue.filter(value => value.contains(ali))
                if (maps.isEmpty) false else true
              })
              val combineList = filterList.map(entry => {
                val hashMap = new HashMap[String, Any]()
                val listKey = entry._1
                listKey.foreach(tuple => hashMap.put(tuple.getKey, tuple.getValue))
                val listvalue: List[mutable.Map[String, Any]] = entry._2
                val maps = listvalue.filter(value => value.contains(ali))
                if (maps.nonEmpty) {
                  val sum = maps.map(value => value(ali).toString.toDouble).sum
                  hashMap.put(ali, sum)
                  hashMap
                }
              })
              combineList
            })).map(l => l.flatten)
            val buffer = result.flatMap(a => a.map(map => map.asInstanceOf[mutable.Map[String, Any]]))
            resultBuffer.addAll(buffer)
          }
          if (resultBuffer.nonEmpty) {
            out.collect(((ctx.currentWatermark(), ctx.getCurrentKey), resultBuffer))
          }
          //移除过期数据
          postMapState.remove(longs)
          postState.update(0)
        }
      case _ =>
    }
  }

  //将数据进行存储
  def handleMetrics(metrics: (Long, DynamicKey, Map[String, Any]), l: Long): Unit = {
    val list = postMapState.get(l)
    if (list == null) {
      val buffer = new ArrayBuffer[Map[String, Any]]()
      buffer.append(metrics._3)
      postMapState.put(l, buffer)
    } else {
      list.add(metrics._3)
      postMapState.put(l, list)
    }
  }

  def compute(field_first: Rule.Field, metricsList: List[mutable.Map[String, Any]]): Predef.Map[List[Any], Map[String, Any]] = {
    val firstName = field_first.getFieldName
    val firstFilters = field_first.getFilters
    var firstList: List[Map[String, Any]] = null
    if (firstFilters != null && !firstFilters.isEmpty) {
      firstList = metricsList.filter(map => map.keySet.contains(firstName))
        .filter(metric => {
          DynamicPostAggregateFun.filter(metric, firstFilters)
        })
    } else firstList = metricsList.filter(map => map.keySet.contains(firstName))
    val firstAggType = field_first.getPostAgg
    val fristCombineList = DynamicPostAggregateFun.combiner(firstList, firstName)
    var firstAgg: Predef.Map[List[Any], Map[String, Any]] = null
    AggregatorFunctionType.fromString(firstAggType) match {
      case AggregatorFunctionType.COUNT =>
        firstAgg = DynamicPostAggregateFun.count(fristCombineList, firstName)
      case AggregatorFunctionType.SUM | _ =>
        firstAgg = DynamicPostAggregateFun.sum(fristCombineList, firstName)
    }
    firstAgg
  }


}

object DynamicPostAggregateFun {
  def filter(metrics: Map[String, Any], filters: JSONObject, flag: Boolean = false): Boolean = {
    var target = flag
    val `type` = filters.getString("type")
    LogicEnum.fromString(`type`) match {
      case LogicEnum.AND =>
        var tempTarg = true
        val arr = filters.getJSONArray("fields").toArray
        val arrFilter = arr.filter(field => {
          val jobj = field.asInstanceOf[JSONObject]
          val fieldType = jobj.getString("type")
          val value = LogicEnum.fromString(fieldType)
          if (value == LogicEnum.SELECTOR) true else false
        })
        val nextFilter = arr.filter(field => {
          val jobj = field.asInstanceOf[JSONObject]
          val fieldType = jobj.getString("type")
          val value = LogicEnum.fromString(fieldType)
          if (value == LogicEnum.SELECTOR) false else true
        })
        for (l <- arrFilter) {
          val field = l.asInstanceOf[JSONObject]
          val key = field.getString("key")
          val operator = field.getString("operator")

          val value = field.getString("value")
          val metricsOpt = metrics.get(key)
          if (metricsOpt.isDefined) {
            val metricTag = metricsOpt.get.toString
            LimitOperatorEnum.fromString(operator) match {
              case LimitOperatorEnum.EQUAL => if (value.equalsIgnoreCase(metricTag) && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.NOT_EQUAL => if (!value.equalsIgnoreCase(metricTag) && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.LESS => if (value.toDouble > metricTag.toString.toDouble && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.LESS_EQUAL => if (value.toDouble >= metricTag.toString.toDouble && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.GREATER => if (value.toDouble < metricTag.toString.toDouble && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.GREATER_EQUAL => if (value.toDouble <= metricTag.toString.toDouble && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.IN => if (metricTag.split(",").contains(value) && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.NOTIN => if (!metricTag.split(",").contains(value) && tempTarg) tempTarg = true else tempTarg = false
              case LimitOperatorEnum.REGEX => if (metricTag.r.findFirstIn(value).isEmpty && tempTarg) tempTarg = false else tempTarg = true
            }
          } else {
            tempTarg = false
          }

        }

        if (!nextFilter.isEmpty && !filter(metrics, nextFilter(0).asInstanceOf[JSONObject], flag)) {
          tempTarg = false
        }
        target = tempTarg
      case LogicEnum.OR =>
        var tempTarg = false
        val arr = filters.getJSONArray("fields").toArray
        val filterArr_1 = arr.filter(field => {
          val jobj = field.asInstanceOf[JSONObject]
          val fieldType = jobj.getString("type")
          val value = LogicEnum.fromString(fieldType)
          if (value == LogicEnum.SELECTOR) true else false
        })
        for (l <- filterArr_1) {
          val field = l.asInstanceOf[JSONObject]
          val key = field.getString("key")
          val operator = field.getString("operator")
          val value = field.getString("value")

          val metricsOpt = metrics.get(key)
          if (metricsOpt.isDefined) {
            val metricTag = metricsOpt.get.toString
            LimitOperatorEnum.fromString(operator) match {
              case LimitOperatorEnum.EQUAL => if (value.equalsIgnoreCase(metricTag) || tempTarg) tempTarg = true
              case LimitOperatorEnum.NOT_EQUAL => if (!value.equalsIgnoreCase(metricTag) || tempTarg) tempTarg = true
              case LimitOperatorEnum.LESS => if (value > metricTag || tempTarg) tempTarg = true
              case LimitOperatorEnum.LESS_EQUAL => if (value >= metricTag || tempTarg) tempTarg = true
              case LimitOperatorEnum.GREATER => if (value < metricTag || tempTarg) tempTarg = true
              case LimitOperatorEnum.GREATER_EQUAL => if (value <= metricTag || tempTarg) tempTarg = true
              case LimitOperatorEnum.IN => if (metricTag.split(",").contains(value) || tempTarg) tempTarg = true
              case LimitOperatorEnum.NOTIN => if (!metricTag.split(",").contains(value) || tempTarg) tempTarg = true
              case LimitOperatorEnum.REGEX => if (metricTag.r.findFirstIn(value).isDefined || tempTarg) tempTarg = true
            }
          }

        }
        val filterArr_2 = arr.filter(field => {
          val jobj = field.asInstanceOf[JSONObject]
          val fieldType = jobj.getString("type")
          val value = LogicEnum.fromString(fieldType)
          if (value == LogicEnum.SELECTOR) false else true
        })
        if ((!filterArr_2.isEmpty && !filter(metrics, filterArr_2(0).asInstanceOf[JSONObject], target)) || tempTarg) {
          target = true
        }
      case LogicEnum.SELECTOR =>
        var tempTarg = false
        val key = filters.getString("key")
        val operator = filters.getString("operator")
        val value = filters.getString("value")
        val metricTagOpt = metrics.get(key)
        if (metricTagOpt.isDefined) {
          val metricTag = metricTagOpt.get.toString
          LimitOperatorEnum.fromString(operator) match {
            case LimitOperatorEnum.EQUAL => if (value.equalsIgnoreCase(metricTag)) tempTarg = true
            case LimitOperatorEnum.NOT_EQUAL => if (!value.equalsIgnoreCase(metricTag)) tempTarg = true
            case LimitOperatorEnum.LESS => if (value.toDouble > metricTag.toDouble) tempTarg = true
            case LimitOperatorEnum.LESS_EQUAL => if (value.toDouble >= metricTag.toDouble) tempTarg = true
            case LimitOperatorEnum.GREATER => if (value.toDouble < metricTag.toDouble) tempTarg = true
            case LimitOperatorEnum.GREATER_EQUAL => if (value.toDouble <= metricTag.toDouble) tempTarg = true
            case LimitOperatorEnum.IN => if (metricTag.split(",").contains(value)) tempTarg = true
            case LimitOperatorEnum.NOTIN => if (!metricTag.split(",").contains(value)) tempTarg = true
            case LimitOperatorEnum.REGEX => if (metricTag.r.findFirstIn(value).isEmpty) tempTarg = false
          }
        }

        target = tempTarg
    }
    target
  }

  def combiner(list: List[Map[String, Any]], name: String): Predef.Map[List[Any], List[Map[String, Any]]] = {
    list.groupBy(map => {
      val set = map.keySet.toList.toBuffer
      val i = set.indexOf(name)
      set.remove(i)
      set.map(key => map(key)).toList
    })
  }

  def sum(list: Predef.Map[List[Any], List[Map[String, Any]]], name: String): Predef.Map[List[Any], Map[String, Any]] = {
    list.mapValues(list => list.reduceLeft((m1, m2) => {
      m1.put(name, m1(name).toString.toDouble + m2(name).toString.toDouble)
      m1
    }))
  }

  def count(list: Predef.Map[List[Any], List[Map[String, Any]]], name: String): Predef.Map[List[Any], Map[String, Any]] = {
    list.mapValues(arr => {
      val maps = arr.filter(map => map.getOrDefault(name, 0).toString.toDouble != 0.0)
      val head = arr.head
      head.put(name, maps.size)
      head
    })
  }

  def handleRate(first: Predef.Map[List[Any], Map[String, Any]], second: Predef.Map[List[Any], Map[String, Any]], firstName: String, secondName: String, operator: String, aliasName: String): List[Map[String, Any]] = {

    val m1 = new mutable.HashMap[List[Any], mutable.Map[String, Any]]()
    val m2 = new mutable.HashMap[List[Any], mutable.Map[String, Any]]()
    m1.putAll(first)
    m2.putAll(second)
    val df = new DecimalFormat("#.00")
    var reulst: List[Map[String, Any]] = null
    if (m2.keySet.contains(List())) {
      reulst = m2.map(tuple => {
        var sum: Any = null
        val firstDouble = m1.getOrDefault(tuple._1, new HashMap[String, Any]).getOrDefault(firstName, 0).toString.toDouble
        ComputeEnum.fromString(operator) match {
          case ComputeEnum.addition => sum = tuple._2.getOrDefault(firstName, 0).toString.toDouble + m2(List())(secondName).toString.toDouble
          case ComputeEnum.subtraction => sum = firstDouble - m2(List())(secondName).toString.toDouble
          case ComputeEnum.multiplication => sum = firstDouble * m2(List())(secondName).toString.toDouble
          case ComputeEnum.division => sum = df.format(firstDouble / m2(List())(secondName).toString.toDouble)
        }
        val hashMap = new mutable.HashMap[String, Any]()
        hashMap.putAll(tuple._2)
        //                hashMap.remove(secondName)
        hashMap.put(aliasName, sum)
        hashMap
      }).toList
    } else {
      reulst = m2.map(tuple => {
        var sum: Any = null
        val map = new mutable.HashMap[String, Any]()
        val f1 = m1.getOrDefault(tuple._1, map).getOrDefault(firstName, 0).toString.toDouble
        val f2 = tuple._2.getOrDefault(secondName, 0).toString.toDouble
        ComputeEnum.fromString(operator) match {
          case ComputeEnum.addition => sum = f1.toString.toDouble + f2
          case ComputeEnum.subtraction => sum = f1 - f2
          case ComputeEnum.multiplication => sum = f1 * f2
          case ComputeEnum.division => sum = df.format(f1 / f2)
        }
        val hashMap = new mutable.HashMap[String, Any]()
        hashMap.putAll(tuple._2)
        //                hashMap.remove(secondName)
        hashMap.put(aliasName, sum)
        hashMap
      }).toList
    }
    reulst
  }


}


