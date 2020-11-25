package com.atguigu.chen

import java.util

import com.atguigu.orderpay_detect.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrdersTimeoutCep {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 0. 从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      // val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)
    val pattern=Pattern.begin[OrderEvent]("create").where(_.eventType=="create")
      .followedBy("pay").where(_.eventType=="pay").within(Time.minutes(15))
    val outputTag=new OutputTag[OrderResult]("timeout")
    val cepStream=CEP.pattern(orderEventStream,pattern)
    val result: DataStream[OrderResult] = cepStream.select(outputTag, new TimeoutFunction, new SelectFunction)
    result.print("success")
    result.getSideOutput(outputTag).print("timeout")
    env.execute("cep job")

  }

}

class TimeoutFunction extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
     val orderId=map.get("create").iterator().next().orderId
    OrderResult( orderId, "timeout")
  }

}

class SelectFunction extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderId=map.get("pay").iterator().next().orderId
    OrderResult(orderId = orderId, resultMsg = "success")
  }
}
