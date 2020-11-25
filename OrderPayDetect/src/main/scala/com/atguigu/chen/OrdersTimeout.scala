package com.atguigu.chen

import com.atguigu.orderpay_detect.{OrderEvent, OrderResult}
import com.atguigu.orderpay_detect.OrderTimeoutWithoutCep.getClass
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrdersTimeout {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("/OrderLog.csv")
    val orderDs=env.readTextFile(resource.getPath)
      .map(line=>{
        val arr=line.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      }).assignAscendingTimestamps(_.timestamp*1000L)
    val result=orderDs
      .keyBy(_.orderId)
      .process(new OrderPayProcess)
    result.print("success")
    val timeout=result.getSideOutput(new OutputTag[OrderResult]("timeout"))
    timeout.print("timeout")
    env.execute("order pay match")

  }

}

class OrderPayProcess extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
  lazy val isPayState=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreateState=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  lazy val timeTsState=getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeTsState", classOf[Long]))

  val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isPayed=isPayState.value()
    val isCreated=isCreateState.value()
    val timeState=timeTsState.value()
    if(value.eventType=="create"){
      if(isPayed){
        out.collect(OrderResult(value.orderId,"payed successfully"))
        isPayState.clear()
        isCreateState.clear()
        timeTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timeState)
      }else{
        //没付款，设置15分钟的定时器
        val ts=value.timestamp*1000+600*1000
        ctx.timerService().registerEventTimeTimer(ts)
        timeTsState.update(ts)
        isCreateState.update(true)
      }

    }else if(value.eventType=="pay"){
      if(isCreated){
        //判断是否超时
        if(value.timestamp*1000<timeState){
          out.collect(OrderResult(value.orderId,"pay successfully"))
        }else{
          ctx.output(orderTimeoutOutputTag,OrderResult(value.orderId,"pay timeout"))
        }
        isPayState.clear()
        isCreateState.clear()
        timeTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timeState)
      }else{
        ctx.timerService().registerEventTimeTimer(value.timestamp*1000)
        timeTsState.update(value.timestamp*1000)
        isPayState.update(true)
      }
    }


  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //pay来了，create没来
    if(isPayState.value()){
      ctx.output(orderTimeoutOutputTag,OrderResult(ctx.getCurrentKey,"order not create"))
    }else{
      //create创建，未付款
      ctx.output(orderTimeoutOutputTag,OrderResult(ctx.getCurrentKey,"order timeout"))
    }
    isPayState.clear()
    isCreateState.clear()
    timeTsState.clear()
  }
}
