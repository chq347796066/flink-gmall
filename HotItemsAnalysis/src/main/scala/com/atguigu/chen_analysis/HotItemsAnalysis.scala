package com.atguigu.chen_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class UsersBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class ItemsViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItemsAnalysis {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 定义事件时间语义

    // 从文件中读取数据，并转换成样例类，提取时间戳生成watermark
    val inputStream: DataStream[String] = env.readTextFile("D:\\java_workspace\\flink-gmall\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val usersBehaviorDs=inputStream.map(data=>{
      val arr=data.split(",")
      UsersBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
    }).filter(_.behavior=="pv")
      .assignAscendingTimestamps(_.timestamp*1000)
    val itemViewResult=usersBehaviorDs.keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new CountAggregate(),new ItemViewWindowResult())
    val top5Result=itemViewResult.keyBy(_.windowEnd)
      .process(new TopNHotItems(5))
    top5Result.print("top5")
    env.execute("hotitems job")

  }

}


class TopNHotItems(n:Int) extends KeyedProcessFunction[Long,ItemsViewCount,String]{

  var listState:ListState[ItemsViewCount]=_

  override def open(parameters: Configuration): Unit = {
    listState=getRuntimeContext.getListState(new ListStateDescriptor[ItemsViewCount]("itemsListCount-state",classOf[ItemsViewCount]))
  }

  override def processElement(value: ItemsViewCount, ctx: KeyedProcessFunction[Long, ItemsViewCount, String]#Context, out: Collector[String]): Unit = {
    listState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)


  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemsViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer=new ListBuffer[ItemsViewCount]
    val listStateIterator=listState.get().iterator()
    while (listStateIterator.hasNext){
      listBuffer+=listStateIterator.next()
    }
    listState.clear()
    val sortItemsCount=listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(n)
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")
    for(i <- sortItemsCount.indices){
      val currentItemViewCount = sortItemsCount(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }
    result.append("\n==================================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())

  }
}

class CountAggregate extends AggregateFunction[UsersBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UsersBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class ItemViewWindowResult extends WindowFunction[Long,ItemsViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemsViewCount]): Unit = {
      val itemId=key
      val count=input.iterator.next();
      val itemsViewCount=ItemsViewCount(itemId, window.getEnd,count )
      out.collect(itemsViewCount)

  }
}