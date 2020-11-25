package com.atguigu.chen_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsBySql {

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
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)


    val dataTable = tableEnv.fromDataStream(usersBehaviorDs, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 1. Table API进行开窗聚合统计
    val aggTable = dataTable
      .filter('behavior === "pv")
      .window( Slide over 1.hours every 5.minutes on 'ts as 'sw )
      .groupBy( 'itemId, 'sw)
      .select( 'itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt )


    tableEnv.createTemporaryView("items_behavior",aggTable,'itemId,'windowEnd,'cnt)
    val sql=
      """
        |select * from
        |(select *,row_number() over (partition by windowEnd order by cnt desc) as row_num from items_behavior) t
        |where t.row_num <= 5
        |""".stripMargin
    val topNResult=tableEnv.sqlQuery(sql)
    topNResult.toRetractStream[Row].print("result")
    env.execute("sql execute")

  }

}
