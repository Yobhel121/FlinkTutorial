package com.atguigu.chapter07

import com.atguigu.chapter05.ClickSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object TopNProcessAllWindowExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.map(_.url)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          val urlCountMap = mutable.Map[String, Long]()
          elements.foreach(
            data => urlCountMap.get(data) match {
              case Some(count) => urlCountMap.put(data, count + 1)
              case None => urlCountMap.put(data, 1)
            }
          )

          val urlCountList = urlCountMap.toList.sortBy(-_._2).take(2)

          val result = new StringBuilder()
          result.append(s"============窗口：${context.window.getStart} ~ ${context.window.getEnd}===========\n")
          for (i <- urlCountList.indices){
            val tuple = urlCountList(i)
            result.append(s"浏览量Top ${i+1} ")
              .append(s"url: ${tuple._1} ")
              .append(s"浏览量是： ${tuple._2} \n")
          }

          out.collect(result.toString())

        }
      }).print()

    env.execute()

  }

}
