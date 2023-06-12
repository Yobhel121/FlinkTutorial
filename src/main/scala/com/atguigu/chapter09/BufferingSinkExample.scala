package com.atguigu.chapter09

import com.atguigu.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object BufferingSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .addSink(new BufferingSink(10))

    env.execute()
  }

  // 实现自定义的SinkFunction
  class BufferingSink(threshold: Int) extends SinkFunction[Event] with CheckpointedFunction {
    // 定义列表状态，保存要缓冲的数据
    var bufferedState: ListState[Event] = _
    // 定义一个本地变量列表
    val bufferedList = ListBuffer[Event]()


    override def invoke(value: Event, context: SinkFunction.Context): Unit = {
      // 缓冲数据
      bufferedList += value

      // 判断是否达到了阈值
      if (bufferedList.size == threshold){
        // 输出到外部系统，用打印到控制台模拟
        bufferedList.foreach( data => println(data) )
        println("===========输出完毕==============")

        // 清空缓冲
        bufferedList.clear()
      }
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      // 清空状态
      bufferedState.clear()
      for (data <- bufferedList){
        bufferedState.add(data)
      }
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      bufferedState = context.getOperatorStateStore.getListState(new ListStateDescriptor[Event]("buffered-list", classOf[Event]))
      // 判断如果是从故障中恢复，那么就将状态中的数据添加到局部变量中
      if (context.isRestored){
        import scala.collection.convert.ImplicitConversions._
        for (data <- bufferedState.get()){
          bufferedList += data
        }
      }
    }
  }
}
