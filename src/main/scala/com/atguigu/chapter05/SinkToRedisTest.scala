package com.atguigu.chapter05

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 *
 * Project:  FlinkTutorial
 *
 * Created by  wushengran
 */

object SinkToRedisTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)

    // 创建一个Jedis连接的配置项
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build()
    stream.addSink( new RedisSink[Event](conf, new MyRedisMapper) )

    env.execute()
  }

  // 实现RedisMapper接口
  class MyRedisMapper extends RedisMapper[Event]{
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "clicks")

    override def getKeyFromData(t: Event): String = t.user

    override def getValueFromData(t: Event): String = t.url
  }
}
