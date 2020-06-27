/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart
import java.util.Properties

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit

import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy

/**
 * This class is the main class which has to be run.
 * It contains the whole pipeline from the kafka consumer that get the topics to the output of the anomalies
 */
object StreamingJob {

  //Definition of the different thresholds to detect anomalies
  val seuil_ctr = 0.3
  val seuil_avg_time_click = 1.5
  //val seuil_var_time_click = 1
  val seuil_avg_click_delay = 2
  //We chose to use sliding windows. We set the width of the windows and the trigger intervals in seconds
  val window_size = 1800  // 30 minutes
  val window_slide = 300  // 5 minutes

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    // Get data from Kafka
    val source_click = new FlinkKafkaConsumer011[Event](List("clicks","displays").asJava, KafkaStringSchema, properties)
    val stream_click = env
      .addSource(source_click)
      // We define a custom watermark generator created in the file Watermark_generator.scala
      .assignTimestampsAndWatermarks(new Watermark_generator())

    // Computation of ctr
    val ctr: DataStream[(String, Double)] = Statistics.ctr_uid(stream_click, window_size, window_slide)
    // Filter the CTR values considered as anomalies according to the set threshold
    val anomalie_ctr = Anomalies.anomalie_ctr(ctr, seuil_ctr)

    // Create a stream composed of only clicks events
    val stream_click_only = stream_click.filter(e => e.eventType == "click")
    // Compute the average time difference between two clicks
    val avg_time_diff : DataStream[(String, Double)] = Statistics.average_time_diff(stream_click_only, window_size, window_slide)
    // Filter anomalies
    val anomalie_avg_td = Anomalies.anomalie_avg_time_diff(avg_time_diff,seuil_avg_time_click)

    // Compute the average delay between each click and the last corresponding display
    val avg_click_delay: DataStream[(String, Double)] = Statistics.average_click_delay(stream_click, window_size, window_slide)
    // Filter anomalies
    val anomalie_click_delay = Anomalies.anomalie_avg_click_delay(avg_click_delay, seuil_avg_click_delay)
    // Compute the variance of time difference between two clicks
    //val var_time_diff : DataStream[(String, Double)] = Statistics.variance_time_diff(stream_click_only, window_size, window_slide)
    //val anomalie_var_td = Anomalies.anomalie_var_time_diff(var_time_diff, seuil_var_time_click)


    // We define a sink for each anomaly type.
    // We write each anomalies in files stored in repositories by anomaly type and date
    val sink_ctr: StreamingFileSink[(String, Double)] = StreamingFileSink
      .forRowFormat(new Path("anomalie_ctr"), new SimpleStringEncoder[(String, Double)]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()
    anomalie_ctr.addSink(sink_ctr)

    val sink_avg_td: StreamingFileSink[(String, Double)] = StreamingFileSink
      .forRowFormat(new Path("anomalie_avg_td"), new SimpleStringEncoder[(String, Double)]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()
    anomalie_avg_td.addSink(sink_avg_td)

    /**val sink_var_td: StreamingFileSink[(String, Double)] = StreamingFileSink
      .forRowFormat(new Path("anomalie_var_td"), new SimpleStringEncoder[(String, Double)]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()
    anomalie_var_td.addSink(sink_var_td)*/

    val sink_avg_click_delay: StreamingFileSink[(String, Double)] = StreamingFileSink
      .forRowFormat(new Path("anomalie_click_delay"), new SimpleStringEncoder[(String, Double)]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()
    anomalie_click_delay.addSink(sink_avg_click_delay)

    env.execute("Flink Streaming Scala API Skeleton")
  }


}

object KafkaStringSchema extends AbstractDeserializationSchema[Event] {

  import org.apache.flink.api.common.typeinfo.TypeInformation
  import org.apache.flink.api.java.typeutils.TypeExtractor

  override def isEndOfStream(t: Event): Boolean = false

// For deserialization, we finished it before the Update of the docker image with docker pull and therefore we weren't able
  // to use JsonDeserializationSchema, which is now more appropriate
  // our custom deserialization still works though
  override def deserialize(bytes: Array[Byte]): Event = {
    val str = new String(bytes, "UTF-8")
    val split = str.split("\"")
    val eventType = split(3)
    val uid = split(7)
    val timestr = split(10).substring(1,split(10).length()-2)
    val ip = split(13)
    val impressionId = split(17)
    val timestamp = timestr.toInt
    Event(ip, uid, eventType, timestamp, impressionId )
  }
//{"eventType":"display", "uid":"87efceb2-fd63-44e6-bcea-2d6bc35d265e", "timestamp":1591278407, "ip":"8.199.222.9", "impressionId": "3b2ffcb8-55b5-4fb3-bd3a-921131e3b365"}
  override def getProducedType: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
}