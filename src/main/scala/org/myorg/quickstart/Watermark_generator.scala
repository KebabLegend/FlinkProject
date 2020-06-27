package org.myorg.quickstart

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import scala.math.max

/**
 * This class implements our watermark generator
 * We pick timestamps in the timestamp field of the event.
 */
class Watermark_generator extends AssignerWithPeriodicWatermarks[Event] {

  val maxOutOfOrderness = 5000L // 5 seconds

  var currentMaxTimestamp: Long = _

  override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
    val timestamp = element.timestamp * 1000
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark(): Watermark = {
    // return the watermark as current highest timestamp minus the out-of-orderness bound
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}
