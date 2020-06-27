package org.myorg.quickstart

import org.apache.flink.streaming.api.scala.DataStream

/**
 * This class is called to filter anomalies above(or under depending on the anomaly) the thresholds defined in the main
 * function
 */

object Anomalies {

  def anomalie_ctr(lines: DataStream[(String, Double)], seuil_ctr: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 > seuil_ctr)
  }

  def anomalie_avg_time_diff(lines: DataStream[(String, Double)], seuil: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 < seuil && e._2 > 0.0)
  }

  def anomalie_avg_click_delay(lines: DataStream[(String, Double)], seuil: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 < seuil)
  }

  def anomalie_var_time_diff(lines: DataStream[(String, Double)], seuil: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 < seuil && e._2 >= 0.0)
  }
}
