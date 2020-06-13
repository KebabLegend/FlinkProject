package org.myorg.quickstart

import org.apache.flink.streaming.api.scala.DataStream

object Anomalies {

  def anomalie_ctr(lines: DataStream[(String, Double)], seuil_ctr: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 > seuil_ctr)
  }

  def anomalie_avg_time_diff(lines: DataStream[(String, Double)], seuil: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 < seuil && e._2 > 0.0)
  }

  def anomalie_var_time_diff(lines: DataStream[(String, Double)], seuil: Double): DataStream[(String, Double) ] = {
    lines.filter(e => e._2 < seuil && e._2 >= 0.0)
  }

  //A RAJOUTER : FONCTIONS QUI CHECK LE MIN OU ALORS UN PARAMETRE POUR DEFINIR SI LE SEUIL DOIT ETRE UN MIN OU UN MAX
  // TYPIQUEMENT : AVG TIME DIFF DOIT ETRE UN SEUIL MINIMUM
}
