package org.myorg.quickstart
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import scala.util.Sorting.quickSort

object Statistics {


  def ctr_uid(lines: DataStream[Event], window_size : Int, window_slide: Int ): DataStream[(String, Double)] = {
    val count_events = lines
      .map(event => (event.uid, is_click(event.eventType), 1-is_click(event.eventType)))
      .keyBy(0)
      .timeWindow( Time.seconds(window_size), Time.seconds(window_slide))//sliding Window
      // https://flink.apache.org/news/2015/12/04/Introducing-windows.html
      // deal with event time events and not processing time
      .reduce((a,b) => (a._1, a._2+ b._2, a._3 + b._3))
      .filter(e=> e._3>0)   // Ensure that we don't get clicks with no display (could come from the fact that the
      // display happened in the previous window
      .map(e=> (e._1, compute_ratio(e._2, e._3) ))
    count_events
  }


  def average_time_diff(lines : DataStream[Event], window_size : Int, window_slide: Int):DataStream[(String,Double)] = {
    // temps moyen entre tous les events de la meme ip
    // lines, groupby ip => list of timestamps (String, String, list[String])
    val timestamps_by_uid = lines
      .map(e=>(e.uid,e.timestamp.toString+";"))
      .keyBy(0)
      //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))

      /*
      Note that in order to run this example in event time, the program needs to either use sources that directly define
      event time for the data and emit watermarks themselves, or the program must inject a Timestamp Assigner & Watermark
      Generator after the sources.
      Those functions describe how to access the event timestamps, and what degree of out-of-orderness the event stream
      exhibits.
      */

      .timeWindow( Time.seconds(window_size), Time.seconds(window_slide)) // Sliding Window
      .reduce((x,y) => (x._1,x._2.concat(y._2)))
      .map(e=> (e._1,e._2.split(";")))

    // from list of timestamps => sort and list of diff
    val time_diff_by_uid = timestamps_by_uid
      .map(e=>(e._1,create_diff_list(e._2)))

    // from list of diff => avg of diff
    val avg_time_diff_by_uid = time_diff_by_uid
      .map(e=>(e._1,compute_avg(e._2)))

    avg_time_diff_by_uid

  }

  def average_click_delay(lines : DataStream[Event], window_size : Int, window_slide: Int):DataStream[(String,Double)] = {
    val timestamps_by_uid = lines
      .map(e=>(e.uid,e.timestamp.toString+";",e.eventType+";"))
      .keyBy(0)
      .timeWindow( Time.seconds(window_size), Time.seconds(window_slide))
      .reduce((x,y) => (x._1,x._2.concat(y._2), x._3.concat(y._3)))
      .map(e=> (e._1,e._2.split(";"), e._3.split(";")))
    // e : e._1: uid, e._2: list of timestamp for the corresponding uid, e._3: list of event types (same size as e._2)

    val delay_by_uid = timestamps_by_uid
      .map(e => (e._1, create_list_delays(e._2,e._3)))
      .map(e=> (e._1, compute_avg2(e._2)))
      .filter(e=> !e._2.isNaN)

    delay_by_uid

  }


  def variance_time_diff(lines : DataStream[Event], window_size : Int, window_slide: Int):DataStream[(String,Double)] = {
    // lines, groupby ip => list of timestamps (String, String, list[String])
    val timestamps_by_uid = lines
      .map(e=>(e.uid,e.timestamp.toString+";"))
      .keyBy(0)
      .timeWindow( Time.seconds(window_size), Time.seconds(window_slide)) //
      .reduce((x,y) => (x._1,x._2.concat(y._2)))
      .map(e=> (e._1,e._2.split(";")))

    // from list of timestamps => sort and list of diff
    val time_diff_by_uid = timestamps_by_uid
      .map(e=>(e._1,create_diff_list(e._2)))

    // from list of diff => avg of diff
    val avg_time_diff_by_uid = time_diff_by_uid
      .map(e=>(e._1,e._2, compute_avg(e._2)))

    // compute variance
    val variance_diff_by_uid = avg_time_diff_by_uid
      .map(e => (e._1,compute_var(e._2,e._3)))
        variance_diff_by_uid
  }

  def create_diff_list(list : Array[String]):Array[Int]={
    val list_int = list.map(_.toInt)
    quickSort(list_int)
    val diffs = new Array[Int](list_int.length)
    var i = 0
    for(i <- 1 to list_int.length - 1){
      diffs(i-1) = (list_int(i) - list_int(i-1))
    }
    diffs
  }

  def create_list_delays(timestamps : Array[String], types : Array[String]): Array[Int]={
    /**
     * Create a list of delays between each click and the most recent display
     */

    var clicks = new Array[Int](0)
    var displays = new Array[Int](0)
    for(i <- 0 to timestamps.length - 1){
      if (types(i)=="click"){
        clicks = clicks :+ timestamps(i).toInt
      }
      else{
        displays = displays :+ timestamps(i).toInt
      }

    }
    quickSort(clicks)
    quickSort(displays)
    var idx_last_display = 0
    var delays = new Array[Int](0)
    for(i <- 0 to clicks.length - 1){
      var c = clicks(i)

      var break = false
      for (j <- idx_last_display to displays.length - 1) {
        if (!break){
          var d = displays(j)
          if (d >= c) {
            if (j == 0) {
            }
            else {
              delays = delays :+ (c - displays(j - 1))
              idx_last_display = j - 1
              break = true
            }
          }
        }

      }

    }
    delays
  }

  def compute_avg(list : Array[Int]):Double={
    var avg = 0.0
    for(i<-0 to list.length-1){
      avg = avg + list(i)
    }
    avg / list.length
  }


  def compute_avg2(list : Array[Int]):Double={
    var avg = 0.0
    for(i<-0 to list.length-1){
      avg = avg + list(i)
    }
    avg / list.length
  }


  def compute_var(list : Array[Int],moy: Double):Double={
    var avg = 0.0
    for(i<-0 to list.length-1){
      avg = avg + (list(i) - moy)*(list(i) - moy)
    }

    if (list.length <2) {
      -1.0
    }
    else{
      avg / list.length
    }
  }

  def is_click(event_type: String):Int={
    var return_ = 0
    if (event_type == "click"){
      return_ = 1
    }
    return_
  }




  def compute_ratio(click: Int, display: Int):Double={
    var ratio = 0.0
    if (display != 0){
      ratio = click.toDouble / display.toDouble
    }
    else{
      ratio = 1.0
    }
    ratio
  }
}
