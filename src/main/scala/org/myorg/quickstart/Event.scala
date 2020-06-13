package org.myorg.quickstart

case class Event(var ip: String, var uid: String, var eventType: String, var timestamp: Int, var impressionId: String){
  override def toString: String=
    s"(ip: $ip, uid: $uid, eventType: $eventType, timestamp: $timestamp, impressionId: $impressionId)"
}