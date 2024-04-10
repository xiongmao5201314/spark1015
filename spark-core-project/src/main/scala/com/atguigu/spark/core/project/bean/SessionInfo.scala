package com.atguigu.spark.core.project.bean

case class SessionInfo ( sessionId :String,
                         count :Long) extends Ordered[SessionInfo]{
  override def compare(that: SessionInfo): Int =
    if (this.count >= that.count) -1
//    else if (this.count == that.count) 0
    else 1

}


