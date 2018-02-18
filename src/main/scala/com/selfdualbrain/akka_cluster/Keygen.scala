package com.selfdualbrain.akka_cluster

import java.util.concurrent.atomic.AtomicInteger

trait Keygen {
  def nextSegmentId(): Int
}

object KeygenImpl extends Keygen {
  val counter = new AtomicInteger(0)
  def nextSegmentId(): Int = counter.incrementAndGet()
}
