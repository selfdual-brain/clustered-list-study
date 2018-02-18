package com.selfdualbrain

import akka.actor.ActorRef

package object akka_cluster {
  type SegmentId = ActorRef
  type Slot = Int
}
