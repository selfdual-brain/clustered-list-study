package com.selfdualbrain.akka_cluster.segmentImpl

import akka.actor.Actor
import com.selfdualbrain.akka_cluster.ListHandle
import com.selfdualbrain.akka_cluster.TraverserProtocol._

class TraverserActor(list: ListHandle) extends Actor {
  var currentPosition: ListHandle = list

  override def receive: Receive = {

    case GetNextItemFromTraverser =>
      currentPosition.segment


    case Close =>
      //todo
  }

}
