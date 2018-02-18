package com.selfdualbrain.akka_cluster.segmentImpl

import akka.actor.{Actor, ActorRef, Props}
import com.selfdualbrain.akka_cluster.SegmentProtocol._
import com.selfdualbrain.akka_cluster._

class SegmentActor(
                    internalSegmentId: Int,
                    memorySize: Int,
                    startNewListSpaceReserveThreshold: Int,
                    extendRemoteListSpaceReserveThreshold: Int,
                    keygen: Keygen,
                    clusterMonitor: ActorRef
                  ) extends Actor {

  val memory = new SegmentMemory(memorySize, startNewListSpaceReserveThreshold, extendRemoteListSpaceReserveThreshold)
  var currentContinuationSegment: Option[SegmentId] = None

  clusterMonitor ! ClusterMonitor.NewSegmentWasCreated(self, internalSegmentId)

  override def receive: Receive = {

    case HeadRequest(slot) =>
      memory.readItem(slot) match {
        case None => //evidence of a bug in our implementation so we blow the cluster
          throw new RuntimeException(s"Accessing empty slot $slot in segment $self")
        case Some(SegmentMemory.LocalPointer(element, slotOfNextElement, nextPartition)) =>
          sender ! ClientProtocol.HeadResponse(element, Some(ListHandle(self, slotOfNextElement)))
        case Some(SegmentMemory.IntersegmentPointer(element, targetSegment, targetSlot)) =>
          sender ! ClientProtocol.HeadResponse(element, Some(ListHandle(targetSegment, targetSlot)))
        case Some(SegmentMemory.Terminator(element)) =>
          sender ! ClientProtocol.HeadResponse(element, None)
      }

    case AppendRequest(element, slot) =>
      memory.extendLocalList(element, slot) match {
        case Some(slotOfCreatedList) => sender ! ClientProtocol.AppendResponse(ListHandle(self, slotOfCreatedList))
        case None => //my memory is full
          clusterMonitor ! ClusterMonitor.SegmentMemoryIsFull(internalSegmentId)
          this.findContinuationSegment() ! SegmentProtocol.ContinueList(element, ListHandle(self, slot), sender)
      }

    case msg@ContinueList(head, tailHandle, client) =>
      memory.extendRemoteList(head, tailHandle.segment, tailHandle.slot) match {
        case Some(slot) => client ! ClientProtocol.AppendResponse(ListHandle(self, slot))
        case None =>
          //because it looks like my memory is now full, I forward list continuation request to another segment
          //but I also notify sender of this message to avoid considering me as a "good candidate for continuation" anymore
          this.findContinuationSegment() ! msg
          clusterMonitor ! ClusterMonitor.SegmentMemoryIsFull(internalSegmentId)
          sender ! SegmentProtocol.UpdateContinuationSegmentPointer(currentContinuationSegment.get)
      }

    case msg@StartNewListRequest(element) =>
      memory.startNewList(element) match {
        case Some(slot) => sender ! ClientProtocol.StartNewListResponse(ListHandle(self, slot))
        case None =>
          //The framework is generally supposed to perform a "smart" selection of a candidate segment.
          //When a new list is created, occasionally the segment may become full between the moment of being selected as a candidate
          //and the moment of "start new list" being actually delivered.
          //This is kinda "bad luck" but in fact rather harmless and it should be (asymptotically) not important for performance.
          //We handle this situation by "take it easy" approach - just by forwarding the request to a "better" segment
          //(and we hope that the cluster will eventually figure out that this segment is not a good candidate for sending
          //"start new list" requests)
          this.findContinuationSegment() ! msg
          clusterMonitor ! ClusterMonitor.SegmentMemoryWarning(internalSegmentId)
      }

    case CreateTraverserRequest(slot) =>
      //todo

    case MapRequest(slot, function) =>
      //todo

    case FilterRequest(slot, predicate) =>
      //todo

    case ReverseRequest(slot) =>
      //todo

    case UpdateContinuationSegmentPointer(segment) =>
      currentContinuationSegment = Some(segment)
  }

  private def findContinuationSegment(): SegmentId = currentContinuationSegment match {
      case Some(segment) => segment
      case None =>
        val newSegment = context.actorOf(SegmentActor.props(keygen.nextSegmentId(), memorySize, keygen, clusterMonitor))
        currentContinuationSegment = Some(newSegment)
        newSegment
    }

}

object SegmentActor {
  def props(internalSegmentId: Int, memorySize: Int, keygen: Keygen, clusterMonitor: ActorRef) = ???
//  Props(new SegmentActor(internalSegmentId, memorySize, keygen, clusterMonitor))
}
