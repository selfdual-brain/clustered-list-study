package com.selfdualbrain.akka_cluster

import akka.actor.ActorRef

object SegmentProtocol {
  case class HeadRequest(slot: Int)
  case class AppendRequest(element: Object, slot: Int)
  case class StartNewListRequest(element: Object)
  case class CreateTraverserRequest(slot: Int)
  case class MapRequest[A,B](slot: Int, function: A => B)
  case class FilterRequest[A](slot: Int, predicate: A => Boolean)
  case class ReverseRequest(slot: Int)
  case class ContinueList(head: Object, tail: ListHandle, client: ActorRef)
  case class SelectContinuationSegmentResponse(segment: SegmentId)
  case class UpdateContinuationSegmentPointer(segment: SegmentId)

}

object ClientProtocol {
  case class HeadResponse[T](head: T, tail: Option[ListHandle])
  case class AppendResponse(handle: ListHandle)
  case class StartNewListResponse(handle: ListHandle)
  case class CreateTraverserResponse(traverser: ActorRef)
  case class MapResponse(handle: ListHandle)
  case class FilterResponse(handle: ListHandle)
  case class ReverseResponse(handle: ListHandle)
  case class NextItemFromTraverser[T](element: T)
  case object TraverserReachedTheEnd
}

object ClusterMonitor {
  case class SegmentMemoryWarning(internalSegmentId: Int)
  case class SegmentMemoryIsFull(internalSegmentId: Int)
  case class NewSegmentWasCreated(segmentId: SegmentId, internalSegmentId: Int)

//  case class SelectContinuationSegmentRequest(segment: SegmentId, )

}

object TraverserProtocol {
  case class GetNextItemFromTraverser()
  case object Close

}
