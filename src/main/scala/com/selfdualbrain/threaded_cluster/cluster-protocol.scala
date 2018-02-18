package com.selfdualbrain.threaded_cluster

sealed abstract class ClusterProtocolMessage

sealed abstract class ClusterProtocolRequest extends ClusterProtocolMessage {
}

object ClusterProtocolRequest {
  case class Prepend[A](list: ListRef, element: A) extends ClusterProtocolRequest
  case class HeadTail(list: ListRef) extends ClusterProtocolRequest
  case class IsEmpty(list: ListRef) extends ClusterProtocolRequest
  case object AllocateNewEmptyListStart extends ClusterProtocolRequest
  case class AllocateListExtensionStart(list: ListRef, root: ListRef) extends ClusterProtocolRequest
  case class Map[A,B](list: ListRef, f: A => B) extends ClusterProtocolRequest
  case class Filter[A](list: ListRef, p: A => Boolean) extends ClusterProtocolRequest
  case class Append[A](list: ListRef, element: A) extends ClusterProtocolRequest
  case class FoldLeft[A,B](list: ListRef, seed: B, op: (B, A) => B) extends ClusterProtocolRequest
  case class Reverse(list: ListRef) extends ClusterProtocolRequest
}

sealed abstract class ClusterProtocolResponse extends ClusterProtocolMessage {
}

object ClusterProtocolResponse {
  case class Prepend(list: ListRef) extends ClusterProtocolResponse
  case class HeadTail[A](head: Option[A], tail: Option[ListRef]) extends ClusterProtocolResponse
  case class IsEmpty(answer: Boolean) extends ClusterProtocolResponse
  case class AllocateNewList(list: ListRef) extends ClusterProtocolResponse
  case class AllocateListExtension(list: ListRef) extends ClusterProtocolResponse
  case object ClusterMemoryIsFull extends ClusterProtocolResponse
  case class Map(list: ListRef) extends ClusterProtocolResponse
  case class Filter(list: ListRef) extends ClusterProtocolResponse
  case class Append(list: ListRef) extends ClusterProtocolResponse
  case class FoldLeft[A](result: A) extends ClusterProtocolResponse
  case class Reverse(list: ListRef) extends ClusterProtocolResponse
}

sealed abstract class ClusterProtocolNotification extends ClusterProtocolMessage

object ClusterProtocolNotification {
  case class DeleteTreeStart(rootRef: ListRef) extends ClusterProtocolNotification
  case class DeleteTreeContinue(rootRef: ListRef) extends ClusterProtocolNotification

  case class AllocateNewEmptyListContinue[MID](
                                           waitingClient: MessageBrokerClient.EndpointAddress,
                                           initialRequestId: MID,
                                           jumpsLeft: Int) extends ClusterProtocolNotification

  case class AllocateListExtensionContinue[MID](
                                            waitingClient: MessageBrokerClient.EndpointAddress,
                                            initialRequestId: MID,
                                            jumpsLeft: Int,
                                            listToBeExtended: ListRef,
                                            root: ListRef) extends ClusterProtocolNotification
}




