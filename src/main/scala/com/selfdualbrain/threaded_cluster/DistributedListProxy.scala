package com.selfdualbrain.threaded_cluster

import com.selfdualbrain.FunctionalList

/**
  * Implementation of the list contract that delegates (almost) all operations to a (remote) distributed list living in the cluster.
  *
  * @param ref reference of the target list
  * @param rmiClient rmi client to be used for making remote calls
  * @tparam A type of elements
  */
class DistributedListProxy[+A](val ref: ListRef, rmiClient: ClusterCommunicationClient[_]) extends FunctionalList[A] {

  override def head: A = this.headOption.getOrElse(throw new RuntimeException(s"trying to get head of empty list, ref=$ref"))

  override def tail: DistributedListProxy[A] = this.getHeadAndTail(ref)._2 match {
    case Some(r) => new DistributedListProxy[A](r, rmiClient)
    case None => throw new RuntimeException(s"tried to get tail of empty list, ref=$ref")
  }

  def isEmpty: Boolean = {
    val request = ClusterProtocolRequest.IsEmpty(ref)
    val response = rmiClient.sendMessageAndWaitForResponse(ref.clusterNodeEndpoint, request).asInstanceOf[ClusterProtocolResponse.IsEmpty]
    return response.answer
  }

  override def headOption: Option[A] = this.getHeadAndTail(ref)._1.asInstanceOf[Option[A]]

  override def iterator: Iterator[A] = new Iterator[A] {
    var currentPointer: ListRef = ref
    var bufferedHead: Option[A] = None
    var bufferedTail: Option[ListRef] = None
    var bufferingDone: Boolean = false

    override def hasNext: Boolean = {
      doBuffering()
      return bufferedHead.isDefined
    }

    override def next(): A = {
      doBuffering()
      if (bufferedHead.isDefined) {
        bufferingDone = false
        currentPointer = bufferedTail.get
        return bufferedHead.get
      } else {
        throw new RuntimeException(s"iterator reached the end")
      }
    }

    private def doBuffering(): Unit = {
      if (! bufferingDone) {
        val (h,t) = getHeadAndTail(currentPointer)
        bufferedHead = h.asInstanceOf[Option[A]]
        bufferedTail = t
        bufferingDone = true
      }
    }

  }

  override def foreach[U](f: A => U): Unit = this.iterator.foreach(f)

  override def append[B >: A](e: B): DistributedListProxy[B] = {
    val response = executeCall[ClusterProtocolResponse.Append](ClusterProtocolRequest.Append(ref, e))
    return new DistributedListProxy[B](response.list, rmiClient)
  }

  override def prepend[B >: A](e: B): DistributedListProxy[B] = {
    val response = executeCall[ClusterProtocolResponse.Prepend](ClusterProtocolRequest.Prepend(ref, e))
    return new DistributedListProxy[B](response.list, rmiClient)
  }

  override def map[B](f: A => B): DistributedListProxy[B] = {
    val response = executeCall[ClusterProtocolResponse.Map](ClusterProtocolRequest.Map(ref, f))
    return new DistributedListProxy[B](response.list, rmiClient)
  }

  override def filter[B >: A](f: B => Boolean): DistributedListProxy[A] = {
    val response = executeCall[ClusterProtocolResponse.Filter](ClusterProtocolRequest.Filter(ref, f))
    return new DistributedListProxy[A](response.list, rmiClient)
  }

  override def reverse: DistributedListProxy[A] = {
    val response = executeCall[ClusterProtocolResponse.Reverse](ClusterProtocolRequest.Reverse(ref))
    return new DistributedListProxy[A](response.list, rmiClient)
  }

  override def foldLeft[B](seed: B, op: (B, A) => B): B = {
    val response = executeCall[ClusterProtocolResponse.FoldLeft[B]](ClusterProtocolRequest.FoldLeft(ref, seed, op))
    return response.result
  }

  def deleteTree(): Unit = {
    rmiClient.sendMessageAndWaitForResponse(ref.clusterNodeEndpoint, ClusterProtocolNotification.DeleteTreeStart(ref))
  }

  //=============================== PRIVATE ====================================

  private def getHeadAndTail(ref: ListRef): (Option[Any], Option[ListRef]) = {
    val request = ClusterProtocolRequest.HeadTail(ref)
    val response = rmiClient.sendMessageAndWaitForResponse(ref.clusterNodeEndpoint, request).asInstanceOf[ClusterProtocolResponse.HeadTail[Any]]
    return (response.head, response.tail)
  }

  private def executeCall[R](request: ClusterProtocolRequest): R = rmiClient.sendMessageAndWaitForResponse(ref.clusterNodeEndpoint, request).asInstanceOf[R]

}


