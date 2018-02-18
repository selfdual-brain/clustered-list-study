package com.selfdualbrain.threaded_cluster

import com.selfdualbrain.FunctionalList
import com.selfdualbrain.threaded_cluster.Memory.{Jumper, Nub, Terminator}

/**
  * Implementation of functional list to be used on cluster members.
  * Distributed list can span over several cluster nodes.
  *
  * Caution: not to be mistaken with DistributedListProxy, which is dedicated to be used outside the cluster.
  *
  * @param ref cluster-wide reference to the list.
  * @tparam A type of elements
  */
abstract class DistributedList[+A](val ref: ListRef) extends FunctionalList[A] {
  import DistributedList.ListRefOps

  @throws(classOf[ClusterMemoryFull])
  override def append[B >: A](e: B): DistributedList[B] = {
    var result = DistributedList(e)
    for (element <- this.reverse)
      result = result.prepend(element)
    return result
  }

  override def head: A = this.headOption.getOrElse(throw new RuntimeException(s"trying to get head of empty list, ref=$ref"))

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
        if (currentPointer.clusterNode == ClusterContext.localNodeId) {
          ClusterContext.localNodeMemory.readItemAtAddress(currentPointer.slot) match {
            case Nub(e, next, root) =>
              bufferedHead = Some(e.asInstanceOf[A])
              bufferedTail = Some(ListRef(ref.clusterNode, next, root.magic))
            case Jumper(next, root) =>
              currentPointer = next
              this.doBuffering()
            case Terminator(magic) =>
              bufferedHead = None
              bufferedTail = None
          }
        } else {
          val (h,t) = currentPointer.getHeadAndTail
          bufferedHead = h.asInstanceOf[Option[A]]
          bufferedTail = t
        }
        bufferingDone = true
      }
    }

  }

  override def foreach[U](f: A => U): Unit = this.iterator.foreach(f)

  @throws(classOf[ClusterMemoryFull])
  override def reverse: DistributedList[A] = {
    var accumulator: ListRef = ClusterContext.allocateNewListInTheCluster()
    for (element <- this)
      accumulator = accumulator.prepend(element)
    return DistributedList[A](accumulator)
  }

  @throws(classOf[ClusterMemoryFull])
  override def map[B](f: A => B): DistributedList[B] = {
    var tmp: ListRef = ClusterContext.allocateNewListInTheCluster()
    for (element <- this)
      tmp = tmp.prepend(f(element))
    val temporaryList: DistributedList[B] = DistributedList[B](tmp)
    val result: DistributedList[B] = temporaryList.reverse
    tmp.deleteTree()
    return result
  }

  @throws(classOf[ClusterMemoryFull])
  override def filter[B >: A](p: B => Boolean): DistributedList[A] = {
    var tmp: ListRef = ClusterContext.allocateNewListInTheCluster()
    for (element <- this)
      if (p(element))
        tmp = tmp.prepend(element)
    val temporaryList: DistributedList[A] = DistributedList[A](tmp)
    val result: DistributedList[A] = temporaryList.reverse
    tmp.deleteTree()
    return result
  }

  override def foldLeft[B](seed: B, f: (B, A) => B): B = {
    var accumulator: B = seed
    for (element <- this)
      accumulator = f(accumulator, element)
    return accumulator
  }

  protected def getRootRefFor(slot: Slot): ListRef = ClusterContext.localNodeMemory.getRootFor(slot)

  override def prepend[B >: A](e: B): DistributedList[B]

  override def tail: DistributedList[A]
}

/**
  * The case when the list reference points to the local cluster node.
  */
class LocalDistributedList[+A](ref: ListRef) extends DistributedList[A](ref) {
  import DistributedList.ListRefOps

  @throws(classOf[ClusterMemoryFull])
  override def prepend[B >: A](e: B): DistributedList[B] = {
    ClusterContext.localNodeMemory.extendLocalList(e, ref.slot) match {
      case Some(ref) =>
        //happy path - we managed to extend the list within the local cluster node
        new LocalDistributedList[B](ref)
      case None =>
        //local node memory is full - we need to extend the list using another cluster node
        val root: ListRef = this.getRootRefFor(ref.slot)
        val listExtendedWithJumper: ListRef = ClusterContext.allocateExtensionOfLocalListOnAnotherClusterNode(ref, root)
        val resultRef = listExtendedWithJumper.prepend(e)
        DistributedList[B](resultRef)
    }
  }

  override def headOption: Option[A] = ClusterContext.localNodeMemory.readItemAtAddress(ref.slot) match {
    case Nub(e, next, root) => Some(e.asInstanceOf[A])
    case Jumper(next, root) => next.getHeadAndTail._1.asInstanceOf[Option[A]]
    case Terminator(magic) => None
  }


  override def tail: DistributedList[A] = ClusterContext.localNodeMemory.readItemAtAddress(ref.slot) match {
    case Nub(e, next, root) => new LocalDistributedList[A](ListRef(ref.clusterNode, next, root.magic))
    case Jumper(next, root) => new RemoteDistributedList[A](next)
    case Terminator(magic) => throw new RuntimeException(s"tried to get tail of empty list, ref=$ref")
  }

  override def isEmpty: Boolean = ClusterContext.localNodeMemory.readItemAtAddress(ref.slot) match {
    case x: Memory.Nub => false
    case Memory.Jumper(nextItem, root) => if (nextItem == root) true else ref.isEmpty
    case x: Memory.Terminator => true
  }
}

/**
  * The case when the list reference does not point to the local cluster node.
  */
class RemoteDistributedList[+A](ref: ListRef) extends DistributedList[A](ref) {
  import DistributedList.ListRefOps

  @throws(classOf[ClusterMemoryFull])
  override def prepend[B >: A](e: B): DistributedList[B] = {
    val resultRef = ref.prepend(e)
    return DistributedList[B](resultRef)
  }

  override def headOption: Option[A] = ref.getHeadAndTail._1.asInstanceOf[Option[A]]

  override def tail: DistributedList[A] = ref.getHeadAndTail._2 match {
    case Some(r) => DistributedList[A](r)
    case None => throw new RuntimeException(s"tried to get tail of empty list, ref=$ref")
  }

  override def isEmpty: Boolean = ref.isEmpty
}

object DistributedList {

  @throws(classOf[ClusterMemoryFull])
  def empty: DistributedList[Nothing] = DistributedList(ClusterContext.allocateNewListInTheCluster())

  def apply[A](ref: ListRef): DistributedList[A] =
    if (ref.clusterNode == ClusterContext.localNodeId)
      new LocalDistributedList[A](ref)
    else
      new RemoteDistributedList[A](ref)

  def apply[A](elements: A*): DistributedList[A] = {
    var result: DistributedList[A] = DistributedList.empty
    for (i <- elements.indices.reverse)
      result = result.prepend(elements(i))
    return result
  }

  /**
    * Extends ListRef case class with some methods that work in the context of a cluster.
    */
  implicit class ListRefOps(ref: ListRef) {

    def isEmpty: Boolean = {
      val request = ClusterProtocolRequest.IsEmpty(ref)
      val response = ClusterContext.sendClusterMessage_Ask(ref.clusterNode, request).asInstanceOf[ClusterProtocolResponse.IsEmpty]
      return response.answer
    }

    def prepend(element: Any): ListRef = {
      val request = ClusterProtocolRequest.Prepend(ref, element)
      val response = ClusterContext.sendClusterMessage_Ask(ref.clusterNode, request).asInstanceOf[ClusterProtocolResponse.Prepend]
      return response.list
    }

    def getHeadAndTail: (Option[Any], Option[ListRef]) = {
      val request = ClusterProtocolRequest.HeadTail(ref)
      val response = ClusterContext.sendClusterMessage_Ask(ref.clusterNode, request).asInstanceOf[ClusterProtocolResponse.HeadTail[Any]]
      return (response.head, response.tail)
    }

    def deleteTree(): Unit = {
      ClusterContext.sendClusterMessage_Tell(ref.clusterNode, ClusterProtocolNotification.DeleteTreeStart(ref))
    }

  }

}
