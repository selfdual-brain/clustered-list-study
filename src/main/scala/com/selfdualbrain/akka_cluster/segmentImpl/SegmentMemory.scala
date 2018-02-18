package com.selfdualbrain.akka_cluster.segmentImpl

import java.util.concurrent.atomic.AtomicInteger

import com.selfdualbrain.akka_cluster.{ListHandle, SegmentId, Slot}

/**
  * Represents chunk of memory used to store distributed list.
  * This chunk exists on some node in a cluster and is accessed concurrently
  * only by dedicated SegmentActor and its workers.
  *
  * Elements are only added to this segment (= there is no element delete operation or
  * any garbage collection going on here) and concurrent access to this structure
  * actually relies on this principle.
  *
  * @param size maximal number of elements this segment can store.
  */
class SegmentMemory(size: Int, startNewThreshold: Int, extendRemoteThreshold: Int) {
  val memory = new Array[SegmentMemory.Item](size)
  val firstFreeSlot = new AtomicInteger(0)

  /**
    * I create new list by extending one my my lists with one element.
    * Returns None if I am full (= no free space in my memory is left).
    *
    * @param newElement the new element (will be the head of new list)
    * @param slotOfTail pointer to the list I am going to extend (= slot referencing my memory)
    * @return pointer to the created list
    */
  def extendLocalList(newElement: Object, slotOfTail: Slot): Option[Slot] =
    storeInNewSlotIfPossible(0) {
      this.readItem(slotOfTail) match {
        case None => throw new RuntimeException(s"tried to extend local list referencing empty slot $slotOfTail") //means we have a bug in our implementation
        case Some(memoryItem) => SegmentMemory.LocalPointer(newElement, slotOfTail, memoryItem.nextPartition)
      }

    }

  /**
    * I create new list by extending a list from another segment with one element.
    * Returns None if I am full (= no free space in my memory is left).
    *
    * @param newElement the new element (will be the head of new list)
    * @param remoteSegment segment where the list being extended lives
    * @param slotOfTail pointer to the list I am going to extend (= slot referencing memory of the remote segment)
    * @return pointer to the created list
    */
  def extendRemoteList(newElement: Object, remoteSegment: SegmentId, slotOfTail: Slot): Option[Slot] =
    storeInNewSlotIfPossible(extendRemoteThreshold)(SegmentMemory.IntersegmentPointer(newElement, remoteSegment, slotOfTail))

  /**
    * I create new list with one element.
    * Returns None if I am full (= no free space in my memory is left).
    *
    * @param element the element to be encapsulated in to a list
    * @return pointer to the created list (= slot referencing my memory)
    */
  def startNewList(element: Object): Option[Slot] = storeInNewSlotIfPossible(startNewThreshold)(SegmentMemory.Terminator(element))

  /**
    * I read a cell of my memory.
    *
    * @param slot pointer to my memory
    * @return memory item stored at that pointer (or none, if there was nothing stored at this slot)
    */
  def readItem(slot: Slot): Option[SegmentMemory.Item] = Option(memory(slot))

  private def storeInNewSlotIfPossible(expectedFreeSpaceLeft: Int)(blockBuildingNewItem: => SegmentMemory.Item): Option[Slot] = {
    if (firstFreeSlot.get > size - 1 - expectedFreeSpaceLeft)
      None
    else {
      val nextSlot = firstFreeSlot.incrementAndGet()
      if (nextSlot >= size)
        None
      else {
        val newItem: SegmentMemory.Item = blockBuildingNewItem
        memory(nextSlot) = newItem
        Some(nextSlot)
      }
    }
  }

}

object SegmentMemory {
  sealed abstract class Item(val element: Object, val nextPartition: Option[ListHandle])
  case class LocalPointer(override val element: Object, slotOfNextElement: Slot, override val nextPartition: Option[ListHandle]) extends Item(element, nextPartition)
  case class IntersegmentPointer(override val element: Object, targetSegment: SegmentId, targetSlot: Slot) extends Item(element, Some(ListHandle(targetSegment, targetSlot)))
  case class Terminator(override val element: Object) extends Item(element, None)
}
