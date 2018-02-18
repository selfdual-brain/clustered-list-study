package com.selfdualbrain

import scala.collection.mutable.ArrayBuffer
import scala.language.higherKinds

/**
  * Abstract contract for list-like collections as was required in the exercise.
  * This API is inspired by classic 'functional' immutable list.
  * The list is composed of "head" (left end element) and the rest of the collection (tail).
  *
  * @tparam A type of elements
  */
trait FunctionalList[+A] {

  //====================== METHODS OF "CLASSIC (IMMUTABLE) LIST ====================

  //Adds element to the right end of this list.
  def append[B >: A](e: B): FunctionalList[B]

  //Adds element to the left end of this list, so actually a new head.
  def prepend[B >: A](e: B): FunctionalList[B]

  //Returns the head if the list is not empty.
  def head: A

  def headOption: Option[A]

  //Returns the tail.
  def tail: FunctionalList[A]

  //Checks whether the list is empty.
  def isEmpty: Boolean

  //Returns iterator of elements - iteration starts from the head.
  def iterator: Iterator[A]

  //Applies a function `f` to all elements of this collection.
  def foreach[U](f: A => U): Unit

  //========================= ADDITIONAL METHODS EXPLICITLY REQUESTED IN THE EXERCISE =====================

  //Builds new list with elements obtained from this list by applying function f to each source element.
  def map[B](f: A => B): FunctionalList[B]

  //Builds new list with elements obtained from this list by leaving only elements for which predicate f returned true.
  //Order of elements in the resulting list follows the original order.
  def filter[B >: A](p: B => Boolean): FunctionalList[A]

  //Builds new list with same elements as this list has, but in reversed order.
  def reverse: FunctionalList[A]

  //Collapses this list to a single value by recursively applying operator f to left-hand side of the list.
  def foldLeft[B](seed: B, op: (B,A) => B): B

  def toSeq: Seq[A] = {
    val buffer = new ArrayBuffer[A]
    for (element <- this)
      buffer += element
    return buffer.toSeq
  }

}
