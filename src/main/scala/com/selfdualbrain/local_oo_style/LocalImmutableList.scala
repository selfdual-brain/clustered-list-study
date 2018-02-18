package com.selfdualbrain.local_oo_style

import com.selfdualbrain.FunctionalList

/**
  * Straightforward implementation of FunctionalList that roughly resembles classic implementation of functional lists.
  * Of course we aim for simplicity so the performance could be optimized here and there.
  *
  * @tparam A type of elements
  */
abstract class LocalImmutableList[+A] extends FunctionalList[A] {

  def append[B >: A](e: B): LocalImmutableList[B] = {
    var result = LocalImmutableList(e)
    for (element <- this.reverse)
      result = result.prepend(element)
    return result
  }

  def prepend[B >: A](e: B): LocalImmutableList[B] = NonemptyList(head = e, tail = this)

  override def tail: LocalImmutableList[A]

  def iterator: Iterator[A] = new Iterator[A] {
    var currentNode: LocalImmutableList[A] = LocalImmutableList.this

    override def hasNext: Boolean = currentNode match {
      case EmptyList => false
      case _ => true
    }

    override def next(): A = {
      val result = currentNode.headOption match {
        case Some(element) => element
        case None => throw new RuntimeException("Iterator reached the end")
      }
      currentNode = currentNode.tail
      result
    }
  }

  override def foreach[U](f: A => U): Unit = this.iterator.foreach(f)

  def map[B](f: A => B): LocalImmutableList[B]

  def filter[B >: A](f: B => Boolean): LocalImmutableList[A]

  //we optimize reverse() to be tail-recursive (by using the classic 'accumulator' trick)
  def reverse: LocalImmutableList[A] = this.privateReverse(EmptyList)

  def privateReverse[B >: A](accumulator: LocalImmutableList[B]): LocalImmutableList[B]

}

case object EmptyList extends LocalImmutableList[Nothing] {

  override def isEmpty = true

  override def head: Nothing = throw new RuntimeException("tried to access head of empty list")

  override def tail: LocalImmutableList[Nothing] = throw new RuntimeException("tried to access tail of empty list")

  override def headOption: Option[Nothing] = None

  override def map[B](f: Nothing => B): LocalImmutableList[Nothing] = EmptyList

  override def filter[B >: Nothing](f: B => Boolean): LocalImmutableList[Nothing] = EmptyList

  override def foldLeft[B](seed: B, f: (B, Nothing) => B): B = seed

  override def privateReverse[B >: Nothing](accumulator: LocalImmutableList[B]): LocalImmutableList[B] = accumulator
}

case class NonemptyList[+A](head: A, override val tail: LocalImmutableList[A]) extends LocalImmutableList[A] {

  override def isEmpty = false

  override def headOption: Option[A] = Some(head)

  override def map[B](f: A => B): LocalImmutableList[B] = NonemptyList(f(head), tail.map(f))

  override def filter[B >: A](f: B => Boolean): LocalImmutableList[A] =
    if (f(head))
      NonemptyList(head, tail.filter(f))
    else
      tail.filter(f)

  override def foldLeft[B](seed: B, f: (B, A) => B): B = tail.foldLeft(f(seed,head), f)

  override def privateReverse[B >: A](accumulator: LocalImmutableList[B]): LocalImmutableList[B] = tail.privateReverse(NonemptyList(head, accumulator))
}

object LocalImmutableList {
  val empty: LocalImmutableList[Nothing] = EmptyList

  def apply[A](elements: A*): LocalImmutableList[A] = {
    var result: LocalImmutableList[A] = LocalImmutableList.empty
    for (i <- elements.indices.reverse)
      result = result.prepend(elements(i))
    return result
  }

}
