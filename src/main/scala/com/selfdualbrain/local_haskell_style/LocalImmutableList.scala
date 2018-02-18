package com.selfdualbrain.local_haskell_style

import com.selfdualbrain.FunctionalList

/**
  * Straightforward implementation of FunctionalList that roughly resembles classic implementation of functional lists.
  * Of course we aim for simplicity so the performance could be optimized here and there.
  *
  * @tparam A type of elements
  */
sealed abstract class LocalImmutableList[+A] extends FunctionalList[A] {

  //could be faster if converted to tail-recursive implementation as we did for reverse()
  def append[B >: A](e: B): LocalImmutableList[B]

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

  //reversing is optimized to be tail-recursive
  def reverse: LocalImmutableList[A] = this.privateReverse(EmptyList)

  def foldLeft[B](seed: B, f: (B,A) => B): B =
    this match {
      case EmptyList => seed
      case NonemptyList(h,t) => t.foldLeft(f(seed, h), f)
    }

  private def privateReverse[B >: A](accumulator: LocalImmutableList[B]): LocalImmutableList[B] =
    this match {
      case EmptyList => accumulator
      case NonemptyList(h,t) => t.privateReverse(NonemptyList(h, accumulator))
    }

}

case object EmptyList extends LocalImmutableList[Nothing] {
  override def isEmpty = true

  override def head: Nothing = {
    throw new RuntimeException("tried to access head of empty list")
  }

  override def tail: LocalImmutableList[Nothing] = {
    throw new RuntimeException("tried to access tail of empty list")
  }

  override def headOption: Option[Nothing] = None

  override def append[B >: Nothing](e: B): LocalImmutableList[B] = NonemptyList(e, this)

  override def map[B](f: Nothing => B): LocalImmutableList[Nothing] = EmptyList

  override def filter[B >: Nothing](f: B => Boolean): LocalImmutableList[Nothing] = EmptyList

}

case class NonemptyList[+A](head: A, override val tail: LocalImmutableList[A]) extends LocalImmutableList[A] {
  override def isEmpty = false

  override def headOption: Option[A] = Some(head)

  override def append[B >: A](e: B): LocalImmutableList[B] = NonemptyList(head, tail.append(e))

  override def map[B](f: A => B): LocalImmutableList[B] = NonemptyList(f(head), tail.map(f))

  override def filter[B >: A](f: B => Boolean): LocalImmutableList[A] =
    if (f(head))
      NonemptyList(head, tail.filter(f))
    else
      tail.filter(f)

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
