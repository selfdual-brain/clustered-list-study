package com.selfdualbrain.akka_cluster

import akka.actor.ActorRef
import com.selfdualbrain.FunctionalList

sealed abstract class DistributedUniformSegmentsList[+A] extends FunctionalList[A] {

  override def append[B >: A](e: B): DistributedUniformSegmentsList[B] = ???

  override def prepend[B >: A](e: B): DistributedUniformSegmentsList[B] = ???

  override def head: A = ???

  override def headOption: Option[A] = ???

  override def tail: DistributedUniformSegmentsList[A] = ???

  override def isEmpty: Boolean = ???

  override def iterator: Iterator[A] = ???

  override def map[B](f: A => B): DistributedUniformSegmentsList[B] = ???

  override def filter[B >: A](f: B => Boolean): DistributedUniformSegmentsList[A] = ???

  override def reverse: DistributedUniformSegmentsList[A] = ???

  override def foldLeft[B](seed: B, f: (B, A) => B): B = ???
}

case object EmptyList extends DistributedUniformSegmentsList[Nothing] {
  override def foreach[U](f: Nothing => U): Unit = ???
}

case class NonemptyList[+A](segment: ActorRef, pointer: Int) extends DistributedUniformSegmentsList[A] {
  override def foreach[U](f: A => U): Unit = ???
}

object DistributedUniformSegmentsList {
  val empty: DistributedUniformSegmentsList[Nothing] = EmptyList

//  def apply[A](elements: A*): LocalImmutableList[A] = {
//    var result: LocalImmutableList[A] = LocalImmutableList.empty
//    for (i <- elements.indices.reverse)
//      result = result.prepend(elements(i))
//    return result
//  }

}
