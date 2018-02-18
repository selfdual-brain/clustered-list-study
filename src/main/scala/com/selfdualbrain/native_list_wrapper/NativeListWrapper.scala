package com.selfdualbrain.native_list_wrapper

import com.selfdualbrain.FunctionalList

case class NativeListWrapper[+A](list: List[A]) extends FunctionalList[A] {

  override def append[B >: A](e: B): FunctionalList[B] = NativeListWrapper(list :+ e)

  override def prepend[B >: A](e: B): FunctionalList[B] = NativeListWrapper(e :: list)

  override def head: A = list.head

  override def headOption: Option[A] = list.headOption

  override def tail: FunctionalList[A] = NativeListWrapper(list.tail)

  override def isEmpty: Boolean = list.isEmpty

  override def iterator: Iterator[A] = list.iterator

  override def foreach[U](f: A => U): Unit = list.foreach(f)

  override def map[B](f: A => B): FunctionalList[B] = NativeListWrapper(list.map(f))

  override def filter[B >: A](p: B => Boolean): FunctionalList[A] = NativeListWrapper(list.filter(p))

  override def reverse: FunctionalList[A] = NativeListWrapper(list.reverse)

  override def foldLeft[B](seed: B, op: (B, A) => B): B = list.foldLeft(seed)(op)
}
