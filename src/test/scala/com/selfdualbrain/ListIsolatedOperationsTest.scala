package com.selfdualbrain

import org.scalatest.Matchers._
import org.scalatest.{MustMatchers, WordSpec}

class ListIsolatedOperationsTest extends WordSpec with MustMatchers {

  val availableImplementations = Seq(HaskellStyleLocalListFactory, OOStyleLocalListFactory, NativeListFactory, ThreadedClusterListFactory)

  for (factory <- availableImplementations) {
    factory.implementationName should {

      "be initially empty" in {
        val list = factory.create()
        list.isEmpty mustBe true
      }

      "correctly handle append()" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(1)
        list = list.prepend(2)
        list = list.append(3)
        list.toSeq should contain theSameElementsInOrderAs List(2, 1, 3)
      }

      "allow duplicate elements" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(1)
        list = list.prepend(2)
        list = list.prepend(2)
        list = list.prepend(3)
        list.toSeq should contain theSameElementsInOrderAs List(3,2,2,1)
      }

      "allow diversity of elements" in {
        var list: FunctionalList[Any] = factory.create()
        val ob = new Object
        val f = (n:Int) => n+1
        val array = Array(10,11,12)
        list = list.prepend(42)
        list = list.prepend(true)
        list = list.prepend("foo")
        list = list.prepend(math.Pi)
        list = list.prepend(array)
        list = list.prepend('x')
        list = list.prepend(ob)
        list = list.prepend(f)
        list.toSeq should contain theSameElementsInOrderAs List(f,ob,'x',array,math.Pi,"foo",true,42)
      }

      "correctly handle map()" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(3)
        list = list.prepend(2)
        list = list.prepend(1)
        list.map(n => n*n).toSeq should contain theSameElementsInOrderAs List(1,4,9)
      }

      "correctly handle filter()" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(3)
        list = list.prepend(2)
        list = list.prepend(1)
        list.filter(n => n % 2 != 0).toSeq should contain theSameElementsInOrderAs List(1,3)
      }

      "correctly handle foldLeft()" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(3)
        list = list.prepend(2)
        list = list.prepend(1)
        list.foldLeft(0, (a: Int,b: Int) => a + b) mustBe 6
      }

      "correctly handle reverse()" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(3)
        list = list.prepend(2)
        list = list.prepend(1)
        list.reverse.toSeq should contain theSameElementsInOrderAs List(3,2,1)
      }

      "behave immutably" in {
        var list: FunctionalList[Int] = factory.create()
        list = list.prepend(3)
        list = list.prepend(2)
        list = list.prepend(1)
        val snapshot1 = list
        list = list.reverse
        val snapshot2 = list
        list = list.filter(n => n % 2 != 0)
        val snapshot3 = list
        list = list.append(0)
        snapshot1.toSeq should contain theSameElementsInOrderAs List(1,2,3)
        snapshot2.toSeq should contain theSameElementsInOrderAs List(3,2,1)
        snapshot3.toSeq should contain theSameElementsInOrderAs List(3,1)
        list.toSeq should contain theSameElementsInOrderAs List(3,1,0)
      }

    }

  }

}
