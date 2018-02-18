package com.selfdualbrain

import com.selfdualbrain.BigParallelTest.Operation

class BigParallelTest(listFactory: EmptyListFactory, numberOfIterations: Int, opFreq: Map[Operation, Double]) {


}

object BigParallelTest {

  abstract sealed class Operation {
  }

  object Operation {
    case object StartNewList
    case object GlowExistingList
    case object Split
    case object Map
    case object Reverse
    case object DeleteTree
  }
}
