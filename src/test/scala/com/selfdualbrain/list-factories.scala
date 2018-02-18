package com.selfdualbrain

import com.selfdualbrain.native_list_wrapper.NativeListWrapper
import com.selfdualbrain.threaded_cluster.{ClusterConfig, SimulatedClusterManager}

trait EmptyListFactory {
  def create(): FunctionalList[Nothing]
  def implementationName: String
}

object HaskellStyleLocalListFactory extends EmptyListFactory {
  override def implementationName: String = "haskell-style local list"
  override def create(): FunctionalList[Nothing] = local_haskell_style.LocalImmutableList.empty
}

object OOStyleLocalListFactory extends EmptyListFactory {
  override def implementationName: String = "OO-style local list"
  override def create(): FunctionalList[Nothing] = local_oo_style.LocalImmutableList.empty
}

object NativeListFactory extends EmptyListFactory {
  override def implementationName: String = "native scala immutable list"
  override def create(): FunctionalList[Nothing] = NativeListWrapper(List.empty[Nothing])
}

object ThreadedClusterListFactory extends EmptyListFactory {
  override def implementationName: String = "threaded cluster list"
  val clusterManager = new SimulatedClusterManager

  val config = ClusterConfig(
    initialClusterSize = 5,
    nodeMemoryTableSize = 10000,
    nodeMemoryTableMaxSaturation = 0.8,
    nodeMemoryTableAllocationRefusalMargin = 10,
    nodeWorkerThreadsLimit = 100
  )

  clusterManager.startCluster(config)

  override def create(): FunctionalList[Nothing] = clusterManager.createNewDistributedList
}