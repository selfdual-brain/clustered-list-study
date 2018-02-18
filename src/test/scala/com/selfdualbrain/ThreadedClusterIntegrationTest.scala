package com.selfdualbrain

import com.selfdualbrain.threaded_cluster.{ClusterConfig, DistributedListProxy, SimulatedClusterManager}
import org.slf4j.LoggerFactory

object ThreadedClusterIntegrationTest {
  private val log = LoggerFactory.getLogger(this.getClass)

  val clusterManager = new SimulatedClusterManager

  val config = ClusterConfig(
    initialClusterSize = 5,
    nodeMemoryTableSize = 1000,
    nodeMemoryTableMaxSaturation = 0.8,
    nodeMemoryTableAllocationRefusalMargin = 10,
    nodeWorkerThreadsLimit = 100
  )

  def main(args: Array[String]): Unit = {
    clusterManager.startCluster(config)
    var list: DistributedListProxy[Int] = clusterManager.createNewDistributedList
    list = list.prepend(2)
    list = list.prepend(3)
    val originalList = list
    val mappedList = list.map(n => n * n)

    println(s"source list: ${originalList.toSeq.toList}")
    println(s"mapped list: ${mappedList.toSeq.toList}")

    log.debug("sleeping main for 3 seconds ...")
    Thread.sleep(3000)
    clusterManager.shutdownCluster()
  }

}
