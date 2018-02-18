package com.selfdualbrain.threaded_cluster

import java.util.concurrent.ThreadLocalRandom

import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Central orchestrator (and facade) of the simulated cluster.
  *
  * This class encapsulates our "kitchen of hacking" to set up a cluster of virtual machines within
  * a single JVM. Would be replaced by something completely different in a real cluster.
  *
  * Caution: for obvious reasons this implementation is really minimalistic. For example the size of the cluster
  * is fixed (= it cannot grow/shrink). Cluster machines are initialized once, topology of the cluster is static.
  */
class SimulatedClusterManager {
  private val log = LoggerFactory.getLogger("cluster-manager")

  private val id2node: mutable.Map[Int, ClusterNode[Long]] = new mutable.HashMap[Int, ClusterNode[Long]]
  private var clusterConfig: ClusterConfig = _
  private val messageBroker = new JvmEmbeddedMessageBroker
  private var clusterIsInitialized = false
  private val random = ThreadLocalRandom.current()

  /**
    * Starts simulated (in-memory) cluster.
    * Once created, cluster members are available via getNode().
    *
    * @param config recipe defining properties of the cluster to be created
    */
  def startCluster(config: ClusterConfig): Unit = {
    if (clusterIsInitialized)
      throw new RuntimeException("tried to re-initialize already running cluster")

    log.debug("cluster startup ...")

    clusterConfig = config

    if (config.initialClusterSize < 3)
      throw new RuntimeException(s"requested cluster size = ${config.initialClusterSize}; the implementation of the cluster requires that at least 3 cluster nodes are created")

    val minimalNodeTableSizeImpliedByConfig = (math.max(config.nodeMemoryTableAllocationRefusalMargin, 3).toDouble / config.nodeMemoryTableMaxSaturation).toInt + 1

    if (config.nodeMemoryTableSize < minimalNodeTableSizeImpliedByConfig)
      throw new RuntimeException(s"configured cluster node memory table size is ${config.nodeMemoryTableSize} is inconsistent with other configuration parameters. " +
        s"Should be at least $minimalNodeTableSizeImpliedByConfig")

    for (i <- 0 until config.initialClusterSize) {
      log.debug(s"creating machine $i")
      val rawMessagingClient = new JvmEmbeddedMessageBroker.ClientImpl(messageBroker)
      val clusterCommClient = new ClusterCommunicationClient(rawMessagingClient)
      val node = new ClusterNode(clusterManager = this, clusterCommClient, nodeId = i, config)
      id2node += i -> node
      val nextNodeId = (i+1) % clusterConfig.initialClusterSize
      val nPlus2NodeId = (i+2) % clusterConfig.initialClusterSize
      node.updateRingPointers(nextNodeId, nPlus2NodeId)
    }

    for (node <- id2node.values) {
      log.debug(s"spinning up cluster node ${node.nodeId}")
      node.startup()
    }

    log.debug("cluster startup completed")

    clusterIsInitialized = true
  }

  def shutdownCluster(): Unit = {
    if (! clusterIsInitialized)
      throw new RuntimeException("tried to start shutdown sequence for a cluster that was never initialized")

    log.debug("shutting down cluster ...")

    for ((id, node) <- id2node) {
      log.debug(s"shutting down cluster node $id")
      node.shutdown()
    }

  }

  def getNode(id: Int): ClusterNode[Long] = id2node(id)

  def chooseRandomClusterNode(): ClusterNodeId = random.nextInt(clusterConfig.initialClusterSize)

  def chooseRandomClusterNode(avoiding: ClusterNodeId): ClusterNodeId = {
    val candidate = random.nextInt(clusterConfig.initialClusterSize)
    return if (candidate != avoiding)
      candidate
    else
      (candidate + 1) % clusterConfig.initialClusterSize
  }

  def currentClusterSize: Int = clusterConfig.initialClusterSize

  @throws(classOf[ClusterMemoryFull])
  def createNewDistributedList: DistributedListProxy[Nothing] = {
    log.debug("creating new distributed list proxy")
    val rawMessagingClient = new JvmEmbeddedMessageBroker.ClientImpl(messageBroker)
    val clusterCommClient = new ClusterCommunicationClient(rawMessagingClient)
    val clusterNodeId = this.chooseRandomClusterNode()
    val clusterNodeAddress = MessageBrokerClient.EndpointAddress("cluster", clusterNodeId)
    clusterCommClient.connect("list-proxy")
    val response = clusterCommClient.sendMessageAndWaitForResponse(clusterNodeAddress, ClusterProtocolRequest.AllocateNewEmptyListStart)
    response match {
      case ClusterProtocolResponse.AllocateNewList(listRef) => return new DistributedListProxy[Nothing](listRef, clusterCommClient)
      case ClusterProtocolResponse.ClusterMemoryIsFull =>
        clusterCommClient.disconnect()
        throw new ClusterMemoryFull
      case other =>
        throw new RuntimeException(s"got unexpected response message from cluster during new list allocation: $other")
    }
  }

}
