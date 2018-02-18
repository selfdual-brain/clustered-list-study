package com.selfdualbrain.threaded_cluster

/**
  * In a real cluster environment nodes (= cluster members) are separate computers - they have own memory and processing power.
  * Every code executes on some cluster member, so the code can easily know "on which cluster node I am just executing"
  * just by static configuration (= sharing a global state).
  *
  *
  * In our implementation we simulate the execution semantics of a cluster by having separated groups of JVM-level threads.
  * Every thread keeps the reference to the cluster node that owns it.
  *
  * ClusterContext stands as a syntax sugar for providing centralized access to "cluster" features
  * to objects that "execute on a given cluster node". It summarizes what objects living in a cluster node N know about N.
  * Implementation-wise ClusterContext is just a syntax sugar for calling methods of this ClusterNode instance which
  * is pointed to by the current thread.
  */
object ClusterContext {

  //============================================================= PUBLIC ===============================================================

  /**
    * I return id of the cluster node where we live.
    */
  def localNodeId: ClusterNodeId = localNodeSingleton.nodeId

  /**
    * I execute a request-response communication between cluster nodes.
    *
    * @param targetClusterNode cluster node where the message should be sent to
    * @param msg the message itself
    * @return response message
    */
  def sendClusterMessage_Ask(targetClusterNode: ClusterNodeId, msg: ClusterProtocolRequest): ClusterProtocolResponse = {
    val response = localNodeSingleton.messagingClient.sendMessageAndWaitForResponse(MessageBrokerClient.EndpointAddress("cluster", targetClusterNode), msg)
    return response.asInstanceOf[ClusterProtocolResponse]
  }

  def sendClusterMessage_Tell(targetClusterNode: ClusterNodeId, msg: ClusterProtocolNotification): Unit = {
    localNodeSingleton.sendMessageTell(targetClusterNode, msg)
  }

  def sendClusterMessage_Broadcast(msg: ClusterProtocolNotification): Unit = {
    localNodeSingleton.broadcastMessageWithinClusterMembers(msg)
  }

  def localNodeMemory: Memory = localNodeSingleton.memory

  @throws(classOf[ClusterMemoryFull])
  def allocateNewListInTheCluster(): ListRef = localNodeSingleton.allocateNewListInTheCluster()

  @throws(classOf[ClusterMemoryFull])
  def allocateExtensionOfLocalListOnAnotherClusterNode(ref: ListRef, root: ListRef): ListRef = {
    localNodeSingleton.allocateExtensionOfLocalListOnAnotherClusterNode(ref, root)
  }

  //============================================================= PRIVATE ===============================================================

  /**
    * Returns ClusterNode instance.
    */
  private def localNodeSingleton: ClusterNode[_] = {
    //hopefully this method is called only from a thread belonging to the simulated cluster node
    //if this is not the case, we clearly have a bug
    Thread.currentThread() match {
      case t: ClusterNodeAwareThread[_] => t.clusterNode
      case other => throw new RuntimeException(s"unable to get reference to local cluster node - current thread was $other")
    }
  }

}
