package com.selfdualbrain.threaded_cluster

import java.util.concurrent._

import com.selfdualbrain.threaded_cluster.ClusterProtocolRequest.{AllocateListExtensionStart, AllocateNewEmptyListStart}
import org.slf4j.LoggerFactory

/**
  * Implementation of (simulated) cluster node. You can think that a cluster node is "a computer in a cluster".
  *
  * We simulate the following:
  *   1. Each cluster node having separate memory (= isolated from other cluster nodes)
  *   2. Objects have cluster-wide references.
  *   3. Cluster nodes communicate ONLY by exchanging messages.
  *   4. Each cluster node has has own computing power (think "multi-core processor"), so cluster nodes run concurrently.
  *
  * The cluster mode is simulated as a collection of threads.
  *
  * @param clusterManager reference to cluster manager singleton (possible because this is in-memory cluster simulation !)
  * @param messagingClient abstraction of network layer used by the cluster
  * @param nodeId numeric id of this node (assigned by cluster manager)
  * @param config cluster configuration params
  */
class ClusterNode[MessageId](clusterManager: SimulatedClusterManager, val messagingClient: ClusterCommunicationClient[MessageId], val nodeId: ClusterNodeId, config: ClusterConfig) {
  private val log = LoggerFactory.getLogger(s"cluster-node-$nodeId")

  import MessageBrokerClient._

  //custom implementation of memory; this is where we keep lists
  val memory: Memory = new Memory(nodeId, config.nodeMemoryTableSize, 0.75)

  //thread pool used to serve requests
  private val executor = Executors.newCachedThreadPool(new RequestHandlersThreadFactory)

  //pointers to n+1 and n+2 cluster nodes in the ring; kept as a tuple because changing of this pair must be atomic
  @volatile var nextNodesInRing: Option[(ClusterNodeId, ClusterNodeId)] = None

  //flag signalling that shutdown sequence of this cluster node was already initiated
  @volatile private var shutdownInProgress: Boolean = false

  //Factory of threads used by the executor. These threads are special because
  //they are cluster-aware (each thread knows the cluster node that owns this thread)
  class RequestHandlersThreadFactory extends ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val result = new ClusterNodeAwareThread(ClusterNode.this, r)
      result.setPriority(Thread.NORM_PRIORITY) //higher priority than incoming message handling loops
      return result
    }
  }

  //A single thread that handles the whole incoming messages traffic.
  //Its only job is to dispatch messages to a proper handling thread:
  //   - for requests we spawn a new handling thread per-requests
  //   - responses are passed to the rmi client
  private val incomingMessagesLoopThread = new Thread {
    this.setPriority(Thread.NORM_PRIORITY - 2) //priority lower than grabbing responses we wait for
    override def run(): Unit = {
      while (! shutdownInProgress) {
        val incomingMessage: MsgEnvelope[MessageId]  = messagingClient.waitUntilNextIncomingMessageArrives()
        incomingMessage.payload match {
          case m: ClusterProtocolRequest => executor.submit(new RequestHandlingTask(incomingMessage))
          case m: ClusterProtocolNotification => executor.submit(new RequestHandlingTask(incomingMessage))
          case other => throw new RuntimeException(s"unexpected message payload: $incomingMessage")
        }
      }
    }
  }

  //This is a task (= instance of Runnable) that each request handling thread executes
  //once woke up from the "executor" thread pool.
  class RequestHandlingTask(msg: MsgEnvelope[MessageId]) extends Runnable {
    override def run(): Unit = {
      msg.payload match {
        case request: ClusterProtocolRequest =>
          log.debug(s"handling request $msg")
          val processingResult: Option[ClusterProtocolResponse] = processIncomingRequest(msg.sender, msg.msgId,  request)
          if (processingResult.isDefined)
            messagingClient.sendMessage(processingResult.get, Some(msg.msgId), msg.sender)

        case notification: ClusterProtocolNotification =>
          log.debug(s"handling notification $msg")
          processIncomingNotification(msg.sender, msg.msgId, notification)

        case other => throw new RuntimeException(s"unexpected message type received: $other")
      }
    }
  }

  //=========================================== PUBLIC API ==========================================

  /**
    * Boot this node.
    * In practice it means just "begin serving incoming messages".
    */
  def startup(): Unit = {
    incomingMessagesLoopThread.start()
    messagingClient.connect(MessageBrokerClient.EndpointAddress("cluster", nodeId))
    log.debug(s"startup completed")
  }

  /**
    * Initiate shutdown sequence of this node.
    * Stops serving incoming messages.
    *
    * Caution: after a shutdown, this cluster node cannot be restarted.
    */
  def shutdown(): Unit = {
    shutdownInProgress = true
    executor.shutdown()
    messagingClient.disconnect()
  }

  def updateRingPointers(next: ClusterNodeId, nPlus2: ClusterNodeId): Unit = {
    nextNodesInRing = Some((next, nPlus2))
  }

  //=========================================== INTERNAL USE ==========================================


  /**
    * Executes cluster node --> cluster node communication.
    * This is a one-way (= asynchronous) communication pattern we send the message but there is no expected response.
    * The sending thread will continue to execute not waiting for the message to be delivered.
    *
    * @param targetClusterNode to which cluster node the message should be delivered
    * @param message the message itself
    */
  def sendMessageTell(targetClusterNode: ClusterNodeId, message: ClusterProtocolNotification): MessageId = {
    messagingClient.sendMessage(message, None, MessageBrokerClient.EndpointAddress("cluster", targetClusterNode))
  }

  def broadcastMessageWithinClusterMembers(message: ClusterProtocolNotification): Unit = {
    messagingClient.broadcastMessage(message, "cluster")
  }

  def allocateNewListInTheCluster(): ListRef = {
    //our first attempt is to create new list on the local node (= on the same node where the caller was running)
    memory.startNewList() match {
      case Some(ref) => return ref
      case None =>
        //only if local node allocation failed, we try initiate cluster-wide search for a best place where the new list can be started
        val nodeWhereWeStartSearching = clusterManager.chooseRandomClusterNode(avoiding = nodeId)
        val addressOfNodeWhereWeStartSearching = MessageBrokerClient.EndpointAddress("cluster", nodeWhereWeStartSearching)
        messagingClient.sendMessageAndWaitForResponse(addressOfNodeWhereWeStartSearching , AllocateNewEmptyListStart) match {
          case ClusterProtocolResponse.AllocateNewList(newListRef) => return newListRef
          case ClusterProtocolResponse.ClusterMemoryIsFull => throw new ClusterMemoryFull
        }
    }
  }

  def allocateExtensionOfLocalListOnAnotherClusterNode(ref: ListRef, root: ListRef): ListRef = {
    val nodeWhereWeStartSearching = clusterManager.chooseRandomClusterNode(avoiding = nodeId)
    val addressOfNodeWhereWeStartSearching = MessageBrokerClient.EndpointAddress("cluster", nodeWhereWeStartSearching)
    messagingClient.sendMessageAndWaitForResponse(addressOfNodeWhereWeStartSearching, AllocateListExtensionStart(ref, root)) match {
      case ClusterProtocolResponse.AllocateListExtension(newListRef) => return newListRef
      case ClusterProtocolResponse.ClusterMemoryIsFull => throw new ClusterMemoryFull
    }
  }

  //=================================== PRIVATE HELPER METHODS ===============================================

  private def nextClusterNode: ClusterNodeId = {
    if (nextNodesInRing.isEmpty)
      throw new RuntimeException(s"cluster ring pointers not initialized for cluster node $nodeId")
    else
      nextNodesInRing.get._1
  }


  private def nPlus2ClusterNode: ClusterNodeId = {
    if (nextNodesInRing.isEmpty)
      throw new RuntimeException(s"cluster ring pointers not initialized for cluster node $nodeId")
    else
      nextNodesInRing.get._2
  }

  private def nextClusterNodeInTheRingAvoiding(nodeToAvoid: ClusterNodeId): ClusterNodeId = {
    if (nextNodesInRing.isEmpty)
      throw new RuntimeException(s"cluster ring pointers not initialized for cluster node $nodeId")

    val (nodeNPlus1, nodeNPlus2) = nextNodesInRing.get //making local thread copy here (just to be thread safe)
    if (nodeNPlus1 != nodeToAvoid)
      nodeNPlus1
    else
      nodeNPlus2
  }

  private def addressOfClusterNode(id: ClusterNodeId): MessageBrokerClient.EndpointAddress = MessageBrokerClient.EndpointAddress("cluster", id)


  /**
    * Handlers of RMI requests.
    *
    * @param request requests message (that we got from another cluster node)
    * @return response message (to be sent back) or None if we postpone sending of response (most likely the response will be sent later, possibly by another cluster node)
    */
  private def processIncomingRequest(sender: MessageBrokerClient.EndpointAddress, msgId: MessageId, request: ClusterProtocolRequest): Option[ClusterProtocolResponse] = {
    import ClusterProtocolRequest._

    request match {
      case Prepend(listRef, element) =>
        val targetObject: DistributedList[Any] = DistributedList[Any](listRef)
        val newList: DistributedList[Any] = targetObject.prepend(element)
        Some(ClusterProtocolResponse.Prepend(newList.ref))

      case HeadTail(listRef) =>
        val targetObject: DistributedList[Any] = DistributedList[Any](listRef)
        val (h,t): (Option[Any], Option[ListRef]) =
          if (targetObject.isEmpty)
            (None, None)
          else
            (Some(targetObject.head), Some(targetObject.tail.ref))
        Some(ClusterProtocolResponse.HeadTail(h,t))

      case IsEmpty(listRef) =>
        val targetObject: DistributedList[Any] = DistributedList[Any](listRef)
        Some(ClusterProtocolResponse.IsEmpty(targetObject.isEmpty))

      case AllocateNewEmptyListStart =>
        memory.startNewList() match {
          case Some(ref) => Some(ClusterProtocolResponse.AllocateNewList(ref))
          case None =>
            messagingClient.sendMessage(
              msg = ClusterProtocolNotification.AllocateNewEmptyListContinue(sender, msgId, jumpsLeft = clusterManager.currentClusterSize - 1),
              respondsToMessage = None,
              recipient = addressOfClusterNode(this.nextClusterNode)
            )
            None
        }

      case AllocateListExtensionStart(listRef, root) =>
        memory.extendRemoteList(listRef, root) match {
          case Some(ref) => Some(ClusterProtocolResponse.AllocateListExtension(ref))
          case None =>
            messagingClient.sendMessage(
              msg = ClusterProtocolNotification.AllocateListExtensionContinue(sender, msgId, jumpsLeft = clusterManager.currentClusterSize - 1, listRef, root),
              respondsToMessage = None,
              recipient = addressOfClusterNode(this.nextClusterNodeInTheRingAvoiding(listRef.clusterNode))
            )
            None
        }

      case Map(listRef, f) =>
        val targetObject = DistributedList[Any](listRef)
        val result = targetObject.map(f.asInstanceOf[Any => Any])
        Some(ClusterProtocolResponse.Map(result.ref))

      case Filter(listRef, p) =>
        val targetObject  = DistributedList[Any](listRef)
        val result = targetObject.filter(p.asInstanceOf[Any => Boolean])
        Some(ClusterProtocolResponse.Filter(result.ref))

      case Append(listRef, element) =>
        val targetObject = DistributedList[Any](listRef)
        val result = targetObject.append(element)
        Some(ClusterProtocolResponse.Append(result.ref))

      case FoldLeft(listRef, seed, op) =>
        val targetObject = DistributedList[Any](listRef)
        val result = targetObject.foldLeft(seed, op.asInstanceOf[(Any,Any) => Any])
        Some(ClusterProtocolResponse.FoldLeft(result))

      case Reverse(listRef) =>
        val targetObject = DistributedList[Any](listRef)
        val result = targetObject.reverse
        Some(ClusterProtocolResponse.Reverse(result.ref))

      case other => throw new RuntimeException(s"unexpected incoming request at cluster node $nodeId, msg = $request")

    }

  }

  private def processIncomingNotification(sender: MessageBrokerClient.EndpointAddress, msgId: MessageId, msg: ClusterProtocolNotification): Unit = {
    import ClusterProtocolNotification._

    msg match {

      case DeleteTreeStart(rootRef) =>
        this.broadcastMessageWithinClusterMembers(DeleteTreeContinue(rootRef))

      case DeleteTreeContinue(rootRef) =>
        memory.deleteWholeTree(rootRef)

      case AllocateNewEmptyListContinue(waitingClient, initialRequestId, jumpsLeft) =>
        memory.startNewList() match {
          case Some(ref) =>
            messagingClient.sendMessage(
              msg = ClusterProtocolResponse.AllocateNewList(ref),
              respondsToMessage = Some(initialRequestId.asInstanceOf[MessageId]),
              recipient = waitingClient)
          case None =>
            messagingClient.sendMessage(
              msg = ClusterProtocolNotification.AllocateNewEmptyListContinue(sender, msgId, jumpsLeft - 1),
              respondsToMessage = None,
              recipient = addressOfClusterNode(this.nextClusterNode))
        }

      case AllocateListExtensionContinue(waitingClient, initialRequestId, jumpsLeft, listToBeExtended, root) =>
        memory.extendRemoteList(listToBeExtended, root) match {
          case Some(ref) =>
            messagingClient.sendMessage(
              msg = ClusterProtocolResponse.AllocateListExtension(ref),
              respondsToMessage = Some(initialRequestId.asInstanceOf[MessageId]),
              recipient = waitingClient
            )
          case None =>
            messagingClient.sendMessage(
              msg = ClusterProtocolNotification.AllocateListExtensionContinue(sender, msgId, jumpsLeft - 1, listToBeExtended, root),
              respondsToMessage = None,
              recipient = addressOfClusterNode(this.nextClusterNodeInTheRingAvoiding(listToBeExtended.clusterNode)))
        }

      case other => throw new RuntimeException(s"unexpected incoming notification at cluster node $nodeId, msg = $msg")

    }
  }


}

