package com.selfdualbrain.threaded_cluster

import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}

import org.slf4j.LoggerFactory

/**
  * Enriches (decorates) messaging clients with an ability to perform synchronous (request-response) calls.
  * We follow the "decorator pattern" here.
  */
class ClusterCommunicationClient[MessageId](messagingClient: MessageBrokerClient[MessageId]) extends MessageBrokerClient[MessageId] {
  private val log = LoggerFactory.getLogger("comm-client")

  import MessageBrokerClient._

  //request id ----> response_holder mapping; response holder works also as a semaphore on which we hang the calling thread after sending a RMI request
  private val rmiSemaphoresMap = new ConcurrentHashMap[MessageId, ResponseHolder]

  //buffer for incoming messages
  private val incomingMessagesQueue: BlockingQueue[MsgEnvelope[MessageId]] = new LinkedBlockingQueue[MsgEnvelope[MessageId]]

  //cached here to optimize logging performance
  private var myAddress: MessageBrokerClient.EndpointAddress = _

  private class IncomingTrafficProcessor extends Thread {
    this.setPriority(Thread.NORM_PRIORITY - 1) //taking new work has lower priority than finishing work we already took
    override def run(): Unit = {
      while (! shutdownInProgress) {
        val incomingMessage: MsgEnvelope[MessageId] = messagingClient.waitUntilNextIncomingMessageArrives()
        incomingMessage.respondsTo match {
          case None => incomingMessagesQueue.put(incomingMessage)
          case Some(requestId) =>
            val resultHolder = rmiSemaphoresMap.get(requestId)
            if (resultHolder == null)
              log.warn(s"(${messagingClient.endpointAddress.get}) Received RMI response message but entry in semaphores map was not found, msg=$incomingMessage")
            resultHolder.synchronized {

              resultHolder.result = Some(incomingMessage.payload.asInstanceOf[ClusterProtocolResponse])
              resultHolder.notify()
            }
        }
      }
    }
  }

  //flag signalling that shutdown sequence of this cluster node was already initiated
  @volatile private var shutdownInProgress: Boolean = false

  //used for calling thread blocking in our "inter-cluster-nodes RMI" implementation
  class ResponseHolder {
    var result: Option[Any] = None
  }

  //A single thread that handles incoming response messages traffic.
  //As opposed to requests, we do not spawn new threads here.
  //The single handling thread actually does the whole job.
  //It must be said that the job is pretty simple however - we need to find the thread
  //hanging and waiting for the response, then we need to provide the response and to resume this thread.
  private var incomingResponsesLoopThread: Thread = _

  override def connect(explicitAddress: MessageBrokerClient.EndpointAddress): Unit = {
    messagingClient.connect(explicitAddress)
    myAddress = explicitAddress
    incomingResponsesLoopThread = new IncomingTrafficProcessor
    incomingResponsesLoopThread.start()
    log.debug(s"($myAddress) connected")
  }

  override def connect(groupName: String): MessageBrokerClient.EndpointAddress = {
    messagingClient.connect(groupName)
    myAddress = messagingClient.endpointAddress.get
    incomingResponsesLoopThread = new IncomingTrafficProcessor
    incomingResponsesLoopThread.start()
    log.debug(s"($myAddress) connected")
    return messagingClient.endpointAddress.get
  }

  override def disconnect(): Unit = {
    messagingClient.disconnect()
    shutdownInProgress = true
    log.debug(s"($myAddress) disconnected")
    myAddress = null
  }

  override def isConnected: Boolean = messagingClient.isConnected

  override def endpointAddress: Option[MessageBrokerClient.EndpointAddress] = messagingClient.endpointAddress

  override def broadcastMessage(msg: Any, nodesGroup: String): MessageId = messagingClient.broadcastMessage(msg, nodesGroup)

  override def sendMessage(msg: Any, respondsToMessage: Option[MessageId], recipient: MessageBrokerClient.EndpointAddress): MessageId = {
    log.debug(s"($myAddress) tell message $msg to $recipient")
    messagingClient.sendMessage(msg, respondsToMessage, recipient)
  }

  /**
    * Executes synchronous request-response communication.
    * Effectively this can be understood as calling remote functionality offered by a server (=cluster member).
    * The sending thread will pause on a semaphore and will wait for the response message to come.
    * Remark: we do not implement any timeouts here so the caller is going to wait forever.
    *
    * @param serverAddress messaging-layer address of the server (= cluster member)
    * @param requestMessage the message to be sent
    * @return the message received from the server as a response
    */
  def sendMessageAndWaitForResponse(serverAddress: MessageBrokerClient.EndpointAddress, requestMessage: Any): Any = {
    log.debug(s"($myAddress) ask message $requestMessage to $serverAddress")
    val requestId = messagingClient.sendMessage(requestMessage, None, serverAddress)
    val holder = new ResponseHolder()
    rmiSemaphoresMap.put(requestId, holder)
    holder.synchronized {
      holder.wait()
      val result = holder.result.get
      rmiSemaphoresMap.remove(requestId)
      return result
    }
  }

  /**
    * Blocks current thread until next non-response message will be received.
    * So, incoming messages that are recognized as responses to requests we previously sent out are filtered away
    * and will NOT wake up a thread waiting in effect of calling waitUntilNextIncomingMessageArrives()
    *
    * @throws BrokerConnectionInterrupted if the connection with the broker was terminated while waiting for next message
    * @return a tuple (sender, message), where 'sender' is client address of client that sent the message
    */
  @throws(classOf[BrokerConnectionInterrupted])
  override def waitUntilNextIncomingMessageArrives(): MsgEnvelope[MessageId] = {
    log.debug(s"($myAddress) waiting for next message")
    val result = incomingMessagesQueue.take()
    log.debug(s"($myAddress) received message: $result")
    return result
  }

}
