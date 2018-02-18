package com.selfdualbrain.threaded_cluster

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.selfdualbrain.threaded_cluster.MessageBrokerClient.EndpointAddress

import scala.collection.mutable

/**
  * Straightforward in-memory message broker implementation based on blocking queues
  * and one thread dedicated for messages dispatching.
  *
  * Pretty simplistic but good enough for our in-memory cluster simulation.
  */
class JvmEmbeddedMessageBroker {
  import com.selfdualbrain.threaded_cluster.JvmEmbeddedMessageBroker._

  //used for generating endpoint address for clients which don't provide explicit address upon connecting
  private val clientIdGenerator = new AtomicInteger(0)
  //used to generate message id
  private val msgIdGenerator = new AtomicLong(0)
  //readers-writers lock we use for optimizing concurrent access
  private val semaphore = new RWLock
  //this is where messages "to be delivered" are accumulated
  private val commandsQueue = new LinkedBlockingQueue[JvmEmbeddedMessageBroker.Command]
  //mapping endpoint-address ----> client instance
  private val id2client = new mutable.HashMap[EndpointAddress, JvmEmbeddedMessageBroker.ClientImpl]
  //multimap group-name -----> members
  private val groups = new mutable.HashMap[String, mutable.Set[EndpointAddress]] with mutable.MultiMap[String, EndpointAddress]
  //flag we turn on upon initiation of shutdown sequence - crucial for graceful shutdown implementation
  @volatile private var shutdownInProgress: Boolean = false

  private val dispatcherThread = new Thread {
    override def run(): Unit = {
      while (! shutdownInProgress) {
        handleCommandFromQueue(commandsQueue.take())
      }
    }

    def handleCommandFromQueue(command: Command) {
      command match {
        case Command.SimpleSend(msgId, respondsTo, sender, recipient, msg) =>
          val targetOption: Option[JvmEmbeddedMessageBroker.ClientImpl] = semaphore.criticalSectionAsReader {
            id2client.get(recipient)
          }
          if (targetOption.isDefined)
            targetOption.get.acceptIncomingMessage(msgId, respondsTo, sender, msg)

        case Command.Broadcast(msgId, sender, targetGroup, msg) =>
          val targets: Iterable[EndpointAddress] = semaphore.criticalSectionAsReader {
            groups.getOrElse(targetGroup, Iterable.empty)
          }
          for (targetEndpoint <- targets)
            if (targetEndpoint != sender)
              id2client(targetEndpoint).acceptIncomingMessage(msgId, None, sender, msg)
      }
    }

  }

  //we start messages dispatching thread immediately during creation of a broker instance
  dispatcherThread.start()

  /**
    * Initiation of graceful shutdown sequence.
    * Caution: this will cause dispatcher thread to terminate at the moment it would be normally taking next message from the queue.
    * It means that because of shutdown some messages accepted by the broker (= added to the queue) may end up being destroyed (= never delivered).
    */
  def shutdown(): Unit = {
    shutdownInProgress = true
  }

  def registerNewNode(client: JvmEmbeddedMessageBroker.ClientImpl, nodesGroup: String): EndpointAddress = ensuringWeAreNotDuringShutdown {
    val nodeId = - clientIdGenerator.incrementAndGet()
    val endpoint = EndpointAddress(nodesGroup, nodeId)
    semaphore.criticalSectionAsWriter {
      id2client.put(endpoint, client)
      groups.addBinding(nodesGroup, endpoint)
    }
    return endpoint
  }

  def registerNewNode(client: JvmEmbeddedMessageBroker.ClientImpl, endpoint: EndpointAddress): Unit = ensuringWeAreNotDuringShutdown {
    semaphore.criticalSectionAsWriter {
      id2client.put(endpoint, client)
      groups.addBinding(endpoint.group, endpoint)
    }
  }

  def unregisterNode(endpoint: EndpointAddress): Unit = ensuringWeAreNotDuringShutdown {
    semaphore.criticalSectionAsWriter {
      id2client.remove(endpoint)
      groups.removeBinding(endpoint.group, endpoint)
    }
  }

  def sendMessage(sender: EndpointAddress, recipient: EndpointAddress, respondsTo: Option[Long], msg: Any): Long = ensuringWeAreNotDuringShutdown {
    val msgId = msgIdGenerator.incrementAndGet()
    semaphore.criticalSectionAsReader {
      commandsQueue.put(Command.SimpleSend(msgId, respondsTo, sender, recipient, msg))
    }
    return msgId
  }

  def broadcastMessage(sender: EndpointAddress, nodesGroup: String, msg: Any): Long = ensuringWeAreNotDuringShutdown {
    val msgId = msgIdGenerator.incrementAndGet()
    semaphore.criticalSectionAsReader {
      commandsQueue.put(Command.Broadcast(msgId, sender, nodesGroup, msg))
    }
    return msgId
  }

  private def ensuringWeAreNotDuringShutdown[T](block: =>T): T = {
    if (! shutdownInProgress)
      block
    else
      throw new ShutdownInProgress
  }

}

object JvmEmbeddedMessageBroker {

  private[JvmEmbeddedMessageBroker] abstract class Command

  private[JvmEmbeddedMessageBroker] object Command {

    case class SimpleSend(msgId: Long, respondsTo: Option[Long], sender: EndpointAddress, recipient: EndpointAddress, msg: Any) extends Command

    case class Broadcast(msgId: Long, sender: EndpointAddress, targetGroup: String, msg: Any) extends Command

  }

  class ShutdownInProgress extends Exception

  class ClientImpl(broker: JvmEmbeddedMessageBroker) extends MessageBrokerClient[Long] {
    import MessageBrokerClient._

    private val incomingMessagesQueue = new LinkedBlockingQueue[MsgEnvelope[Long]]

    type BrokerLocationDescriptor = JvmEmbeddedMessageBroker

    private var _isConnected: Boolean = false
    private var _address: Option[EndpointAddress] = None

    override def connect(explicitAddress: EndpointAddress): Unit = this.synchronized {
      if (_isConnected)
        throw new AlreadyConnectedToBroker
      else {
        broker.registerNewNode(this, explicitAddress)
        _address = Some(explicitAddress)
        _isConnected = true
      }
    }

    override def connect(nodesGroup: String): EndpointAddress = this.synchronized {
      if (_isConnected)
        throw new AlreadyConnectedToBroker
      else {
        _address = Some(broker.registerNewNode(this, nodesGroup))
        _isConnected = true
        return _address.get
      }
    }

    override def isConnected: Boolean = _isConnected

    override def endpointAddress: Option[EndpointAddress] = _address


    override def disconnect(): Unit = this.synchronized {
      if (_isConnected) {
        broker.unregisterNode(_address.get)
        _address = None
        _isConnected = false
      }
    }

    override def sendMessage(msg: Any, respondsTo: Option[Long], recipient: EndpointAddress): Long = this.synchronized {
      if (_isConnected)
        return broker.sendMessage(_address.get, recipient, respondsTo, msg)
      else
        throw new NotConnectedToBroker
    }


    override def broadcastMessage(msg: Any, nodesGroup: String): Long = {
      this.synchronized {
        if (_isConnected)
          return broker.broadcastMessage(_address.get, nodesGroup, msg)
        else
          throw new NotConnectedToBroker
      }
    }

    override def waitUntilNextIncomingMessageArrives(): MsgEnvelope[Long] = incomingMessagesQueue.take()

    //used internally by the broker to actually transport messages (broker ---> client)
    def acceptIncomingMessage(msgId: Long, respondsTo: Option[Long], sender: EndpointAddress, msg: Any): Unit = {
      incomingMessagesQueue.put(MsgEnvelope(msgId, respondsTo, sender, msg))
    }
  }

}




