package com.selfdualbrain.threaded_cluster


/**
  * Contract for message brokers interfacing layer.
  *
  * The general idea is that we have a messaging bus ("broker") and a collection of nodes ("clients") sitting around.
  * A client can control his lifecycle by:
  *   - connecting to broker
  *   - terminate the connection
  *
  * Client can optionally declare a single group name that he wants to join.
  * So from the broker perspective we have a collection of connected clients, every client has an address
  * and there are potentially some disjoint groups of clients identified by group names.
  *
  * When a client is connected, it can:
  *   - send a message to any client (provided he knows the address of the destination) - loopback is also allowed
  *   - broadcast a a message to all clients in a selected group - such a broadcast will NOT include the sender if (it is a member of the target group)
  *
  * Implementations are required to be thread-safe.
  */
trait MessageBrokerClient[MessageId] {
  import MessageBrokerClient._

  /**
    * Initiates connection to the broker with endpoint address enforced.
    *
    * @param explicitAddress endpoint address this client will be identified by
    * @throws BrokerConnectionFailed if the connection could not be established
    * @throws EndpointAddressCollision if such address is already in use
    */
  @throws(classOf[BrokerConnectionFailed])
  @throws(classOf[EndpointAddressCollision])
  def connect(explicitAddress: EndpointAddress)

  /**
    * Initiates connection to the broker. Endpoint address will be assigned within specified group,
    * with node id assigned automatically.
    *
    * @throws BrokerConnectionFailed if the connection could not be established
    * @return assigned endpoint address
    */
  @throws(classOf[BrokerConnectionFailed])
  def connect(groupName: String): EndpointAddress

  /**
    * Terminates connection to the broker.
    * Just does nothing if the client is not connected.
    * Remark: after successful disconnection the client can be connected again with connect() method.
    */
  def disconnect()

  /**
    * Checks connection status.
    */
  def isConnected: Boolean

  /**
    * Returns currently assigned endpoint address (or None if this client is not connected to the broker).
    */
  def endpointAddress: Option[EndpointAddress]

  /**
    * Blocks current thread until next message will be received.
    * (yes, this could be designed using futures instead, but we want to keep things really simple)
    *
    * @throws BrokerConnectionInterrupted if the connection with the broker was terminated while waiting for next message
    * @return a tuple (sender, message), where 'sender' is client address of client that sent the message
    */
  @throws(classOf[BrokerConnectionInterrupted])
  def waitUntilNextIncomingMessageArrives(): MsgEnvelope[MessageId]

  /**
    * Sends the message to specified client.
    * Caution: this delivery will silently fail if the client with a specified address is not currently connected to the broker (!).
    *
    * @param msg message to be sent
    * @param respondsToMessage id of a message this send is a response to
    * @param recipient client where the message should be delivered to
    * @throws BrokerConnectionInterrupted if delivery sender--->broker fails
    */
  @throws(classOf[BrokerConnectionInterrupted])
  def sendMessage(msg: Any, respondsToMessage: Option[MessageId], recipient: EndpointAddress): MessageId

  /**
    * Sends message to all (currently connected to the broker) members of specified group.
    * There is one exceptional case: if the sender is also a member of target group, the message will NOT
    * be delivered to the sender.
    *
    * @param msg message to be sent
    * @param nodesGroup group name where the broadcast should happen
    * @throws BrokerConnectionInterrupted if delivery sender--->broker fails
    */
  @throws(classOf[BrokerConnectionInterrupted])
  def broadcastMessage(msg: Any, nodesGroup: String): MessageId

}

object MessageBrokerClient {

  /**
    * (Logical) address of a connected client.
    *
    * @param group group of nodes (this is just a string defined ad hoc by a client upon connecting)
    * @param node id of a node; must be unique within a group of nodes (it is driver-broker cooperation responsibility to ensure
    *             uniqueness of endpoint addresses)
    */
  case class EndpointAddress(group: String, node: Int) {
    override def toString: String = s"$group:$node"
  }

  case class MsgEnvelope[MID](msgId: MID, respondsTo: Option[MID], sender: EndpointAddress, payload: Any)

  class BrokerConnectionFailed(cause: Exception) extends Exception(cause)
  class EndpointAddressCollision(address: EndpointAddress) extends Exception
  class BrokerConnectionInterrupted(cause: Exception) extends Exception(cause)
  class BrokerIsShuttingDown extends Exception
  class AlreadyConnectedToBroker extends Exception
  class NotConnectedToBroker extends Exception

}


