package com.selfdualbrain.threaded_cluster

/**
  * Cluster-wide reference to a list.
  * It is actually the pointer to the head of the list.
  * This is a simple data structure that can safely be serialized or passed between nodes.
  *
  * @param clusterNode id of a cluster node (where the head of the list is stored)
  * @param slot address in the cluster node memory (where the head of the list is stored)
  * @param magic magic number assigned to the root of the tree where the list belongs; thanks to magic numbers our
  *              "brute force" garbage collection approach can operate in background (we can filter out members of a tree to be deleted
  *              even after the root location is already reused and also we are able to recognize dead references)
  */
case class ListRef(clusterNode: ClusterNodeId, slot: Slot, magic: Magic) {
  def clusterNodeEndpoint: MessageBrokerClient.EndpointAddress = MessageBrokerClient.EndpointAddress("cluster", clusterNode)
}
