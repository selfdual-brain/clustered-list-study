package com.selfdualbrain.threaded_cluster

/**
  * Custom implementation of JVM thread that we use to emulate the execution model of a cluster.
  *
  * @param clusterNode pointer to the cluster node singleton
  * @param task the program that this thread will be executing once started
  */
class ClusterNodeAwareThread[MessageId](val clusterNode: ClusterNode[MessageId], task: Runnable) extends Thread(task) {

}
