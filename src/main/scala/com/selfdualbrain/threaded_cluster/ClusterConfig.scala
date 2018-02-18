package com.selfdualbrain.threaded_cluster

case class ClusterConfig(
    initialClusterSize: Int,
    nodeMemoryTableSize: Int,
    nodeMemoryTableMaxSaturation: Double,
    nodeMemoryTableAllocationRefusalMargin: Int,
    nodeWorkerThreadsLimit: Int
)
