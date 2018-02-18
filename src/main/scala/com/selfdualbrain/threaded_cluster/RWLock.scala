package com.selfdualbrain.threaded_cluster

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
  * Adds some syntax sugar around standard read-write lock from JDK.
  */
class RWLock {
  val semaphore = new ReentrantReadWriteLock(true) //using fair=true to avoid starving

  def criticalSectionAsReader[T](block: => T): T = {
    semaphore.readLock().lock()
    try {
      val result: T = block
      return result
    } finally {
      semaphore.readLock().unlock()
    }
  }

  def criticalSectionAsWriter[T](block: => T): T = {
    semaphore.writeLock().lock()
    try {
      val result: T = block
      return result
    } finally {
      semaphore.writeLock().unlock()
    }
  }

}