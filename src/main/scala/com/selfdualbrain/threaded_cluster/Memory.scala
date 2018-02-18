package com.selfdualbrain.threaded_cluster

import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}
import java.util.concurrent.{ConcurrentHashMap, ThreadLocalRandom}

/**
  * Because JVM memory model does not give us integer pointers to objects, we have a fundamental problem
  * of "how to externalize" a reference to an object. And we have to externalize because we are building a cluster,
  * so ultimately at some point we need to reference an object stored in the memory of machine A, while sitting in the memory
  * of machine B.
  *
  * Changing to another programming language (not JVM-based) would be one option to solve the problem - for example
  * in C++, Ruby and some implementations of Smalltalk we have the idea of immutable object id built into VM or the execution model.
  * Nevertheless, assuming that we want to stay on JVM, two options are left:
  *   1. use canonicalizing mapping, so pretty much sort of a large Int ---> Object map
  *   2. implement the memory yourself
  *
  * Both solutions are viable but their points of complexity are placed somewhat differently.
  * So choosing one depends on how you set up requirements.
  *
  * Memory, as implemented here, stands as a crucial component for the approach (2) above.
  *
  * The implementation is based on following requirements:
  *   1. Keep the solution minimal by supporting only operations supporting the specific data structure we need (= distributed functional list in this case).
  *   2. The memory must be thread-safe, as we assume cluster nodes to be multi-threaded.
  *   3. We are NOT going to implement full-size distributed garbage collection algorithm. Instead, we want a support for
  *   a minimalistic solution where there is specific "delete" operation: delete a tree (tree = all lists that share the same root).
  *
  * ==== Implementation idea ====
  *
  * We organize the memory as a single large array of references, allocated once and forever (so, never growing). Index in this array ("slot", as we call it)
  * becomes a "pointer" in our memory. To keep it thread safe and to comply with java memory model we cannot use just a regular built-in array structure
  * (as its cells are not "volatile") so we actually use AtomicReferenceArray instead.
  *
  * Every slot in the memory has 4 possible states:
  *   1. it is unused (= null)
  *   2. it is occupied by a Terminator - this is the "root" of immutable list "tree" (corresponds to Empty cell in classic functional list implementation)
  *   3. it is occupied by a Nub - keeps one element of a list (corresponds to Cons cell in classic functional list implementation)
  *   4. it is occupied by a Jumper - connects parts of a list that spans across the boundary between cluster nodes
  *
  * Every Terminator keeps a magic number (selected sequentially at the creation of new empty list). We need this magic number for 2 reasons:
  *   1. References to remote lists can become invalid (due to our 'fake' garbage collection). Without magic number we could not discover
  *   that a slot contains a head of a list belonging to a different 'generation' of data, then the one when some old remote reference was created.
  *   2. Thanks to the magic number we do not have to stop the whole cluster to perform the deletion of a list tree. Instead the deletion effort
  *   may safely execute in the background and the reclaimed memory may be immediately reused while the deletion process is on-going.
  *   Every element belonging to the tree being deleted can be identified by (root-location-cluster, root-location-slot, root-magic-number).
  *   While the cluster operates we can have many lists referencing the same root (only one is currently existing, others are just deleted but
  *   the deletion effort apparently is still unfinished).
  *
  * Caution: we frequently refer to a "tree" meaning the tree of lists having the same root. This is crucial that implementation of functional lists
  * leads to a tree structure in memory. In this tree every node is a head of one list.
  *
  * @param owningClusterNode id of a cluster node owning this memory
  * @param masterTableSize physical size of this memory (= number of slots), we anticipate that typically this is a value in range 10^6 ... 10^9
  * @param maxFillLevel a value in range 0.0000 ... 1.000 marking maximal utilization of the memory array (bigger value means better memory utilization
  *                     with a price of lower performance); reasonable values are in range 0.5 ... 0.9
  */
class Memory(owningClusterNode: ClusterNodeId, masterTableSize: Int, maxFillLevel: Double) {
  import Memory._

  //the actual "cells" of memory
  private val array = new AtomicReferenceArray[Memory.Item](masterTableSize)
  //memory allocation algorithm is based on sequential search starting in a random place
  private val random = ThreadLocalRandom.current()
  //for generating magic number (happens every time we start a new list "root"
  private val magicNumberGenerator = new AtomicInteger(0)
  //counter of slots in use so we immediately know when the memory is full
  private val numberOfSlotsInUse = new AtomicInteger(0)
  //the maximal number of slots that we can allocate (which is less then the size of master cells array - to keep the performance on a reasonable level)
  private val capacity: Int = (masterTableSize * maxFillLevel).toInt
  //hash set of all trees that happen to be (at least partially) stored in our memory space - this is to optimize performance of deletion algorithm
  private val registeredRoots = new ConcurrentHashMap[ListRef, Unit]() // using map because we have no ConcurrentHashSet in JDK
  //registeredRoots keeps only pairs (root -> unit), so for us this really works like a set of keys;
  //so we need this unit instance but Scala literal for it looks strange and may be a little misleading while reading the source code
  //hence (just for clarity) we use a properly named value instead
  private val unitSingleton: Unit = ()

  /**
    * I start a new list. This means creation of a new Terminator item and storing it at some slot.
    * I return a cluster-wide reference (= ListRef) to this new list.
    *
    * @return Some(pointer) or None if I am full (=no storage space left)
    */
  def startNewList(): Option[ListRef] = {
    val terminator = Terminator(magicNumberGenerator.incrementAndGet())
    this.alloc(terminator) match {
      case Some(slot) =>
        val ref = ListRef(owningClusterNode, slot, terminator.magic)
        registeredRoots.put(ref, unitSingleton)
        return Some(ref)
      case None =>
        return None
    }
  }

  /**
    * I extend a local list. This means adding one element (as new head) to a list which head I am currently storing.
    * Technically this means creation of a new Nub item and storing it at some slot.
    * I return the pointer (= ListRef) of this new list.
    *
    * @param element element to be added as new head of the list
    * @param slotOfCurrentHead local pointer to the current head of the list
    * @return Some(ref) or None if I am full (=no storage space left)
    */
  def extendLocalList(element: Any, slotOfCurrentHead: Slot): Option[ListRef] = {
    val root = this.getRootFor(slotOfCurrentHead)
    val nub = Nub(element, slotOfCurrentHead, root)
    return this.alloc(nub).map(slot => ListRef(owningClusterNode, slot, root.magic))
  }

  /**
    * I extend a list from another cluster node.
    * Technically this means creation of a new  Jumper item and storing it at some slot.
    * This way a new local list is created. The new list has the same elements as the remote list.
    * I return the pointer (= ListRef) of this new list.
    *
    * @param remoteList reference to the list in another cluster node
    * @param root reference to the root of the list
    * @return Some(ref) or None if I am full (=no storage space left)
    */
  def extendRemoteList(remoteList: ListRef, root: ListRef): Option[ListRef] = {
    val jumper = Jumper(remoteList, root)
    registeredRoots.put(root, unitSingleton)
    return this.alloc(jumper).map(slot => ListRef(owningClusterNode, slot, root.magic))
  }

  /**
    * Given the 'pointer' (which is just a slot number) I read the memory table at this pointer.
    *
    * @param slot 'address' of the memory (think 'pointer")
    * @return whatever was stored there
    */
  def readItemAtAddress(slot: Slot): Memory.Item = array.get(slot)

  /**
    * This is rather rough replacement for a "real" garbage collection mechanism (that is much more difficult to implement).
    * I delete all items belonging to the tree (the root of the tree is provided).
    *
    * @param rootToBeCleared root of the tree to be deleted
    */
  def deleteWholeTree(rootToBeCleared: ListRef): Unit = {
    if (registeredRoots.containsKey(rootToBeCleared)) {
      registeredRoots.remove(rootToBeCleared)
      for (slot <- 0 until masterTableSize)
        array.get(slot) match {
          case Nub(element, slotOfNextItem, root) =>
            if (root == rootToBeCleared) {
              array.set(slot, null)
              numberOfSlotsInUse.decrementAndGet()
            }
          case Jumper(nextItem, root) =>
            if (root == rootToBeCleared) {
              array.set(slot, null)
              numberOfSlotsInUse.decrementAndGet()
            }
          case Terminator(magic) =>
            if (rootToBeCleared.magic == magic) {
              array.set(slot, null)
              numberOfSlotsInUse.decrementAndGet()
            }
        }
    }
  }

  /**
    * Number of free slots at the moment (= number of additional items that I can store).
    */
  def freeMemorySlots: Int = capacity - numberOfSlotsInUse.get()

  /**
    * Number of used memory slots (= number of items I currently store).
    */
  def usedMemorySlots: Int = numberOfSlotsInUse.get()

  /**
    * Number of used memory slot as fraction of total capacity.
    */
  def memoryUtilization: Double = usedMemorySlots.toDouble / capacity.toDouble

  /**
    * Number of used memory slots as fraction of master table size.
    */
  def memoryTableCurrentSaturation: Double = usedMemorySlots.toDouble / masterTableSize

  /**
    * Number of list trees that I am storing.
    *
    * Caution: list tree can span several cluster nodes, so several memory segments.
    * Here we count every tree that overlaps with this memory segment.
    */
  def numberOfRegisteredRoots: Int = registeredRoots.size()

  /**
    * Average number of slots belonging to the same tree.
    * This is just a statistical value that may be possibly used for some optimizations.
    */
  def averageTreeSize: Double = usedMemorySlots.toDouble / numberOfRegisteredRoots

  /**
    * I extract the reference to the root of the list with head stored in provided slot
    *
    * @param slot location of the head
    * @return ref to the root (= terminator item for this list)
    * @throws RuntimeException if there was no list head at this location
    */
  def getRootFor(slot: Slot): ListRef = {
    val memoryItem: Memory.Item = array.get(slot)
    if (memoryItem == null)
      throw new RuntimeException(s"encountered null pointer while trying to get root reference, cluster node=$owningClusterNode, looked-up-slot=$slot")

    return memoryItem match {
      case Nub(e, next, r) => r
      case Jumper(next, r) => r
      case Terminator(magic) => ListRef(owningClusterNode, slot, magic)
    }
  }

  /**
    * I find an empty slot in main table an I store provided item in this slot.
    *
    * @param item item to be stored in the memory
    * @return Some(pointer) or None if I am too full (= all space is used)
    */
  private def alloc(item: Memory.Item): Option[Slot] = {
    val numberOfSlotsInUseAfterAllocation = numberOfSlotsInUse.incrementAndGet()
    if (numberOfSlotsInUseAfterAllocation > capacity) {
      numberOfSlotsInUse.decrementAndGet()
      return None
    }

    var slot = random.nextInt(masterTableSize)
    while (! array.compareAndSet(slot, null, item))
      slot = (slot + 1) % masterTableSize

    return Some(slot)
  }

}

object Memory {

  //Algebraic data type used for representing distributed lists.
  //We are actually conceptually quite close to classic functional lists:
  //  Nub ........... corresponds to ..... Cons
  //  Terminator .... corresponds to ..... EmptyList
  //  Jumper ........ one extra case we needed for joining pieces of list that live on different memory segments
  sealed abstract class Item
  case class Nub(element: Any, slotOfNextItem: Slot, root: ListRef) extends Item
  case class Jumper(nextItem: ListRef, root: ListRef) extends Item
  case class Terminator(magic: Magic) extends Item

}

