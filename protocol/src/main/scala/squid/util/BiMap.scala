package squid.util

import scala.collection.mutable

/** Poor man's bidirectional map. */
class BiMap[A, B] {

  def update(a: A, b: B): Unit = {
    removeByLeft(a)
    removeByRight(b)
    ab.update(a, b)
    ba.update(b, a)
  }

  def removeByLeft(a: A): Unit = {
    ab.get(a).foreach { ba.remove }
    ab.remove(a)
  }

  def removeByRight(b: B): Unit = {
    ba.get(b).foreach { ab.remove }
    ba.remove(b)
  }

  def getByLeft(a: A): Option[B] = ab.get(a)

  def getByRight(b: B): Option[A] = ba.get(b)

  private val ab = mutable.Map.empty[A, B]
  private val ba = mutable.Map.empty[B, A]
}

object BiMap {
  def empty[A, B] = new BiMap[A, B]
}
