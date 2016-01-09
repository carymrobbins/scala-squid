package squid.util

/** Utilities for pattern matching on streams. */
object StreamAssertions {
  def zeroOrOne[A](s: Stream[A]): Option[A] = s match {
    case Stream.Empty => None
    case a #:: Stream.Empty => Some(a)
    case other => throw new RuntimeException(s"Expected zero or one element, got: $other")
  }

  def zeroOrTwo[A](s: Stream[A]): Option[(A, A)] = s match {
    case Stream.Empty => None
    case a1 #:: a2 #:: Stream.Empty => Some((a1, a2))
    case other => throw new RuntimeException(s"Expected zero or two elements, got: $other")
  }
}
