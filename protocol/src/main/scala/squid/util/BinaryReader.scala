package squid.util

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.charset.Charset

/** Helper class for decoding binary data from a stream. */
class BinaryReader(in: InputStream, charset: Charset) {

  /** Retrieves the total bytes read by this reader. */
  def getReadCount: BigInt = readCount

  /**
    * Reads the first byte from the stream, returning -1 for no result.
    * Resets the stream and does not increment the read count.
    */
  def peek(): Int = {
    in.mark(1)
    val result = in.read()
    in.reset()
    result
  }

  /** Skips the given number of bytes. */
  // TODO: This probably should increment the read count.
  def skip(length: Long): Long = in.skip(length)

  /** Applies the reader function n times, returning a Stream of results. */
  def rep[A](n: Int, f: BinaryReader => A): Stream[A] = {
    if (n == 0) {
      Stream.empty
    } else {
      Stream.continually(f(this)).take(n)
    }
  }

  /** Read a single byte from the stream. */
  def read(): Byte = {
    val b = in.read()
    if (b == -1) throw new RuntimeException("Unexpected end of input")
    readCount += 1
    b.toByte
  }

  /** Read a given number of bytes from the stream. */
  def read(length: Int): Array[Byte] = {
    val builder = new ByteArrayOutputStream(length)
    for (i <- 1 to length) {
      builder.write(read())
    }
    builder.toByteArray
  }

  /** Reads a String of the provided length from the stream. */
  def readString(length: Int): String = new String(read(length), charset)

  /** Reads a null-terminated String from the stream. */
  def readStringNul(): String = {
    val builder = new ByteArrayOutputStream()
    while (true) {
      val b = read()
      if (b == 0) return builder.toString(charset.name)
      builder.write(b)
    }
    throw new RuntimeException("Expected null terminator")
  }

  /** Reads a single byte character from the stream. */
  def readChar8(): Char = read().toChar

  /** Reads a 2 byte int from the stream. */
  def readInt16(): Short = ByteBuffer.wrap(read(2)).getShort()

  /** Reads a 4 byte int from the stream. */
  def readInt32(): Int = ByteBuffer.wrap(read(4)).getInt()

  /** Reads a 4 byte int from the stream as a BigInt. */
  def readBigInt32(): BigInt = BigInt(read(4))

  private[this] var readCount: BigInt = BigInt(0)
}
