package squid.util

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.Charset

/** Helper class for encoding values to binary. */
class BinaryWriter(charset: Charset) {

  /** Writes a single byte. */
  def writeByte(b: Byte): Unit = builder.write(b)

  /** Writes the given bytes. */
  def writeBytes(bs: Array[Byte]): Unit = builder.write(bs)

  /** Writes a character as a single byte. */
  def writeChar8(c: Char): Unit = builder.write(c.toByte)

  /** Writes a 2 byte int. */
  def writeInt16(n: Short): Unit = builder.write(ByteBuffer.allocate(2).putShort(n).array())

  /** Writes a 4 byte int. */
  def writeInt32(n: Int): Unit = builder.write(ByteBuffer.allocate(4).putInt(n).array())

  /** Writes bytes then terminates with a null byte. */
  def writeStringNul(s: Array[Byte]): Unit = {
    builder.write(s)
    builder.write(0)
  }

  /** Writes the provided string, terminating it with a null byte. */
  def writeStringNul(s: String): Unit = writeStringNul(s.getBytes(charset))

  /** Returns the currently written bytes. */
  def toByteArray: Array[Byte] = builder.toByteArray

  private val builder = new ByteArrayOutputStream()
}
