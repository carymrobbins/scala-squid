package squid.protocol

import squid.util.BinaryReader

/** Base trait for values retrieved from PostgreSQL queries via the protocol. */
sealed trait PGValue {
  def length: Int = this match {
    case PGValue.Null => -1
    case PGValue.Text(v) => v.length
    case PGValue.Binary(v) => v.length
  }

  def encode: Array[Byte] = this match {
    case PGValue.Null => Array.empty
    case PGValue.Text(v) => v.getBytes(PGProtocol.CHARSET)
    case PGValue.Binary(v) => v
  }

  def as[A : FromPGValue]: A = {
    implicitly[FromPGValue[A]].fromPGValue(this) match {
      case Right(a) => a
      case Left(err) => throw new RuntimeException(err) // TODO: Better exception
    }
  }
}

object PGValue {
  case object Null extends PGValue
  sealed case class Text(value: String) extends PGValue
  sealed case class Binary(value: Array[Byte]) extends PGValue

  /** Decode a PGValue from binary. */
  def decode(r: BinaryReader): PGValue = r.readInt32() match {
    case 0xFFFFFFFF => PGValue.Null
    case len => PGValue.Text(r.readString(len))
  }
}

/** Typeclass for converting a Scala value into a PGValue. */
trait ToPGValue[A] {
  def toPGValue(a: A): PGValue
}

object ToPGValue {
  /** Simplified constructor for building ToPGValue instances. */
  def apply[A](f: A => PGValue): ToPGValue[A] = new ToPGValue[A] {
    override def toPGValue(a: A): PGValue = f(a)
  }

  /** Helper method to convert a Scala value to a PGValue. */
  def from[A : ToPGValue](a: A): PGValue = implicitly[ToPGValue[A]].toPGValue(a)

  implicit val toPGValueInt: ToPGValue[Int] = ToPGValue(n => PGValue.Text(n.toString))

  implicit val toPGValueOID: ToPGValue[OID] = ToPGValue(oid => from(oid.toInt))

  implicit val toPGValueString: ToPGValue[String] = ToPGValue(PGValue.Text)
}

/** Typeclass for converting a PGValue into a Scala value. */
trait FromPGValue[A] {
  def fromPGValue(v: PGValue): Either[String, A]
}

object FromPGValue {
  /** Simplified constructor for building FromPGValue instances. */
  def apply[A](f: PGValue => Either[String, A]): FromPGValue[A] = new FromPGValue[A] {
    override def fromPGValue(v: PGValue): Either[String, A] = f(v)
  }

  implicit val fromPGValueBoolean: FromPGValue[Boolean] = FromPGValue {
    case PGValue.Text("t") => Right(true)
    case PGValue.Text("f") => Right(false)
    case other => Left(s"Expected 't' or 'f', got: $other")
  }

  implicit val fromPGValueOID: FromPGValue[OID] = FromPGValue {
    case PGValue.Text(s) =>
      try {
        Right(OID(s.toInt))
      } catch {
        case e: NumberFormatException => Left(s"Invalid OID number: $s")
      }

    case other => Left(s"Expected OID number, got: $other")
  }

  implicit val fromPGValueString: FromPGValue[String] = FromPGValue {
    case PGValue.Text(s) => Right(s)
    case other => Left(s"Expected String, got: $other")
  }
}
