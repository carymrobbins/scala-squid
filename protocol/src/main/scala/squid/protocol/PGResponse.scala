package squid.protocol

/** Describes success or error responses from the protocol. */
sealed trait PGResponse[+A] {
  def fold[X](onError: PGResponse.Error => X, onSuccess: A => X): X = this match {
    case e: PGResponse.Error => onError(e)
    case PGResponse.Success(a) => onSuccess(a)
  }

  def map[B](f: A => B): PGResponse[B] = this match {
    case e: PGResponse.Error => e
    case PGResponse.Success(a) => PGResponse.Success(f(a))
  }

  def flatMap[B](f: A => PGResponse[B]): PGResponse[B] = this match {
    case e: PGResponse.Error => e
    case PGResponse.Success(a) => f(a)
  }

  def collect[B](err: String)(pf: PartialFunction[A, B]): PGResponse[B] = {
    flatCollect(err)(pf.andThen(PGResponse.Success(_)))
  }

  def flatCollect[B](err: String)(pf: PartialFunction[A, PGResponse[B]]): PGResponse[B] = {
    flatMap { a => if (pf.isDefinedAt(a)) pf(a) else PGResponse.Error(s"$err, got: $a") }
  }

  def successOr[AA >: A](f: PGResponse.Error => AA): AA = fold(f, identity)

  def getOrThrow: A = fold(err => throw PGProtocolError.fromPGResponseError(err), identity)
}

object PGResponse {
  final case class Success[+A](result: A) extends PGResponse[A]

  final case class Error(
    message: String,
    er: Option[PGBackendMessage.ErrorResponse] = None
  ) extends PGResponse[Nothing] {
    def getMessageForSQL(sql: String): String = er.map(_.getMessageForSQL(sql)).getOrElse(message)
  }

  def fromErrorResponse(er: PGBackendMessage.ErrorResponse): Error = {
    Error(er.getMessage, Some(er))
  }
}
