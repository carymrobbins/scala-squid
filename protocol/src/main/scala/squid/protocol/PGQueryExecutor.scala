package squid.protocol

import scala.annotation.tailrec
import scala.collection.mutable

import PGBackendMessage._
import PGFrontendMessage._
import squid.util.StreamAssertions

/** Manages execution of backend queries. */
final class PGQueryExecutor(socket: PGSocket) {

  /** Describes a prepared statement given its SQL string and parameter types (optional). */
  def describe(sql: String, types: List[OID]): PGResponse[DescribeResult] = {
    socket.sync()
    socket.send(Parse("", sql, types))
    socket.send(Describe(""))
    socket.send(Flush)
    socket.send(Sync)
    socket.flush()

    @tailrec
    def waitForParseTree(): PGResponse[String] = socket.receive() match {
      case err: PGResponse.Error => err

      case PGResponse.Success(x) =>
        x match {
          case Some(NoticeResponse(fields)) =>
            fields.toMap.get('D') match {
              case Some(result) => PGResponse.Success(result) // Found parse tree
              case None => waitForParseTree() // Ignore and recurse, waiting for parse tree
            }

          case other => PGResponse.Error(s"Expected NoticeResponse during describe, got: $other")
        }
    }

    for {
      parseTree <- waitForParseTree()
      _ <- socket.receive().collect("Expected ParseComplete") {
        case Some(ParseComplete) =>
      }
      paramTypes <- socket.receive().collect("Expected ParameterDescription") {
        case Some(ParameterDescription(pts)) => pts
      }
      cols <- socket.receive().collect("Expected NoData or RowDescription") {
        case Some(NoData) => Nil
        case Some(RowDescription(cds)) =>
          cds.map { cd =>
            val nullable = columnIsNullable(cd.table, cd.number)
            DescribeColumn(cd.name, cd.colType, nullable)
          }
      }
    } yield DescribeResult(paramTypes, cols, parseTree)
  }

  /** Executes a prepared query and returns its results int a list. */
  def prepared
      (query: String, params: List[PGParam], binaryCols: List[Boolean] = Nil)
      : Stream[List[PGValue]] = {
    bind(query, params, binaryCols)
    socket.send(Execute("", 0)) // zero for "fetch all rows"
    socket.send(Flush)
    socket.send(Sync)
    socket.flush()
    new Iterator[List[PGValue]] {
      // NOTE: There doesn't seem to be a way around throwing an exception here if we
      // get an ErrorResponse unless we return a List instead of a Stream.  If we
      // do get an ErrorResponse while traversing the result set, this seems rather exceptional
      // and unexpected, so throwing an exception might be the best bet either way.
      private def receiveNext() = socket.receive().getOrThrow

      private var nextMsg = receiveNext()

      override def hasNext: Boolean = nextMsg match {
        case Some(EmptyQueryResponse) => false
        case Some(_: CommandComplete) => false
        case _ => true
      }

      override def next(): List[PGValue] = nextMsg match {
        case Some(DataRow(values)) =>
          nextMsg = receiveNext()
          values

        case other => throw new RuntimeException(s"Unexpected row message: $other")
      }
    }.toStream
  }

  /** Binds a prepared statement. */
  private def bind(query: String, params: List[PGParam], binaryCols: List[Boolean]): PGResponse[Unit] = {
    socket.sync()
    val paramTypes = params.flatMap(_.typeOID)
    val key = (query, paramTypes)
    val id = preparedStatements.getOrElse(key, {
      val id = preparedStatements.size + 1
      socket.send(Parse(name = id.toString, query, paramTypes))
      id
    })
    val paramValues = params.map(_.value)
    socket.send(Bind(portal = "", name = id.toString, paramValues, binaryCols))
    socket.send(Flush) // TODO: Why do we have to send a Flush here?
    socket.flush()

    @tailrec
    def loop(): PGResponse[Unit] = socket.receive() match {
      case err: PGResponse.Error => err

      case PGResponse.Success(x) =>
        x match {
          case Some(ParseComplete) =>
            preparedStatements.update(key, id)
            loop()

          case Some(_: NoticeResponse) => loop() // Parse tree, ignored.
          case Some(BindComplete) => PGResponse.Success(())
          case other => PGResponse.Error(s"Unexpected response during bind: $other")
        }
    }
    loop()
  }

  /** Infers column nullability by checking the pg_attribute table. */
  private def columnIsNullable(table: OID, colNum: Int): Boolean = {
    if (table.toInt == 0) {
      true
    } else {
      val colRef = ColRef(table, colNum)
      columnNullables.getOrElse(ColRef(table, colNum), {
          val result = prepared(
            "select attnotnull from pg_catalog.pg_attribute where attrelid = $1 and attnum = $2",
            List(PGParam.from(table), PGParam.from(colNum))
          ).flatten
          // val nullable = result.flatten ma(v => !v.as[Boolean]).getOrElse(true)
          val nullable = StreamAssertions.zeroOrOne(result) match {
            case None => true
            case Some(v) => !v.as[Boolean]
          }
          columnNullables.update(colRef, nullable)
          nullable
      })
    }
  }

  private case class ColRef(table: OID, colNum: Int)

  private val preparedStatements = mutable.Map.empty[(String, List[OID]), Int]
  private val columnNullables = mutable.Map.empty[ColRef, Boolean]
}
