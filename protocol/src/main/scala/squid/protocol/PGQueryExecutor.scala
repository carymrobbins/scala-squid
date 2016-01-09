package squid.protocol

import scala.annotation.tailrec
import scala.collection.mutable

import PGBackendMessage._
import PGFrontendMessage._
import squid.util.StreamAssertions

/** Manages execution of backend queries. */
final class PGQueryExecutor(socket: PGSocket) {

  /** Describes a prepared statement given its SQL string and parameter types (optional). */
  def describe(sql: String, types: List[OID]): DescribeResult = {
    socket.sync()
    socket.send(Parse("", sql, types))
    socket.send(Describe(""))
    socket.send(Flush)
    socket.send(Sync)
    socket.flush()

    @tailrec
    def waitForParseTree(): String = socket.receive() match {
      case Some(NoticeResponse(fields)) =>
        fields.toMap.get('D') match {
          case Some(result) => result // Found parse tree
          case None => waitForParseTree() // Ignore and recurse, waiting for parse tree
        }

      case Some(err: ErrorResponse) => throw PGProtocolError(err)
      case other => throw new RuntimeException(s"Expected NoticeResponse, got: $other")
    }

    val parseTree = waitForParseTree()
    socket.receive() match {
      case Some(ParseComplete) => // Skip
      case other => throw new RuntimeException(s"Expected ParseComplete, got: $other")
    }
    val paramTypes = socket.receive() match {
      case Some(ParameterDescription(pts)) => pts
      case other => throw new RuntimeException(s"Expected ParameterDescription, got: $other")
    }
    val cols = socket.receive() match {
      case Some(NoData) => List()
      case Some(RowDescription(cds)) =>
        cds.map { cd =>
          val nullable = columnIsNullable(cd.table, cd.number)
          DescribeColumn(cd.name, cd.colType, nullable)
        }
      case other => throw new RuntimeException(s"Expected RowDescription, got: $other")
    }
    DescribeResult(paramTypes, cols, parseTree)
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
      private var nextMsg = socket.receive()

      override def hasNext: Boolean = nextMsg match {
        case Some(EmptyQueryResponse) => false
        case Some(_: CommandComplete) => false
        case _ => true
      }

      override def next(): List[PGValue] = nextMsg match {
        case Some(DataRow(values)) =>
          nextMsg = socket.receive()
          values

        case other => throw new RuntimeException(s"Unexpected row message: $other")
      }
    }.toStream
  }

  /** Binds a prepared statement. */
  private def bind(query: String, params: List[PGParam], binaryCols: List[Boolean]): Unit = {
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

    while (true) {
      socket.receive() match {
        case Some(ParseComplete) => preparedStatements.update(key, id)
        case Some(_: NoticeResponse) => // Parse tree, ignored.
        case Some(BindComplete) => return
        case other => throw new RuntimeException(s"Unexpected response: $other")
      }
    }
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
