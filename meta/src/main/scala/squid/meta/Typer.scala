package squid.meta

import squid.meta.ASTHelper.QualField
import squid.parser.ast._

/**
  * TODO
  */
object Typer {
  def getReturnValues(sql: SQL)(implicit tmd: TableMetaData): List[RetVal] = {
    sql match {
      case s: Select => getReturnValues(s)
    }
  }

  private def getReturnValues(s: Select)(implicit tmd: TableMetaData): List[RetVal] = {
    s.targetList.map(getReturnValue(_, s))
  }

  private def getReturnValue(rt: ResTarget, s: Select)(implicit tmd: TableMetaData): RetVal = {
    val ResTarget(v, name) = rt
    v match {
      case cf: ColumnRef =>
        ASTHelper.getQualifiedField(cf) match {
          case QualField(None, fieldName) =>
            // Find table with field `n`
            val column = s.fromClause.getOrElse(
              throw new RuntimeException(s"Select '$cf' with empty from clause")
            ).view.flatMap {
              case rv: RangeVar =>
                tmd.get(rv.schemaname, rv.relname).getOrElse(
                  throw new RuntimeException(s"Could not find table '$rv'")
                ).find(_.name == fieldName)
              case other =>
                throw new RuntimeException(s"Unsupported range type: $other")
            }.headOption.getOrElse(
              throw new RuntimeException(s"Could not find '$cf' in tables")
            )
            RetVal(name.getOrElse(fieldName), column.typeName, column.nullable)
          case QualField(Some(fieldPrefix), fieldName) =>
            throw new RuntimeException("Not implemented")
        }
      case _ =>
        throw new RuntimeException(s"Inferring type of '$v' not supported")
    }
  }

  case class RetVal(name: String, typeName: String, nullable: Boolean)
}

object ASTHelper {
  def getQualifiedField(cf: ColumnRef): QualField = {
    cf.fields.map {
      case FieldPart.Name(s) => s
      case FieldPart.Star => throw new RuntimeException("Inferring type of 'Star' not supported")
    } match {
      case List(n) => QualField(None, n)
      case List(t, n) => QualField(Some(t), n)
      case _ => throw new RuntimeException(s"Invalid qualified field '$cf'")
    }
  }

  case class QualField(prefix: Option[String], name: String)
}
