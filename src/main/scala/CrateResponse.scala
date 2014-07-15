import scala.collection.JavaConverters._

import io.crate.action.sql.SQLResponse
import io.crate.types._

case class CrateResponse(
  cols: Array[String],
  rows: Array[Array[_]],
  columnTypes: Array[DataType[_]],
  rowCount: Long,
  duration: Long) {

  override def toString() = {
    "CrateResponse { " +
      "cols=" + cols.mkString("[", ",", "]") +
      ", colTypes=" + columnTypes.map(_.toString()).mkString("[", ",", "]") +
      ", rows=" + rows.length  +
      ", rowCount=" + rowCount  +
      ", duration=" + duration  +
      " }";
  }

}

object CrateResponse {

  def apply(response: SQLResponse): CrateResponse = {

    val duration = response.duration()

    implicit val columnTypes = response.columnTypes()

    CrateResponse(
      cols = response.cols(),
      rows = convertRows(response.rows()),
      columnTypes = columnTypes,
      rowCount = response.rowCount(),
      duration = duration
    )
  }

  def convertRows(rows: Array[Array[Object]])(implicit columnTypes: Array[DataType[_]]): Array[Array[_]] = {
    rows.map(convertRow(_))
  }

  def convertRow(row: Array[Object])(implicit columnTypes: Array[DataType[_]]): Array[_] = {
    if (columnTypes.isEmpty) {
      row.map {
        case l: java.util.List[_] => l.asScala.toList.map(_.asInstanceOf[AnyVal])
        case m: java.util.Map[_, _] => m.asScala.toMap.mapValues(_.asInstanceOf[AnyVal])
        case o: AnyRef => o.asInstanceOf[AnyVal]
        case _ => // null
      }
    } else {
      Array.tabulate(row.length) { i =>
        convertToScalaColumnType(row(i), columnTypes(i))
      }
    }
  }

  def convertToScalaColumnType(o: Any, columnType: DataType[_]): Any = {
    columnType match {
      case _: StringType => o.asInstanceOf[String]
      case _: ShortType => o match {
        case i: java.lang.Integer => i.shortValue().asInstanceOf[Short]
        case o: Any => o.asInstanceOf[Short]
      }
      case _: IntegerType => o.asInstanceOf[Int]
      case _: LongType => o.asInstanceOf[Long]
      case _: FloatType => o match {
        case d: java.lang.Double => d.floatValue().asInstanceOf[Float]
        case o: Any => o.asInstanceOf[Float]
      }
      case _: DoubleType => o.asInstanceOf[Double]
      case _: ByteType => o match {
        case i: java.lang.Integer => i.byteValue().asInstanceOf[Byte]
        case o: Any => o.asInstanceOf[Byte]
      }
      case _: BooleanType => o.asInstanceOf[Boolean]
      case arrayType: ArrayType => o.asInstanceOf[java.util.List[_]].asScala.toList.map { io =>
        convertToScalaColumnType(io, arrayType.innerType())
      }
      case _: ObjectType => o.asInstanceOf[java.util.Map[_, _]].asScala.toMap.mapValues(_.asInstanceOf[AnyVal])
      //case _: IpType => o.asInstanceOf[String]  // unreachable as IpType extends StringType
      //case _: TimestampType => o.asInstanceOf[Long] // unreachable as TimestampType extends LongType
      case _: GeoPointType => o.asInstanceOf[java.util.List[Double]].asScala.toList.map(_.asInstanceOf[AnyVal])
      case _ => // null
    }
  }

}