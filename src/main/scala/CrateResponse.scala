import scala.collection.JavaConverters._

import io.crate.action.sql.SQLResponse

case class CrateResponse(
  cols: Array[String],
  rows: Array[Array[_]],
  columnTypes: Array[String],
  rowCount: Long,
  duration: Long) {

  override def toString() = {
    "CrateResponse { " +
      "cols=" + cols.mkString("[", ",", "]") +
      ", colTypes=" + columnTypes.mkString("[", ",", "]") +
      ", rows=" + rows.length  +
      ", rowCount=" + rowCount  +
      ", duration=" + duration  +
      " }";
  }

}

object CrateResponse {

  def apply(response: SQLResponse): CrateResponse = {

    val duration = response.duration()

    CrateResponse(
      cols = response.cols(),
      rows = convertRows(response.rows()),
      columnTypes = response.columnTypes().map(_.toString()),
      rowCount = response.rowCount(),
      duration = duration
    )
  }

  def convertRows(rows: Array[Array[Object]]): Array[Array[_]] = {
    rows.map(convertRow(_))
  }

  def convertRow(row: Array[Object]): Array[_] = {
    row.map {
      case l: java.util.List[_] => l.asScala.toList.map(_.asInstanceOf[AnyVal])
      case m: java.util.Map[_, _] => m.asScala.toMap.mapValues(_.asInstanceOf[AnyVal])
      case o: AnyRef => o.asInstanceOf[AnyVal]
    }
  }

}