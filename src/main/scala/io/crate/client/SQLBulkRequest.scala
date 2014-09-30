package io.crate.client

import java.util.Date
import java.util.UUID

import scala.collection.JavaConverters._

object SQLBulkRequest extends SQLRequestTrait {

  def apply(stmt: String) = new io.crate.action.sql.SQLBulkRequest(stmt)

  def apply(stmt: String, bulkArgs: Array[Array[Any]]): io.crate.action.sql.SQLBulkRequest = {
    val javaArgs = bulkArgs.map(_.map(convertToJavaColumnType(_)))
    new io.crate.action.sql.SQLBulkRequest(stmt, javaArgs)
  }

}