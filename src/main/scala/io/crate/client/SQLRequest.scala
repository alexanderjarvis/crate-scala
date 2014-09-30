package io.crate.client

object SQLRequest extends SQLRequestTrait {

  def apply(stmt: String) = new io.crate.action.sql.SQLRequest(stmt)

  def apply[T](stmt: String, args: Array[T]): io.crate.action.sql.SQLRequest = {
    val javaArgs = args.map(convertToJavaColumnType(_))
    new io.crate.action.sql.SQLRequest(stmt, javaArgs)
  }

}