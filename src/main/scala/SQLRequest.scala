import scala.collection.JavaConverters._

object SQLRequest {

  def apply(stmt: String) = new io.crate.action.sql.SQLRequest(stmt)

  def apply[T](stmt: String, args: Array[T]): io.crate.action.sql.SQLRequest = {
    val javaArgs = args.map {
      case x: Array[Short] => x.map(_.asInstanceOf[java.lang.Short])
      case x: Array[Int] => x.map(_.asInstanceOf[java.lang.Integer])
      case x: Array[Long] => x.map(_.asInstanceOf[java.lang.Long])
      case x: Array[Float] => x.map(_.asInstanceOf[java.lang.Float])
      case x: Array[Double] => x.map(_.asInstanceOf[java.lang.Double])
      case x: Array[Byte] => x.map(_.asInstanceOf[java.lang.Byte])
      case x: Array[Boolean] => x.map(_.asInstanceOf[java.lang.Boolean])
      case m: Map[_, _] => m.asJava
      case v: Any => v.asInstanceOf[AnyRef]
    }
    new io.crate.action.sql.SQLRequest(stmt, javaArgs)
  }

}