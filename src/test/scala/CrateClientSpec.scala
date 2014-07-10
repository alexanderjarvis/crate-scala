import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

import org.elasticsearch.transport.RemoteTransportException

import io.crate.action.sql.SQLActionException
import io.crate.action.sql.SQLResponse
import io.crate.action.sql.SQLRequest

class CrateClientSpec extends FlatSpec with Matchers {

  val timeout = 5 seconds

  val client = CrateClient("localhost:4300")

  "Crate Client" should "create new client" in {
    val request = client.sql("SELECT * FROM sys.nodes")
    val response = Await.result(request, timeout)
    assert(response.cols.length > 0)
  }

  it should "drop table" in {
    val request = client.sql("DROP TABLE foo")
    val response = Await.result(request, timeout)
    println("drop table: " + response)
  }

  it should "create table" in {
    val request = client.sql("CREATE TABLE foo (id int primary key, name string)")
    val response = Await.result(request, timeout)
    println("create table: " + response)
  }

  it should "produce SQLActionException when creating existing table" in {
    val thrown = intercept[RemoteTransportException] {
       Await.result(client.sql("CREATE TABLE foo (id int primary key, name string)"), timeout)
    }
    println(thrown)
    println(thrown.getCause())
    thrown.getCause() shouldBe a [SQLActionException]
  }

  it should "insert data with parameter substitution" in {
    val stmt = "INSERT INTO foo (id, name) VALUES (?, ?)"
    val args = Array(1, "bar")
    val sqlRequest = SQLRequest(stmt, args)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("insert into: " + response)
    response.rowCount shouldBe (1)
  }

  it should "map Scala to Java data types on insertion" in {
    val dropResult = Await.result(client.sql("DROP TABLE test"), timeout)
    val result = Await.result(client.sql("CREATE TABLE test (st string, sh short, i integer, lo long, fl float, do double, b byte, bo boolean, arr array(string), o object, a ip, ts timestamp, ge geo_point)"), timeout)
    println("create table: " + result)

    val stmt = "INSERT INTO test (st, sh, i, lo, fl, do, b, bo, arr, o, a, ts, ge) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val args = Array(
      "hello",
      Short.MaxValue,
      Int.MaxValue,
      Long.MaxValue,
      Float.MaxValue,
      Double.MaxValue,
      Byte.MaxValue,
      true,
      Array("crate", "is", "pretty", "cool"),
      Map("nested" -> true, "maps" -> "yes"),
      "127.0.0.1",
      new java.util.Date().getTime(),
      Array(-0.1015987, 51.5286416)
    )

    val sqlRequest = SQLRequest(stmt, args)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("insert into: " + response)
    response.rowCount shouldBe (1)
  }

  it should "map Scala to Java data types for arrays" in {
    val dropResult = Await.result(client.sql("DROP TABLE testarrays"), timeout)
    val result = Await.result(client.sql("CREATE TABLE testarrays (a_string array(string), a_short array(short), a_integer array(integer), a_long array(long), a_float array(float), a_double array(double), a_byte array(byte), a_boolean array(boolean), a_object array(object), a_ip array(ip), a_timestamp array(timestamp))"), timeout)
    println("create table: " + result)

    val stmt = "INSERT INTO testarrays (a_string, a_short, a_integer, a_long, a_float, a_double, a_byte, a_boolean, a_ip, a_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val args = Array(
      Array("hello"),
      Array(Short.MaxValue),
      Array(Int.MaxValue),
      Array(Long.MaxValue),
      Array(Float.MaxValue),
      Array(Double.MaxValue),
      Array(Byte.MaxValue),
      Array(true),
      Array("127.0.0.1"),
      Array(new java.util.Date().getTime())
    )

    val sqlRequest = SQLRequest(stmt, args)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("insert into: " + response)
    response.rowCount shouldBe (1)
  }

}