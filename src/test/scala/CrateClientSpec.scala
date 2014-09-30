import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

import org.elasticsearch.transport.RemoteTransportException

import io.crate.action.sql.SQLActionException
import io.crate.action.sql.SQLResponse

import io.crate.client.ReactiveCrateClient
import io.crate.client.SQLRequest

class CrateClientSpec extends FlatSpec with Matchers {

  val timeout = 5 seconds

  val timestamp = new java.util.Date().getTime()

  val client = ReactiveCrateClient("localhost:4300")

  def dropTable(table: String) = Await.ready(client.sql("DROP TABLE " + table), timeout)

  dropTable("test")
  dropTable("testarrays")

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
      timestamp,
      Array(-0.1015987, 51.5286416)
    )

    val sqlRequest = SQLRequest(stmt, args)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("insert into: " + response)
    response.rowCount shouldBe (1)
  }

  it should "map Java to Scala data types on select" in {
    refresh("test")
    val request = client.sql("SELECT * FROM test")
    val response = Await.result(request, timeout)
    println("select: " + response)
    response.rowCount shouldBe (1)

    val rows = response.rows
    val row = rows(0)
    row shouldBe a [Array[Any]]

    // result columns are alphabetically sorted
    row(0) shouldBe a [String]
    row(0) shouldBe "127.0.0.1"
    row(1) shouldBe a [List[_]]
    row(1).asInstanceOf[List[String]] should contain inOrderOnly ("crate", "is", "pretty", "cool")
    //row(2) shouldBe a [Byte] // CRATE: 127 was not an instance of byte, but an instance of java.lang.Integer
    row(2) shouldBe Byte.MaxValue
    //row(3) shouldBe a [Boolean]
    row(3) shouldBe true
    //row(4) shouldBe a [Double]
    row(4) shouldBe Double.MaxValue
    //row(5) shouldBe a [Float] // CRATE: 3.4028235E38 was not an instance of float, but an instance of java.lang.Double
    //row(5) shouldBe Float.MaxValue // CRATE: 3.4028235E38 was not equal to 3.4028235E38
    row(6) shouldBe a [List[_]]
    row(6).asInstanceOf[List[Double]] should contain inOrderOnly (-0.1015987, 51.5286416)
    //row(7) shouldBe a [Int]
    row(7) shouldBe Int.MaxValue
    //row(8) shouldBe a [Long]
    row(8) shouldBe Long.MaxValue
    row(9).asInstanceOf[Map[_, _]] should contain only ("nested" -> true, "maps" -> "yes")
    //row(10) shouldBe a [Short] // CRATE: 32767 was not an instance of short, but an instance of java.lang.Integer
    row(10) shouldBe Short.MaxValue
    row(11) shouldBe a [String]
    row(11) shouldBe "hello"
    //row(12) shouldBe a [Long]
    row(12) shouldBe timestamp
  }

  it should "map more data types with types on response set" in {
    val sqlRequest = SQLRequest("SELECT * FROM test")
    sqlRequest.includeTypesOnResponse(true)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("select: " + response)
    response.rowCount shouldBe (1)

    val rows = response.rows
    val row = rows(0)
    row shouldBe a [Array[Any]]

    // result columns are alphabetically sorted
    row(0) shouldBe a [String]
    row(0) shouldBe "127.0.0.1"
    row(1) shouldBe a [List[_]]
    row(1).asInstanceOf[List[String]] should contain inOrderOnly ("crate", "is", "pretty", "cool")
    row(2) shouldBe a [java.lang.Byte]
    row(2) shouldBe Byte.MaxValue
    row(3) shouldBe a [java.lang.Boolean]
    row(3) shouldBe true
    row(4) shouldBe a [java.lang.Double]
    row(4) shouldBe Double.MaxValue
    row(5) shouldBe a [java.lang.Float]
    row(5) shouldBe Float.MaxValue
    row(6) shouldBe a [List[_]]
    row(6).asInstanceOf[List[Double]] should contain inOrderOnly (-0.1015987, 51.5286416)
    row(7) shouldBe a [java.lang.Integer]
    row(7) shouldBe Int.MaxValue
    row(8) shouldBe a [java.lang.Long]
    row(8) shouldBe Long.MaxValue
    row(9).asInstanceOf[Map[_, _]] should contain only ("nested" -> true, "maps" -> "yes")
    row(10) shouldBe a [java.lang.Short]
    row(10) shouldBe Short.MaxValue
    row(11) shouldBe a [String]
    row(11) shouldBe "hello"
    row(12) shouldBe a [java.lang.Long]
    row(12) shouldBe timestamp
  }

  it should "map Scala to Java data types for arrays" in {
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
      Array(timestamp)
    )

    val sqlRequest = SQLRequest(stmt, args)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("insert into: " + response)
    response.rowCount shouldBe (1)
  }

  it should "map Java to Scala data types for arrays" in {
    refresh("testarrays")
    val request = client.sql("SELECT * FROM testarrays")
    val response = Await.result(request, timeout)
    println("select: " + response)
    response.rowCount shouldBe (1)

    val row = response.rows.head

    // result columns are alphabetically sorted
    row(0).asInstanceOf[List[Boolean]] should contain only true
    row(1).asInstanceOf[List[Byte]] should contain only Byte.MaxValue
    row(2).asInstanceOf[List[Double]] should contain only Double.MaxValue
    //row(3).asInstanceOf[List[Float]] should contain only Float.MaxValue // CRATE: List(3.4028235E38) did not contain only (3.4028235E38)
    row(4).asInstanceOf[List[Int]] should contain only Int.MaxValue
    row(5).asInstanceOf[List[String]] should contain only "127.0.0.1"
    row(6).asInstanceOf[List[Long]] should contain only Long.MaxValue
    //row(7)
    row(8).asInstanceOf[List[Short]] should contain only Short.MaxValue
    row(9).asInstanceOf[List[String]] should contain only "hello"
    row(10).asInstanceOf[List[Long]] should contain only timestamp
  }

  it should "map more array data types with types on response set" in {
    val sqlRequest = SQLRequest("SELECT * FROM testarrays")
    sqlRequest.includeTypesOnResponse(true)
    val request = client.sql(sqlRequest)
    val response = Await.result(request, timeout)
    println("select: " + response)
    response.rowCount shouldBe (1)

    val row = response.rows.head

    // result columns are alphabetically sorted
    row(0).asInstanceOf[List[Boolean]] should contain only true
    row(1).asInstanceOf[List[Byte]] should contain only Byte.MaxValue
    row(2).asInstanceOf[List[Double]] should contain only Double.MaxValue
    row(3).asInstanceOf[List[Float]] should contain only Float.MaxValue
    row(4).asInstanceOf[List[Int]] should contain only Int.MaxValue
    row(5).asInstanceOf[List[String]] should contain only "127.0.0.1"
    row(6).asInstanceOf[List[Long]] should contain only Long.MaxValue
    //row(7)
    row(8).asInstanceOf[List[Short]] should contain only Short.MaxValue
    row(9).asInstanceOf[List[String]] should contain only "hello"
    row(10).asInstanceOf[List[Long]] should contain only timestamp
  }

  it should "access cell via column names" in {
    val request = client.sql("SELECT * FROM test")
    val response = Await.result(request, timeout)
    println("select: " + response)
    response.rowCount shouldBe (1)

    implicit val row = response.rows.head

    response.cell("st") shouldBe Some("hello")
    response.cell("i") shouldBe Some(Int.MaxValue)
    response.cell("ts") shouldBe Some(timestamp)
    response.cell("none") shouldBe None

    response.cell[String]("st") shouldBe Some("hello")
  }

  it should "bulk insert data" in {
    val stmt = "INSERT INTO foo (id, name) VALUES (?, ?)"
    val bulkArgs = Array(Array(1000, "bar"), Array(1001, "bar"))
    val request = client.bulkSql(stmt, bulkArgs)
    val response = Await.result(request, timeout)
    println("insert into: " + response)
    response.results.length shouldBe (2)
    val results = response.results
    results(1).rowCount shouldBe (1)
  }

  def refresh(table: String) = Await.ready(client.sql("refresh table " + table), timeout)

}