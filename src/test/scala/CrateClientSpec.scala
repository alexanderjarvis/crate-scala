import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

import io.crate.action.sql.SQLResponse;

class CrateClientSpec extends FlatSpec with Matchers {

  val timeout = 5 seconds

  "Crate Client" should "create new client" in {
    val client = CrateClient("localhost:4300")
    val request = client.sql("SELECT * FROM sys.nodes")

    val response = Await.result(request, timeout)
    assert(response.cols.length > 0)
  }

}