import scala.concurrent._

import org.elasticsearch.action.ActionListener

import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;

class CrateClient(javaCrateClient: io.crate.client.CrateClient) {

  def sql(statement: String)(implicit ec: ExecutionContext): Future[SQLResponse] = {
    val promise = Promise[SQLResponse]()
    javaCrateClient.sql(statement, actionListener(promise))
    promise.future
  }

  def sql(request: SQLRequest)(implicit ec: ExecutionContext): Future[SQLResponse] = {
    val promise = Promise[SQLResponse]()
    javaCrateClient.sql(request, actionListener(promise))
    promise.future
  }

  private def actionListener[A](promise: Promise[A]) = new ActionListener[A] {
    def onResponse(response: A) {
      promise.success(response)
    }
    def onFailure(e: Throwable) {
      promise.failure(e)
    }
  }

}

object CrateClient {

  def apply(servers: String*): CrateClient = {
    val javaCrateClient = new io.crate.client.CrateClient(servers: _*)
    new CrateClient(javaCrateClient)
  }

}