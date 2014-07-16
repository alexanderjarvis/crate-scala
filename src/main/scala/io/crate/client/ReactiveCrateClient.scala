package io.crate.client

import scala.concurrent._

import org.elasticsearch.action.ActionListener

class ReactiveCrateClient(javaCrateClient: CrateClient) {

  def sql(statement: String)(implicit ec: ExecutionContext): Future[CrateResponse] = {
    sql(SQLRequest(statement))
  }

  def sql(statement: String, args: Array[_])(implicit ec: ExecutionContext): Future[CrateResponse] = {
    sql(SQLRequest(statement, args))
  }

  def sql(request: io.crate.action.sql.SQLRequest)(implicit ec: ExecutionContext): Future[CrateResponse] = {
    val promise = Promise[io.crate.action.sql.SQLResponse]()
    javaCrateClient.sql(request, actionListener(promise))
    promise.future.map(CrateResponse(_))
  }

  private def actionListener[A](promise: Promise[A]) = new ActionListener[A] {
    def onResponse(response: A) {
      promise.success(response)
    }
    def onFailure(e: Throwable) {
      promise.failure(e)
    }
  }

  def close(): Unit = {
    javaCrateClient.close()
  }

}

object ReactiveCrateClient {

  def apply(servers: String*): ReactiveCrateClient = {
    val javaCrateClient = new CrateClient(servers: _*)
    new ReactiveCrateClient(javaCrateClient)
  }

}