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

  def bulkSql(statement: String, bulkArgs: Array[Array[Any]])(implicit ec: ExecutionContext): Future[io.crate.action.sql.SQLBulkResponse] = {
    bulkSql(SQLBulkRequest(statement, bulkArgs))
  }

  def bulkSql(bulkRequest: io.crate.action.sql.SQLBulkRequest)(implicit ec: ExecutionContext): Future[io.crate.action.sql.SQLBulkResponse] = {
    val promise = Promise[io.crate.action.sql.SQLBulkResponse]()
    javaCrateClient.bulkSql(bulkRequest, actionListener(promise))
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