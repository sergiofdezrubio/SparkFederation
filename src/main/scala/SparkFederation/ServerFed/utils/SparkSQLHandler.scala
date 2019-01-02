package SparkFederation.ServerFed.utils

import SparkFederation.Exceptions.TableNoExistFed
import SparkFederation.Lib.{HDFSProperties, ZooKeeperProperties}
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}

class SparkSQLHandler (coreServer : ServerHandler) (implicit ss : SparkSession ) {

  val zkMaster = new zkExecutor()

  def parseSQLQuery(query: String): LogicalPlan = {

    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan
  }

  def getQueryPlan(query: String) : Seq[(String,LogicalPlan)] = {
    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect{ case r : SubqueryAlias => (r.alias,r.child)  }
  }

  def getQueryTableNames(query: String): Seq[String] = {

    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName  }

  }

  /*
  def getQueryTables( tables : Seq[String] ): Unit = {
    tables.map(t => {coreServer.getHDFSTable(t).createOrReplaceTempView(t)})
  }
*/

}
