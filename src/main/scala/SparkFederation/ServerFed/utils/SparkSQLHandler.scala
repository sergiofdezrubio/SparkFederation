package SparkFederation.ServerFed.utils

import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.execution.datasources.CreateTable

class SparkSQLHandler (coreServer : ServerHandler) (implicit ss : SparkSession ) {

  val zkMaster = new zkExecutor()



  def exeQuerySpark (query : String ) : DataFrame = ss.sql(query)

  def getTypeQuery (query : String) : (String, String, String) = {
    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    val typeQuery  = logicalPlan.collectFirst {
        case r: CreateTable      => ("CreateStatement" , r.tableDesc.identifier.table, query)
        case r: DropTableCommand => ("DropStatement" ,r.tableName.table, query)
        case r: Project          => ("SelectStatement", "" , query )
      }
    typeQuery.getOrElse(("AnotherStatement", "" ,query))
  }

  def getQueryTableNames(query: String): Seq[String] = {

    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName  }

  }

  def getQueryPlan(query: String): LogicalPlan = {
    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan
  }

}
