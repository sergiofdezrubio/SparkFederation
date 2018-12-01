package SparkFederation.ConnectorsFed

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}

class SparkSQLHandler (implicit ss : SparkSession) {

  def parseSQLQuery(query: String): LogicalPlan = {

    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan
  }

  def getQueryPlan(query: String) (implicit logicalPlan : LogicalPlan): Seq[(String,LogicalPlan)] = {
    //val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect{ case r : SubqueryAlias => (r.alias,r.child)  }
  }

  def getQueryTables(query: String)(implicit logicalPlan : LogicalPlan) : Seq[String] = {

    //val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName  }

  }
  /*
  def getTable( tableName : String ): DataFrame = {


  }

  def compileQuery() (implicit sc : SparkContext) = {


  }
  */

}
