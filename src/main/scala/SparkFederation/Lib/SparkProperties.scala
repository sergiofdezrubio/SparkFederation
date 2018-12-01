package SparkFederation.Lib

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
//import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

object SparkProperties {

   val ss = SparkSession.builder
    .master("local")
    .appName("SparkFederation")
    .config("spark.driver.host","localhost")
    .master("local[*]")
    .getOrCreate()


  var tables: Map [String,DataFrame] = Map()

   def getHDFSTable(tableName: String)(implicit  ss : SparkSession): DataFrame = {

     if ( ! this.tables.exists(_._1 == tableName)){
       println ("********* " + this.tables)
       this.tables  = this.tables + ( tableName -> HDFSProperties.getDataset(tableName + ".parquet"))
     }


     val table : DataFrame = this.tables.get(tableName ).get

     table

   }
/*
  def parseSQLQuery(query: String)(implicit ss : SparkSession): LogicalPlan = {

    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan
  }

  def getQueryPlan(query: String) (implicit ss : SparkSession): Seq[(String,LogicalPlan)] = {
    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect{ case r : SubqueryAlias => (r.alias,r.child)  }
  }

  def getQueryTables(query: String)(implicit ss : SparkSession) : Seq[String] = {

    val logicalPlan = ss.sessionState.sqlParser.parsePlan(query)
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName  }

  }

  def compileQuery() (implicit sc : SparkContext) = {


  }
*/
/*
  def getHDFSTable (tablename: String) : DataFrame = {


  }*/
}
