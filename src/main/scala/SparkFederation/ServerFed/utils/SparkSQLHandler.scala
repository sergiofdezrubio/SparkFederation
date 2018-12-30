package SparkFederation.ServerFed.utils

import SparkFederation.Lib.{HDFSProperties, ZooKeeperProperties}
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}

class SparkSQLHandler (implicit ss : SparkSession) {


  var tables: Map [String,DataFrame] = Map()
  val zkMaster = new zkExecutor()

  def getHDFSTable(tableName: String)(implicit  ss : SparkSession): DataFrame = {

    /*
    if ( ! this.tables.exists(_._1 == tableName)){
      println ("********* " + this.tables)
      val hdfsMaster = new HDFSHandler()
      this.tables  = this.tables + ( tableName -> hdfsMaster.getDataset(tableName + ".parquet"))
    }


    val table : DataFrame = this.tables.get(tableName ).get

    table
    */

    //val rawData = zkMaster.getData("/hdfs/prueba").get
    //val netData = (rawData.map(_.toChar)).mkString

    val metaTable = zkMaster.getData(ZooKeeperProperties.ZNODE_HDFS + "/" + tableName)
//    val table : DataFrame =





  }

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
