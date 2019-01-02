package SparkFederation.ServerFed.utils

import SparkFederation.Exceptions.TableNoExistFed
import SparkFederation.Lib.ZooKeeperProperties
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.{DataFrame, SparkSession}

class ServerHandler(implicit ss : SparkSession){

  val zkMaster = new zkExecutor()
  val hdfsHandler = new HDFSHandler()


  def initServer () : Unit = {
    val OpTablas = zkMaster.getChildern(ZooKeeperProperties.ZNODE_HDFS)

    if (OpTablas.isDefined){
      OpTablas.get.map(this.getHDFSTable)
    }

  }

  def registerTable(name : String , createStatement: String ) : Unit = {
    this.zkMaster.createZnode(ZooKeeperProperties.ZNODE_HDFS + "/" + name , createStatement.getBytes )
  }


  @throws(classOf[TableNoExistFed])
  def getHDFSTable(tableName: String): Unit = {

    //val rawData = zkMaster.getData("/hdfs/prueba").get
    //val netData = (rawData.map(_.toChar)).mkString

    val rawMetaData = zkMaster.getData(ZooKeeperProperties.ZNODE_HDFS + "/" + tableName)

    if (! rawMetaData.isDefined ){
      throw new TableNoExistFed (s"${tableName}'s metadata not found")
    }

    ss.sql(rawMetaData.get.map(_.toChar).mkString)
  }


}
