package SparkFederation.ServerFed.utils

import SparkFederation.Exceptions.TableNoExistFed
import SparkFederation.Lib.ZooKeeperProperties
import SparkFederation.ServerFed.zkCoordinatorFed.zkExecutor
import org.apache.spark.sql.{DataFrame, SparkSession}

class ServerHandler(implicit ss : SparkSession){

  val zkMaster = new zkExecutor()
  val hdfsHandler = new HDFSHandler()


  def initServer (idServer : String , groupId: String) : Boolean = {

    var flgInit = false

    val OpTablas = zkMaster.getChildern(ZooKeeperProperties.ZNODE_HDFS)

    if (OpTablas.isDefined){
      OpTablas.get.map(this.getHDFSTable)
      flgInit = true
    }
    flgInit

  }

  def getServersTopics (idServer : String ) : Array[String] = {
    val serversId = zkMaster.getChildern(ZooKeeperProperties.ZNODE_SERVER)
      .getOrElse(Array(idServer))
      .filter(p => { p != idServer})

    val topicsRaw = serversId.map(t => {
      println(s"${ZooKeeperProperties.ZNODE_SERVER}/${t}")
      zkMaster.getData(s"${ZooKeeperProperties.ZNODE_SERVER}/${t}").get.map(_.toChar)
    })

    topicsRaw.map(_.mkString)
  }


  def registerTable(tableName : String , createStatement: String ) : Unit = {
    zkMaster.createZnode(ZooKeeperProperties.ZNODE_HDFS + "/" + tableName  , createStatement.getBytes )
  }

  def removeTable (tableName : String) : Unit   = {
    zkMaster.deleteZnode(ZooKeeperProperties.ZNODE_HDFS + "/" + tableName )
  }

  def getMetaData(tableName : String) : String  = {
    zkMaster.getData(ZooKeeperProperties.ZNODE_HDFS + "/" +  tableName ).get.map(_.toChar).mkString
  }
  def checkMetaData (tableName : String) : Boolean = {
    if(zkMaster.existsZnode(ZooKeeperProperties.ZNODE_HDFS + "/" + tableName ).isDefined) true else false
  }


  @throws(classOf[TableNoExistFed])
  def getHDFSTable(tableName: String): DataFrame = {

    //val rawData = zkMaster.getData("/hdfs/prueba").get
    //val netData = (rawData.map(_.toChar)).mkString

    val rawMetaData = zkMaster.getData(ZooKeeperProperties.ZNODE_HDFS + "/" + tableName)

    if (! rawMetaData.isDefined ){
      throw new TableNoExistFed (s"${tableName}'s metadata not found")
    }

    ss.sql(rawMetaData.get.map(_.toChar).mkString)

  }


}
