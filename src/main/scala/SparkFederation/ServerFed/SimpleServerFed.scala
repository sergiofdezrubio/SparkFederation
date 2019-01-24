package SparkFederation.ServerFed

import SparkFederation.ConnectorsFed._
import SparkFederation.Exceptions.{TableNoExistFed, UpdateMetaDataException}
import SparkFederation.Lib.{KafkaProperties, SparkProperties}
import SparkFederation.ServerFed.utils.{ServerHandler, SparkSQLHandler}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.zookeeper.KeeperException

import scala.collection.JavaConversions._

class SimpleServerFed (val groupId : String = "ServerGroup" ) (implicit ss : SparkSession) {
//class SimpleServerFed (val idServer : String, val groupId : String ) {

  //val SERVER_ID = KafkaProperties.createUniqueId()
  val SERVER_ID = "Server_2"
  val serverTopic= createServerTopic(SERVER_ID)

  val serverSummiter = new KafkaProducerFed(
      s"${groupId}-${SERVER_ID}", KafkaProperties.getStandardTopic("server")
  )

  val queryListener  = new KafkaConsumerFed[KafkaClientMessage](
      this.groupId
    ,"query"
    ,"SparkFederation.ConnectorsFed.KafkaClientDeserializer"
  )

  val serverListener = new KafkaConsumerFed[String](s"${groupId}-Servers",serverTopic)


  def createServerTopic(serverId : String) : String = KafkaProperties.createTopic(s"${groupId}_${serverId}")

  val coreServer = new ServerHandler()
  val sqlServer = new SparkSQLHandler(this.coreServer)

  val initializeServer = coreServer.initServer(SERVER_ID,groupId)

  def mainServer() : Unit = {

    val message = listenQuery()
    updateServer()

    val resultQuery: KafkaQueryResult = exeQueryFed(message)

    sendStatusResult(message.topicsId.get(0),resultQuery)

    if (resultQuery.typeQuery == "SelectStatement"  && resultQuery.status == "QuerySuccessful" ){
      sendResult(message.topicsId.get(1),resultQuery.dataframe.get )
    } else  if (resultQuery.status == "QuerySuccessful") {
      syncCluster(resultQuery.query)
    }
  }

  def sendStatusResult(topic : String, queryResult : KafkaQueryResult ) : Unit = {


    val serverSummiter = new KafkaProducerFed[KafkaQueryResult](SERVER_ID,topic,"SparkFederation.ConnectorsFed.KafkaQueryResultSerializer")

    val message = new KafkaQueryResult(
      queryResult.dataframe
      ,queryResult.query
      ,queryResult.typeQuery
      ,queryResult.status
      ,queryResult.error
      ,queryResult.schemaDataframe
    )

    serverSummiter.sendMessage(message,topic)

  }

  def sendResult(topic: String, resultRaw : DataFrame ) : Unit = {

    // It create a DataFrame which is formed by one column, called "valued"
    val resultValue = resultRaw.withColumn("value", org.apache.spark.sql.functions.split(
      org.apache.spark.sql.functions.concat_ws(";",  resultRaw.schema.fieldNames.map(c=> org.apache.spark.sql.functions.col(c)):_*), ";"
    ))

    // It adds a new auto-increment an unique column called "key" <-- It could be better a random and auto-generated ID column
    val result = resultValue.withColumn("key", org.apache.spark.sql.functions.monotonically_increasing_id())

    // As a resoult we hav a dataframe wich has 2 columns, one called Key which is a string. an another
    // called Value which is an Array[String]

    // this dataframe can be native written in a kafka's topic
    result.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaProperties.brokers)
      .option("topic", topic)
      .save()
  }

  def listenQuery(): KafkaClientMessage = {

    val consumer = this.queryListener.consumer
    var result :  KafkaClientMessage = null
    var flag = 0

    while (flag == 0) {

      val records = consumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {

        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        result = record.value()
        flag = 1
      }

    }
    consumer.commitSync()

    result
    // llamar a la clase que maneja el contexto de spark y lanza la query

  }


  def exeQueryFed(clientRecord: KafkaClientMessage ) : KafkaQueryResult = {

    val infoQuery = sqlServer.getTypeQuery(clientRecord.query)
    try {

      val exeQuery = sqlServer.exeQuerySpark(clientRecord.query)

      this.updateMetadata(infoQuery)

      lazy val schemaDf = Option(exeQuery.schema.fieldNames)

      if (infoQuery._1 == "SelectStatement") {
        val result: KafkaQueryResult = new KafkaQueryResult(Option(exeQuery),clientRecord.query,infoQuery._1,"QuerySuccessful", Option.empty[Exception],schemaDf)
        result
      } else {
        val result: KafkaQueryResult = new KafkaQueryResult(Option(exeQuery),clientRecord.query,infoQuery._1,"QuerySuccessful", Option.empty[Exception],Option.empty[Array[String]])
        result
      }
    } catch {
      case e: UpdateMetaDataException =>
        val result = new KafkaQueryResult(Option.empty[DataFrame],clientRecord.query,infoQuery._1,"UpdateMetadataError", Option(e),Option.empty[Array[String]])
        result
      case e: Exception =>
        val result = new KafkaQueryResult(Option.empty[DataFrame],clientRecord.query,infoQuery._1,"SparkSQLError", Option(e),Option.empty[Array[String]])
        result
    }

  }


  def updateMetadata (metaQuery : (String,String,String)) : Unit = {


    try {
      metaQuery match {
        case ( "CreateStatement"  ,_ , _ ) => {
          coreServer.registerTable(metaQuery._2,metaQuery._3)
        }
        case ( "DropStatement"    ,_ , _ ) => {
          coreServer.removeTable(metaQuery._2)
        }
        case ( "AnotherStatement" ,_ , _ ) => {
        }
        case _ =>
      }
    } catch {
      case e : KeeperException => {
        if (metaQuery._1 == "CreateStatement" ) {
          // if the create had been executed before and its the same statement, the query execution is correct
          // else is not
          if ( ! (
                coreServer.checkMetaData(metaQuery._2)
                && coreServer.getMetaData(metaQuery._2) == metaQuery._3
              )
            ) {
            throw  new UpdateMetaDataException (e.getMessage, e.getCause)
          }
        } else if (metaQuery._1 == "DropStatement") {
          // If "removeTable" fails because the metadata doesnt exist, the execution is correct
          // else is not
          if (coreServer.checkMetaData(metaQuery._2)){
            throw  new UpdateMetaDataException (e.getMessage, e.getCause)
          }
        } else {
          throw  new UpdateMetaDataException (e.getMessage, e.getCause)
        }
      }
      case e : InterruptedException => {
        throw new UpdateMetaDataException (e.getMessage, e.getCause)
      }
      case e : NullPointerException => {
        if ( !(metaQuery._1 == "DropStatement")) {
          throw new UpdateMetaDataException (e.getMessage, e.getCause)
        }

      }
    }
  }

  def syncCluster(query: String): Unit = {
    // TODO: Proceso de broadcast al resto de servidores

    val serverTopics = coreServer.getServersTopics(SERVER_ID)
    serverTopics.foreach(println)
    serverTopics.map(send2Server(query,_))

  }

  def send2Server( query: String, topic: String ) : Unit = {

    val serverSummiter = new KafkaProducerFed[String](SERVER_ID,topic)
    serverSummiter.sendMessage(query,topic)
  }

  def updateServer(): Unit = {

    var upQuery = listenServerTopic()
    println("*** Hola ")
    while (upQuery.isDefined){
      println("*** Hola 1")
      sqlServer.exeQuerySpark(upQuery.get)
      upQuery = Option.empty[String]
      listenServerTopic()
    }

  }

  def listenServerTopic(): Option[String] = {

    val consumer = this.serverListener.consumer
    var result :  Option[String] = Option.empty[String]
    var flag = 0
    var tried = 0

    println("********* listenServerTopic")

    while (flag == 0) {
      println(s"********* while ${tried}")
      val records = consumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {

        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        result = Option(record.value())
        flag = 1
      }

      if (tried > 4) {
        flag  = 1
      }

      tried += 1

      println(s"********* while ${tried} + ${flag}")
    }
    consumer.commitSync()

    result
  }

}
