package SparkFederation.ServerFed

import SparkFederation.ConnectorsFed._
import SparkFederation.Exceptions.{TableNoExistFed, UpdateMetaDataException}
import SparkFederation.Lib.{KafkaProperties, SparkProperties}
import SparkFederation.ServerFed.utils.{ServerHandler, SparkSQLHandler}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.zookeeper.KeeperException

import scala.collection.JavaConversions._

class SimpleServerFed (val idServer : String, val groupId : String = "ServerGroup" ) (implicit ss : SparkSession) {
//class SimpleServerFed (val idServer : String, val groupId : String ) {


  val serverSummiter = new KafkaProducerFed(
      this.idServer, KafkaProperties.getStandardTopic("server")
  )

  final val queryListener  = new KafkaConsumerFed[KafkaClientMessage](
      this.groupId
    ,"query"
    ,"SparkFederation.ConnectorsFed.KafkaClientDeserializer"
  )

  //implicit val ss = SparkProperties.ss
  val coreServer = new ServerHandler()
  val sqlServer = new SparkSQLHandler(this.coreServer)

  val initializeServer = coreServer.initServer()

  def mainServer() : Unit = {

    val message = listenQuery()

    //val message = new KafkaClientMessage("ClientTopic_1" , "select b.JURISDICTION_NAME,a.ZIP_code,a.Complaint_ID from Consumer_Complaints a inner join Demographic_Statistics_By_Zip_Code b on a.ZIP_code = b.JURISDICTION_NAME")


    val resultQuery: KafkaQueryResult = exeQueryFed(message)

    println("**** Resultado Query: ****")
    println(resultQuery)

    // TODO: Sent "resultQuery" to the specific Client topic
    sendStatusResult(message.topicId,resultQuery)

    // TODO: Synchronize all cluster

  }

  def sendStatusResult(topic : String, queryResult : KafkaQueryResult ) : Unit = {


    val serverSummiter = new KafkaProducerFed[KafkaQueryResult](idServer,topic,"SparkFederation.ConnectorsFed.KafkaQueryResultSerializer")

    val message = new KafkaQueryResult(queryResult.dataframe,queryResult.query,queryResult.typeQuery,queryResult.status,queryResult.error)
    //val message = new KafkaQueryResult(queryResult.query,queryResult.typeQuery,queryResult.status)

    serverSummiter.sendMessage(message,topic)
 //   serverSummiter.sendMessage("respuesta", topic)
  }

  def sendResult(topic: String, result : DataFrame ) : Unit = {

    //val serverSummiter = new KafkaProducerFed[String] (idServer,topic)

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
      //println("** Resultado en el servidor")
      //exeQuery.show()

      val result: KafkaQueryResult = new KafkaQueryResult(Option(exeQuery),clientRecord.query,infoQuery._1,"QuerySuccessful", Option.empty[Exception])
      //val result: KafkaQueryResult = new KafkaQueryResult(clientRecord.query,infoQuery._1,"QuerySuccessful")

      result
    } catch {
      case e: UpdateMetaDataException =>
        val result = new KafkaQueryResult(Option.empty[DataFrame],clientRecord.query,infoQuery._1,"UpdateMetadataError", Option(e))
        //val result = new KafkaQueryResult(clientRecord.query,infoQuery._1,"UpdateMetadataError")
        result
      case e: Exception =>
        val result = new KafkaQueryResult(Option.empty[DataFrame],clientRecord.query,infoQuery._1,"SparkSQLError", Option(e))
        //val result = new KafkaQueryResult(clientRecord.query,infoQuery._1,"SparkSQLError")
        result
    }

  }


  def updateMetadata (metaQuery : (String,String,String)) : Unit = {


    try {
      metaQuery match {
        case ( "CreateStatement"  ,_ , _ ) => {
          coreServer.registerTable(metaQuery._2,metaQuery._3)
          syncCluster(metaQuery)
        }
        case ( "DropStatement"    ,_ , _ ) => {
          coreServer.removeTable(metaQuery._2)
          syncCluster(metaQuery)
        }
        case ( "AnotherStatement" ,_ , _ ) => {
          syncCluster(metaQuery)
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

  def syncCluster(metaQuery : (String,String,String)): Unit = {
    // TODO: Proceso de broadcast al resto de servidores
  }

}
