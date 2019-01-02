package SparkFederation.ServerFed

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Exceptions.TableNoExistFed
import SparkFederation.Lib.KafkaProperties
import SparkFederation.ServerFed.utils.{ServerHandler, SparkSQLHandler}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class SimpleServerFed (val idClient : String, val groupId : String ) (implicit ss : SparkSession) {

  val serverSummiter = new KafkaProducerFed(
      this.idClient,KafkaProperties.getStandardTopic("server")
  )
  final val queryListener  = new KafkaConsumerFed[KafkaClientMessage](
      this.groupId
    ,"query"
    ,"SparkFederation.ConnectorsFed.KafkaClientDeserializer"
  )

  val coreServer = new ServerHandler()
  val sqlServer = new SparkSQLHandler(this.coreServer)


  def listenQuery(): KafkaClientMessage ={

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


  def executeQuery(clientRecord: KafkaClientMessage ) : DataFrame = {

    try {

      val result = ss.sql(clientRecord.query)
      this.updateMetadata(clientRecord)

      result
    } catch {
      case e: Exception =>
        // TODO: function to send a generic error message to the client
        // WorkArround
      throw new Exception(e)
    }

  }

  // TODO: Tiene que lanzar una excepción propia para manejarla en particular en "executeQuery"
  //       porque si falla se podría relanzar de nuevo o simplemente dejarlo como está y comunicar al cliente
  //       que no se ha podido ejecutar la query por fallo en actualziación de metadatos
  def updateMetadata (clientRecord: KafkaClientMessage) : Unit = {
    val dropTablePat= raw"^DROP\s{1,}TABLE\s{1,}".r
    val createTablePat= raw"^CREATE\s{1,}TABLE\s{1,}".r





  }

}
