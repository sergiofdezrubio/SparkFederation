package SparkFederation.ClientFed

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed, KafkaQueryResult}
import SparkFederation.Lib.KafkaProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class SimpleClientFed (val idClient : String, val groupId : String = "StandarClient") (implicit  ss : SparkSession) {

  val querySummiter  = new KafkaProducerFed[KafkaClientMessage](this.idClient,KafkaProperties.getStandardTopic("query"),"SparkFederation.ConnectorsFed.KafkaClientSerializer")
  val serverListener = new KafkaConsumerFed[String](this.groupId,KafkaProperties.getStandardTopic("server"))
  val topicClient    = createClientTopic()
  val topicClientResult = createClientTopic("-Result")
  val clientListener = new KafkaConsumerFed[KafkaQueryResult](this.groupId,this.topicClient,"SparkFederation.ConnectorsFed.KafkaQueryResultDeserializer")


  def createClientTopic(typeTopic : String = "") : String = {
    val topic= KafkaProperties.createClientTopicId()
    println("** client topic  " + topic )
    val result = KafkaProperties.createTopic(topic + typeTopic)
    result
  }

  def getQueryResult() : DataFrame = {

    val df = ss
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaProperties.brokers)
      .option("subscribe", topicClientResult)
      .load()
    df
  }


  def summitQuery (query: String): Unit = {

    // create message to sent
    val message = new KafkaClientMessage(this.topicClient,query)
    // sent query by querySummiter
    this.querySummiter.sendMessage(message, KafkaProperties.getStandardTopic( "query"))
    // listen to the new topic

    println("--- Se ha enviado la query: " + query)
    /*
    val consumer = this.clientListener.consumer

    var flag = 0
    while (flag == 0) {

      val records = consumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("entro")
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        flag = 1
      }

    }
    */
  }

  def getStatusResult(): KafkaQueryResult ={

    val consumer = this.clientListener.consumer
    var result :  KafkaQueryResult = null
    var flag = 0

    while (flag == 0) {

      val records = consumer.poll(100)

      println("Esto es el cliente: Antes bucle  " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {

        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        result = record.value()
        flag = 1
      }

    }
    consumer.commitSync()

    result

  }

  def shutdown (): Unit ={
    KafkaProperties.deleteTopic(this.topicClient)

  }

}
