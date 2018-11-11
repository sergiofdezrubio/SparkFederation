package SparkFederation.ClientFed

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties
import scala.collection.JavaConversions._

class SimpleClientFed (val idClient : String, val groupId : String ) {

  val querySummiter  = new KafkaProducerFed[KafkaClientMessage](this.idClient,KafkaProperties.getStandardTopic("query"))
  val serverListener = new KafkaConsumerFed[String](this.groupId,KafkaProperties.getStandardTopic("server"))
  val topicClient    = createClientTopic()
  val clientListener = new KafkaConsumerFed[String](this.groupId,this.topicClient)

  def createClientTopic() : String = {
    val topic= KafkaProperties.createClientTopicId()
    val result = KafkaProperties.createTopic(topic)
    result
  }


  def summitQuery (query: String): Unit = {

    // create message to sent
    val message = new KafkaClientMessage(this.topicClient,query)
    // sent query by querySummiter
    this.querySummiter.sendMessage(message)
    // listen to the new topic

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
  }

}
