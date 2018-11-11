package SparkFederation.ServerFed

import SparkFederation.ConnectorsFed.{KafkaClientMessage, KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties
import scala.collection.JavaConversions._

class SimpleServerFed (val idClient : String, val groupId : String ) {

  val serverSummiter = new KafkaProducerFed(this.idClient,KafkaProperties.getStandardTopic("server"))
  val queryListener  = new KafkaConsumerFed[KafkaClientMessage](this.groupId,KafkaProperties.getStandardTopic("query"))



  def summitQuery(query : String): Unit ={

    val consumer = this.queryListener.consumer
    var flag = 0
    while (flag == 0) {

      val records = consumer.poll(100)

      println("Antes bucle " + records.count() + " vacio: " + records.isEmpty())

      for (record <- records.iterator()) {
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        flag = 1
      }

    }

    // llamar a la clase que maneja el contexto de spark y lanza la query

  }

}
