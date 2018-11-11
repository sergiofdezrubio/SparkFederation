package SparkFederation.ClientFed

import SparkFederation.ConnectorsFed.{KafkaConsumerFed, KafkaProducerFed}
import SparkFederation.Lib.KafkaProperties

class SimpleClientFed (val idClient : String, val groupId : String ) {

  val querySummiter = new KafkaProducerFed(this.idClient,KafkaProperties.getTopic("query"))
  val serverListener = new KafkaConsumerFed(this.groupId,KafkaProperties.getTopic("server"))

  def summitQuery (query: String): Unit = {
    // create temporal topic

    // sent query by querySummiter

    // listen to the new topic

  }

}
