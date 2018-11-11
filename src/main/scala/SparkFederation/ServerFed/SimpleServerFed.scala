package SparkFederation.ServerFed

import SparkFederation.ConnectorsFed.KafkaProducerFed
import SparkFederation.Lib.KafkaProperties

class SimpleServerFed (val idClient : String, val groupId : String ) {

  val serverSummiter = new KafkaProducerFed(this.idClient,KafkaProperties.getTopic("server"))
  val queryListener  = new KafkaProducerFed(this.groupId,KafkaProperties.getTopic("query"))


  def summitQuery(query : String): Unit ={




  }

}
